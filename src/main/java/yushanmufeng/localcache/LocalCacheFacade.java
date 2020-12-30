package yushanmufeng.localcache;

import yushanmufeng.localcache.util.MapRandomAccessUtil;
import yushanmufeng.localcache.util.SimpleTaskExecutor;
import yushanmufeng.localcache.config.EmptyExpireRateLoader;
import yushanmufeng.localcache.config.LocalCacheConfig;
import yushanmufeng.localcache.task.MergingFutureTask;
import yushanmufeng.localcache.task.MergingTaskFactory;
import yushanmufeng.localcache.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * 缓存管理器为对外提供缓存服务的门面类
 * 类似Hibernate的功能, 提供1级缓存，除了普通的K-V缓存，还支持简单的单表条件查询，即1对多缓存；支持自定义数据访问具体实现，
 */
@SuppressWarnings(value={"unchecked", "rawtypes"})
public class LocalCacheFacade {

    public static final Logger log = LoggerFactory.getLogger(LocalCacheFacade.class);

    /** 缓存管理 */
    private EntityCacheManager cache;

    /** 执行器结束计数器 */
    private CountDownLatch countDownLatch;
    /** 所有要从DB中查询数据的任务队列(主键查询和条件查询)、 所有增删改db操作的任务队列 */
    public final SimpleTaskExecutor<MergingFutureTask<?>>[] selectExecutors, nonSelectExecutors;

    /** 各表的原子操作组件 */
    private Map<Class<Cacheable>, SingleTableAtomicLogic> tableAtomicLogics;

    /** 自适应过期时间参数持久化接口 */
    private IExpireRateLoader expireRateLoader;

    /** 任务执行线程的名字前缀 */
    private String PRE_SELECT_THREAD_NAME, PRE_NON_SELECT_THREAD_NAME;

    /**
     * 对外提供服务调用的门面类
     *
     * @param tableDataSources 所有表的数据源, 如果为空则无任何可用的缓存
     * @param config 可配置参数
     */
    public LocalCacheFacade(List<TableDataSource<?>> tableDataSources, LocalCacheConfig config){
        this(tableDataSources, null, config);
    }

    /**
     * 对外提供服务调用的门面类
     *
     * @param tableDataSources 所有表的数据源, 如果为空则无任何可用的缓存
     * @param expireRateLoader 自适应过期权重系数持久化策略，允许为空
     * @param config 可配置参数
     */
    public LocalCacheFacade(List<TableDataSource<?>> tableDataSources, IExpireRateLoader expireRateLoader, LocalCacheConfig config){
        expireRateLoader = expireRateLoader != null ? expireRateLoader : new EmptyExpireRateLoader();
        PRE_SELECT_THREAD_NAME = config.selectThreadPreName;
        PRE_NON_SELECT_THREAD_NAME = config.nonSelectThreadPreName;
        selectExecutors = new SimpleTaskExecutor[config.selectThreadCount];
        nonSelectExecutors = new SimpleTaskExecutor[config.nonSelectThreadCount];
        cache = new EntityCacheManager(config);
        // 初始化所有table数据源
        if(tableDataSources != null && tableDataSources.size() > 0){
            tableAtomicLogics = new HashMap<>();
            MapRandomAccessUtil.init(); // 初始化Map随机访问工具
            expireRateLoader.initiation();
            for(TableDataSource<?> dataSource : tableDataSources){
                TableDataSource<Cacheable> tableDataSource = (TableDataSource<Cacheable>)dataSource;
                TableDescribe<Cacheable> tableDesc = new TableDescribe<>(config, tableDataSource, cache);
                cache.initTableCache(tableDesc);
                expireRateLoader.load(tableDesc);
                tableAtomicLogics.put(tableDataSource.getEntityClass(), new SingleTableAtomicLogic(config, tableDesc, cache, selectExecutors, nonSelectExecutors));
            }
            countDownLatch = new CountDownLatch(nonSelectExecutors.length);
            startConsumerThread();
        }
    }

    /** 启动sql异步任务执行线程 */
    private void startConsumerThread(){
        // 查询任务消费线程
        for(int i = 0; i < selectExecutors.length; i++){
            selectExecutors[i] = new SimpleTaskExecutor<>(PRE_SELECT_THREAD_NAME + i, true, countDownLatch);
        }
        // 增删改任务消费线程
        for(int i = 0; i < nonSelectExecutors.length; i++){
            nonSelectExecutors[i] = new SimpleTaskExecutor<>(PRE_NON_SELECT_THREAD_NAME + i, true, countDownLatch);
        }
        log.info("DB异步查询线程已启动");
    }

    /**
     * 监听容器关闭，在停止时保存表权重信息和未落盘的数据
     */
    public void shutdown() {
        if(tableAtomicLogics != null && tableAtomicLogics.size() > 0) {
            // 触发自适应权重保存监听器
            if(expireRateLoader != null) {
                for (SingleTableAtomicLogic atomicLogic : tableAtomicLogics.values()) {
                    expireRateLoader.save(atomicLogic.tableDesc);
                }
            }
            log.info("保存表权重系数成功");
            // 等待任务队列中的任务执行结束
            log.info(PRE_NON_SELECT_THREAD_NAME + "执行队列即将停止...");
            SingleTableAtomicLogic atomicLogic = tableAtomicLogics.values().iterator().next();
            for(int i = 0; i < nonSelectExecutors.length; i++){
                MergingFutureTask<?> endFlagTask = MergingTaskFactory.createEmptyTask(nonSelectExecutors[i], atomicLogic.tableDesc, new TaskContext());
                nonSelectExecutors[i].stop(endFlagTask);
            }
            for(int i = 0; i < selectExecutors.length; i++){
                MergingFutureTask<?> endFlagTask = MergingTaskFactory.createEmptyTask(selectExecutors[i], atomicLogic.tableDesc, new TaskContext());
                selectExecutors[i].stop(endFlagTask);
            }
            try{
                countDownLatch.await();
            }catch (Exception e){
                log.error(PRE_NON_SELECT_THREAD_NAME + "执行队列停止发生异常!", e);
            }
            log.info(PRE_NON_SELECT_THREAD_NAME + "执行队列已停止");
        }
    }

    /**
     * 获取实体对象入口，1.如果缓存中有则从缓存中获取；2.缓存中没有就从数据库中查找
     * @param <T>
     * @param entityClass
     * @param pk 主键
     * @return 实体对象
     */
    public <T extends Cacheable> T getByPK(Class<T> entityClass, Object pk){
        SingleTableAtomicLogic atomicLogic = tableAtomicLogics.get(entityClass);
        if( atomicLogic == null ){
            log.error("非法的实体类class！检查参数table:" + entityClass.getSimpleName() + ", pk:" + pk);
            return null;
        }
        Cacheable result = atomicLogic.getByPK(pk);
        return (T)result;
    }

    /**
     * 获取实体对象集合入口，1.如果缓存中有则从缓存中获取；2.缓存中没有就从数据库中查找；3.数据库中也没有就插入新的数据
     * @param entityClass
     * @param <T>
     * @return 以主键为key实体对象为value的map
     */
    public <T extends Cacheable> Map<Object, T> getByCondition(Class<T> entityClass, Object... cons){
        SingleTableAtomicLogic atomicLogic = tableAtomicLogics.get(entityClass);
        if( atomicLogic == null ){
            log.error("非法的实体类class！检查参数table:" + entityClass.getSimpleName() + ", cons:" + Arrays.toString(cons));
            return null;
        }
        return (Map<Object, T>)atomicLogic.getByCondition(new CacheKey(false, cons));
    }

    /**
     * 插入新的对象，按主键插入，异步操作，仅先更新缓存和标记
     *
     * @param <T>
     * @param entity 要插入的实体对象
     * @return 插入的对象，与传入参数相同
     */
    public <T extends Cacheable> T insert(T entity){
        Class<?> entityClass = entity.getClass();
        SingleTableAtomicLogic atomicLogic = tableAtomicLogics.get(entityClass);
        if( atomicLogic == null ){
            String errorMsg = "非法的实体类class！检查参数：" + entityClass.getSimpleName() + ", entity:" + entity.toJsonStr();
            log.error(errorMsg);
            throw new RuntimeException(errorMsg);
        }
        return (T)atomicLogic.insertEntity(entity);
    }

    /**
     * 提交要更新的对象，按主键更新。先更新缓存对象和更新标记，不会立即更新到数据库中
     *
     * @param entity 要更新的实体对象
     */
    public void update(Cacheable entity){
        Class<?> entityClass = entity.getClass();
        SingleTableAtomicLogic atomicLogic = tableAtomicLogics.get(entityClass);
        if( atomicLogic == null ){
            String errorMsg = "非法的实体类class！检查参数：" + entityClass.getSimpleName() + ", entity:" + entity.toJsonStr();
            log.error(errorMsg);
            throw new RuntimeException(errorMsg);
        }
        atomicLogic.updateEntity(entity);
    }

    /**
     * 删除对象
     *
     * @param <T>
     * @param entity 要删除的实体对象
     */
    public <T extends Cacheable> void delete(T entity){
        Class<?> entityClass = entity.getClass();
        SingleTableAtomicLogic atomicLogic = tableAtomicLogics.get(entityClass);
        if( atomicLogic == null ){
            String errorMsg = "非法的实体类class！检查参数：" + entityClass.getSimpleName() + ", entity:" + entity.toJsonStr();
            log.error(errorMsg);
            throw new RuntimeException(errorMsg);
        }
        atomicLogic.deleteEntity(entity);
    }

    /**
     * 移除entity相关联的缓存：主缓存和条件查询缓存(主要用于处理一些不兼容新缓存组件的老代码，可以在未按照规范使用组件进行增加或删除操作时调用)
     *
     * @param <T>
     * @param entity 要移除所有关联缓存的实体对象
     */
    public <T extends Cacheable> void unloadReferCache(T entity){
        Class<?> entityClass = entity.getClass();
        SingleTableAtomicLogic atomicLogic = tableAtomicLogics.get(entityClass);
        if( atomicLogic == null ){
            String errorMsg = "非法的实体类class！检查参数：" + entityClass.getSimpleName() + ", entity:" + entity.toJsonStr();
            log.error(errorMsg);
            throw new RuntimeException(errorMsg);
        }
        atomicLogic.unloadReferCache(entity);
    }

}
