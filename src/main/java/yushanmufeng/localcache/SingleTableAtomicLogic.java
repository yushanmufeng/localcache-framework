package yushanmufeng.localcache;

import yushanmufeng.localcache.atomic.*;
import yushanmufeng.localcache.util.CLHLock;
import yushanmufeng.localcache.util.SimpleTaskExecutor;
import yushanmufeng.localcache.config.LocalCacheConfig;
import yushanmufeng.localcache.task.MergingFutureTask;
import yushanmufeng.localcache.task.MergingTaskFactory;
import yushanmufeng.localcache.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 单表的原子操作逻辑
 * 每张表都会创建一个此类的实例，用于执行针对某一张表的所有对缓存和DB的操作
 */
@SuppressWarnings(value={"unchecked", "rawtypes"})
public class SingleTableAtomicLogic {

    private static final Logger log = LoggerFactory.getLogger(SingleTableAtomicLogic.class);

    public final TableDescribe<Cacheable> tableDesc;

    private final EntityCacheManager cache;

    /** 所有要从DB中查询数据的任务队列(主键查询和条件查询)、增删改任务队列 */
    private final SimpleTaskExecutor<MergingFutureTask<?>>[] selectExecutors, nonSelectExecutors;

    /** 自旋锁，因为主线程全部都是内存操作，查询操作会转换未异步回调任务执行，所以采用自旋锁用来保证较高的性能 */
    private final CLHLock lock = new CLHLock();

    /** 主键对应数据的当前正在执行的异步逻辑 pk-ConcurrentStatus;仅在exec0方法中检测和操作此状态 */
    private final Map<CacheKey, WorkingLogic> workingLogics = new HashMap<>(256);

    /** 所有原子操作集合 */
    private final Map<Integer, IAtomicLogic> logicsMap = new HashMap<>();

    /** 上次计算汇总单表的内存占用的时间，单位毫秒 */
    private final AtomicLong lastSumMemTime = new AtomicLong(System.currentTimeMillis());
    /** 计算汇总单表的内存占用操作的: 最小时间间隔, 最大时间间隔 */
    private final long MIN_SUM_MEM_MS, MAX_SUM_MEM_MS;

    /** 上次检测缓存过期时间，单位毫秒 */
    private final AtomicLong lastCheckExpireTime = new AtomicLong(System.currentTimeMillis());
    /** 检测缓存过期的时间间隔: 最小时间间隔，最大时间间隔 */
    private final long MIN_CHECK_EXPIRE_MS, MAX_CHECK_EXPIRE_MS;

    /** 30秒的毫秒数常量 */
    private static final long SECOND_30 = 30 * 1000;

    public SingleTableAtomicLogic(LocalCacheConfig config, TableDescribe<Cacheable> tableDesc, EntityCacheManager cache, SimpleTaskExecutor<MergingFutureTask<?>>[] selectExecutors, SimpleTaskExecutor<MergingFutureTask<?>>[] nonSelectExecutors){
        this.tableDesc = tableDesc;
        this.cache = cache;
        this.selectExecutors = selectExecutors;
        this.nonSelectExecutors = nonSelectExecutors;
        MIN_SUM_MEM_MS = config.sumOneTableMemMs;
        MAX_SUM_MEM_MS = MIN_SUM_MEM_MS + (MIN_SUM_MEM_MS < SECOND_30 ? MIN_SUM_MEM_MS : SECOND_30);
        MIN_CHECK_EXPIRE_MS = config.checkExpireMs;
        MAX_CHECK_EXPIRE_MS = MIN_CHECK_EXPIRE_MS + (MIN_CHECK_EXPIRE_MS < SECOND_30 ? MIN_CHECK_EXPIRE_MS : SECOND_30);
        initAtomicLogic();
    }

    /** 初始化所有类型的原子操作逻辑 */
    private void initAtomicLogic(){
        // 根据主键查询
        logicsMap.put(IAtomicLogic.SELECT_BY_PK, new AtomicSelectByPk(tableDesc, cache, selectExecutors, nonSelectExecutors, workingLogics));
        // 根据主键查询-完成
        logicsMap.put(IAtomicLogic.SELECT_BY_PK_FINISH, new AtomicSelectByPkFinish(tableDesc, cache, workingLogics));
        // 根据主键更新
        logicsMap.put(IAtomicLogic.UPDATE_BY_PK, new AtomicUpdateByPk(this, tableDesc, cache, nonSelectExecutors, workingLogics));
        // 根据主键更新-完成
        logicsMap.put(IAtomicLogic.UPDATE_BY_PK_FINISH, new AtomicUpdateByPkFinish( workingLogics));
        // 根据主键插入
        logicsMap.put(IAtomicLogic.INSERT_BY_PK, new AtomicInsertByPk(this, tableDesc, cache, nonSelectExecutors, workingLogics));
        // 根据主键插入-完成
        logicsMap.put(IAtomicLogic.INSERT_BY_PK_FINISH, new AtomicInsertByPkFinish(workingLogics));
        // 根据主键删除
        logicsMap.put(IAtomicLogic.DELETE_BY_PK, new AtomicDeleteByPk(this, tableDesc, cache, nonSelectExecutors, workingLogics));
        // 根据主键删除-完成
        logicsMap.put(IAtomicLogic.DELETE_BY_PK_FINISH, new AtomicDeleteByPkFinish(workingLogics));
        // 根据条件查询
        logicsMap.put(IAtomicLogic.SELECT_BY_CONDITION, new AtomicSelectByCondition(tableDesc, cache, selectExecutors, nonSelectExecutors, workingLogics));
        // 根据条件查询-完成
        logicsMap.put(IAtomicLogic.SELECT_BY_CONDITION_FINISH, new AtomicSelectByConditionFinish(tableDesc, cache, workingLogics));
        // 卸载关联缓存
        logicsMap.put(IAtomicLogic.UNLOAD_REFER_CACHE, new AtomicUnloadReferCache(tableDesc, cache));
        // 计算汇总缓存使用的内存大小
        logicsMap.put(IAtomicLogic.SUM_MEM_BYTES, new AtomicSumMemBytes(tableDesc, cache));
        // 检测缓存过期
        logicsMap.put(IAtomicLogic.CHECK_CACHE_EXPIRE, new AtomicSumMemBytes(tableDesc, cache));
    }

    /** 根据主键查询入口 */
    public Cacheable getByPK(Object pk){
        CacheKey cacheKey = new CacheKey(true, pk);
        exec(IAtomicLogic.SELECT_BY_PK, cacheKey, null, null);
        Cacheable entity = IAtomicLogic.entityLocal.get();
        if(entity == null){
            MergingFutureTask<?> futureTask = IAtomicLogic.futureTaskLocal.get();
            if(futureTask != null) {
                try {
                    exec(IAtomicLogic.SELECT_BY_PK_FINISH, cacheKey, (Cacheable) futureTask.get(), null);
                } catch (Exception e) {
                    log.error("Select " + tableDesc.tableStrategy.getEntityClass().getSimpleName() + " By Pk Is Error,pk: " + pk, e);
                    throw new RuntimeException(e);
                }
                entity = IAtomicLogic.entityLocal.get();
            }
        }
        IAtomicLogic.clearLocal();
        return entity;
    }

    /** 根据条件擦汗寻入口 */
    public Map<Object, Cacheable> getByCondition(CacheKey cacheKey){
        exec(IAtomicLogic.SELECT_BY_CONDITION, cacheKey, null, null);
        List<Object> pks = IAtomicLogic.pksLocal.get(); // 命中缓存
        if(pks == null){
            // 未命中缓存，等待异步执行查询db任务返回结果
            MergingFutureTask<List<Cacheable>> futureTask = (MergingFutureTask<List<Cacheable>>)IAtomicLogic.futureTaskLocal.get();
            try {
                List<Cacheable> entitiesFromDb = futureTask.get();
                exec(IAtomicLogic.SELECT_BY_CONDITION_FINISH, cacheKey, null, entitiesFromDb);
            }catch (Exception e){
                log.error( "Select "+ tableDesc.tableStrategy.getEntityClass().getSimpleName() +" By Condition Is Error,condition: " + cacheKey.toString(), e);
                throw new RuntimeException(e);
            }
            pks = IAtomicLogic.pksLocal.get();
        }
        Map<Object, Cacheable> entitiesMap = new LinkedHashMap<>();
        if(pks != null){
            for(Object pk : pks){
                entitiesMap.put(pk, getByPK(pk));
            }
        }
        IAtomicLogic.clearLocal();
        return entitiesMap;
    }

    /** 插入实体对象入口 */
    public Cacheable insertEntity(Cacheable entity){
        exec(IAtomicLogic.INSERT_BY_PK, new CacheKey(true, tableDesc.tableStrategy.getPrimaryKey(entity)), entity, null);
        Cacheable result = IAtomicLogic.entityLocal.get();
        IAtomicLogic.clearLocal();
        return result;
    }

    /** 更新实体对象入口 */
    public void updateEntity(Cacheable entity){
        exec(IAtomicLogic.UPDATE_BY_PK, new CacheKey(true, tableDesc.tableStrategy.getPrimaryKey(entity)), entity, null);
        IAtomicLogic.clearLocal();
    }

    /** 删除实体对象入口 */
    public void deleteEntity(Cacheable entity){
        exec(IAtomicLogic.DELETE_BY_PK, new CacheKey(true, tableDesc.tableStrategy.getPrimaryKey(entity)), entity, null);
        IAtomicLogic.clearLocal();
    }

    /** 卸载缓存 */
    public void unloadReferCache(Cacheable entity){
        exec(IAtomicLogic.UNLOAD_REFER_CACHE, null, entity, null);
    }

    /**
     * 执行任务的核心方法
     * 该方法为同步方法，所以涉及db的耗时操作会放到异步队列中执行
     * 每张表的核心执行任务方法全部为内存操作，为了处理逻辑清晰和性能高效，采用自旋锁的方式保证该方法的核心逻辑代码执行环境不会并发
     * 因为使用的自旋锁，此方法禁止重入，否则会死循环
     *
     * @param  execType 操作类型
     * @param  key 查询条件
     * @param  entity 实体对象
     * @param  entities 实体对象（条件查询结果）
     * @return 返回对象类型与执行的任务类型相关
     */
    public void exec(int execType, CacheKey key, Cacheable entity, List<Cacheable> entities){
        // ======== 原子操作start ========
        boolean isBusyEnd;
        boolean isBusyStart = lock.lock();
        try{
            logicsMap.get(execType).handle(key, entity, entities);
        }catch(Exception e) {
            String message = "ExecType:" + execType + ",table:" + tableDesc.entityName + ",CacheKey:" + (key==null?"null":key.toString());
            log.error(message, e);
            throw new RuntimeException(message, e);
        }finally {
            isBusyEnd = lock.unlock();
        }
        // ======== 原子操作end ========
        long currentMs = System.currentTimeMillis();
        long lastSumMs = lastSumMemTime.get();
        boolean isTimeoutMin = currentMs - lastSumMs >= MIN_SUM_MEM_MS;
        boolean isTimeoutMax = currentMs - lastSumMs >= MAX_SUM_MEM_MS;
        long lastCheckMs = lastCheckExpireTime.get();
        boolean isCheckMin = currentMs - lastCheckMs >= MIN_CHECK_EXPIRE_MS;
        boolean isCheckMax = currentMs - lastCheckMs >= MAX_CHECK_EXPIRE_MS;
        // 计算缓存内存占用情况
        if( (!isBusyStart && !isBusyEnd && isTimeoutMin) || isTimeoutMax ){   // 开始结束时都没有锁竞争，则可以暂时认为任务队列不是很繁忙
            if(lastSumMemTime.compareAndSet(lastSumMs, currentMs)){ // 添加一个汇总计算,更新最新的汇总计算时间(并发时只有一个线程可以成功, 不会重发添加内存检测任务)
                SingleTableAtomicLogic atomicLogic = this;
                SimpleTaskExecutor executor = SimpleTaskExecutor.getLoadLowestExecutor(nonSelectExecutors);
                MergingFutureTask<?> sumMemBytesTask = MergingTaskFactory.createSumMemBytesTask(atomicLogic, executor, tableDesc, new TaskContext());
                executor.put(sumMemBytesTask);
            }
        }
        // 检查缓存过期
        else if( (!isBusyStart && !isBusyEnd && isCheckMin) || isCheckMax ){
            if(lastCheckExpireTime.compareAndSet(lastCheckMs, currentMs)){
                SingleTableAtomicLogic atomicLogic = this;
                SimpleTaskExecutor executor = SimpleTaskExecutor.getLoadLowestExecutor(nonSelectExecutors);
                MergingFutureTask<?> checkExpireTask = MergingTaskFactory.createSumMemBytesTask(atomicLogic, executor, tableDesc, new TaskContext());
                executor.put(checkExpireTask);
            }
        }

    }

}
