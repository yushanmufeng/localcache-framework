package yushanmufeng.localcache.atomic;

import yushanmufeng.localcache.util.SimpleTaskExecutor;
import yushanmufeng.localcache.CacheKey;
import yushanmufeng.localcache.Cacheable;
import yushanmufeng.localcache.EntityCacheManager;
import yushanmufeng.localcache.TableDescribe;
import yushanmufeng.localcache.task.TaskContext;
import yushanmufeng.localcache.task.MergingFutureTask;
import yushanmufeng.localcache.task.MergingTaskFactory;

import java.util.List;
import java.util.Map;

/**
 * 根据主键查询
 */
public class AtomicSelectByPk implements IAtomicLogic{

    private final TableDescribe<Cacheable> tableDesc;
    private final EntityCacheManager cache;
    /** 所有要从DB中查询数据的任务队列(主键查询和条件查询)、增删改任务队列 */
    private final SimpleTaskExecutor<MergingFutureTask<?>>[] selectExecutors, nonSelectExecutors;
    /** 主键对应数据的当前状态 pk-ConcurrentStatus;仅在exec0方法中检测和操作此状态 */
    private final Map<CacheKey, WorkingLogic> workingLogics;

    public AtomicSelectByPk(TableDescribe<Cacheable> tableDesc, EntityCacheManager cache, SimpleTaskExecutor<MergingFutureTask<?>>[] selectExecutors, SimpleTaskExecutor<MergingFutureTask<?>>[] nonSelectExecutors, Map<CacheKey, WorkingLogic> workingLogics){
        this.tableDesc = tableDesc;
        this.cache = cache;
        this.selectExecutors = selectExecutors;
        this.nonSelectExecutors = nonSelectExecutors;
        this.workingLogics = workingLogics;
    }

    @SuppressWarnings(value={"unchecked", "rawtypes"})
    @Override
    public void handle(CacheKey key, List<CacheKey> keyList, Cacheable entity, List<Cacheable> entities) {
        WorkingLogic workingLogic = workingLogics.get(key);
        int curState = getCurState(workingLogic);
        // 优先检测冲突状态：插入和删除。查询不改变冲突状态仅做合并来提升并发查询效率
        if(curState == EntityState.DELETED){    // 冲突，当前状态为删除
            // 无数据返回
        }else if(curState == EntityState.LATEST){   // 冲突，当前状态插入
            entityLocal.set(workingLogic.entity);
        }else if(workingLogic != null && workingLogic.hasSelectTask()){ // 有查询任务，合并查询:对相同主键的数据查询并发，会合并为一次select
            MergingFutureTask<Cacheable> task = MergingTaskFactory.createMergingSelectTask(null, tableDesc, TaskContext.DEFAULT_CONTEXT, key, entity, (MergingFutureTask<Cacheable>)workingLogic.selectTask);
            workingLogic.selectingCount ++;
            workingLogic.selectCallback.add(task);
            futureTaskLocal.set(task);
        }else{
            Cacheable entityFromCache = cache.getByPK(tableDesc, key, true);
            if(entityFromCache == null){    // 未命中缓存，提交异步查询数据库任务
                SimpleTaskExecutor executor = getLoadLowestExecutor(selectExecutors, tableDesc);
                MergingFutureTask<Cacheable> task = MergingTaskFactory.createSelectTask(executor, tableDesc, TaskContext.DEFAULT_CONTEXT, key, entity);
                workingLogics.put( key, WorkingLogic.newSelect(task, executor) );
                futureTaskLocal.set(task);
            }else{  // 命中缓存
                entityLocal.set(entityFromCache);
            }
        }
    }

}
