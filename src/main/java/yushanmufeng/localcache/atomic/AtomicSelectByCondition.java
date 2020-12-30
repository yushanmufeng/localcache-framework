package yushanmufeng.localcache.atomic;

import yushanmufeng.localcache.util.SimpleTaskExecutor;
import yushanmufeng.localcache.CacheKey;
import yushanmufeng.localcache.Cacheable;
import yushanmufeng.localcache.EntityCacheManager;
import yushanmufeng.localcache.TableDescribe;
import yushanmufeng.localcache.task.TaskContext;
import yushanmufeng.localcache.WorkingLogic;
import yushanmufeng.localcache.task.MergingFutureTask;
import yushanmufeng.localcache.task.MergingTaskFactory;

import java.util.List;
import java.util.Map;

/**
 * 根据条件查询
 */
public class AtomicSelectByCondition implements IAtomicLogic{

    private final TableDescribe<Cacheable> tableDesc;
    private final EntityCacheManager cache;
    /** 所有要从DB中查询数据的任务队列(主键查询和条件查询)、增删改任务队列 */
    private final SimpleTaskExecutor<MergingFutureTask<?>>[] selectExecutors, nonSelectExecutors;
    /** 主键对应数据的当前状态 pk-ConcurrentStatus;仅在exec0方法中检测和操作此状态 */
    private final Map<CacheKey, WorkingLogic> workingLogics;

    public AtomicSelectByCondition(TableDescribe<Cacheable> tableDesc, EntityCacheManager cache, SimpleTaskExecutor<MergingFutureTask<?>>[] selectExecutors, SimpleTaskExecutor<MergingFutureTask<?>>[] nonSelectExecutors, Map<CacheKey, WorkingLogic> workingLogics){
        this.tableDesc = tableDesc;
        this.cache = cache;
        this.selectExecutors = selectExecutors;
        this.nonSelectExecutors = nonSelectExecutors;
        this.workingLogics = workingLogics;
    }

    @SuppressWarnings(value={"unchecked", "rawtypes"})
    @Override
    public void handle(CacheKey key, Cacheable entity, List<Cacheable> entities) {
        WorkingLogic workingLogic = workingLogics.get(key);
        List<Object> cachePks = cache.getByCondition(tableDesc, key, false);
        if(cachePks != null){   // 有缓存，直接使用缓存
            pksLocal.set(cachePks);
        }else if(workingLogic != null && workingLogic.hasSelectTask()){ // 无缓存，有并发查询; 合并查询
            MergingFutureTask<?> task = MergingTaskFactory.createMergingSelectByConditionTask(null, tableDesc, TaskContext.DEFAULT_CONTEXT, key, entity, (MergingFutureTask<List<Cacheable>>)workingLogic.selectTask);
            workingLogic.selectingCount ++;
            workingLogic.selectCallback.add(task);
            futureTaskLocal.set(task);
        }else{  // 无缓存，无并发, 去数据库查询
            SimpleTaskExecutor<MergingFutureTask<?>> executor = getLoadLowestExecutor(selectExecutors, tableDesc);
            MergingFutureTask<?> task = MergingTaskFactory.createSelectByConditionTask(executor, tableDesc, TaskContext.DEFAULT_CONTEXT, key, entity);
            workingLogics.put( key, WorkingLogic.newSelect(task, getLoadLowestExecutor(selectExecutors, tableDesc)) );
            futureTaskLocal.set(task);
        }
    }

}
