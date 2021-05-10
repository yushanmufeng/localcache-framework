package yushanmufeng.localcache.atomic;

import yushanmufeng.localcache.util.SimpleTaskExecutor;
import yushanmufeng.localcache.CacheKey;
import yushanmufeng.localcache.Cacheable;
import yushanmufeng.localcache.EntityCacheManager;
import yushanmufeng.localcache.SingleTableAtomicLogic;
import yushanmufeng.localcache.TableDescribe;
import yushanmufeng.localcache.task.TaskContext;
import yushanmufeng.localcache.task.MergingFutureTask;
import yushanmufeng.localcache.task.MergingTaskFactory;

import java.util.List;
import java.util.Map;

/**
 * 根据主键更新
 */
public class AtomicUpdateByPk implements IAtomicLogic{

    private final SingleTableAtomicLogic atomicLogic;
    private final TableDescribe<Cacheable> tableDesc;
    private final EntityCacheManager cache;
    /** 从DB中增删改任务队列 */
    private final SimpleTaskExecutor<MergingFutureTask<?>>[] nonSelectExecutors;
    /** 主键对应数据的当前状态 pk-ConcurrentStatus;仅在exec0方法中检测和操作此状态 */
    private final Map<CacheKey, WorkingLogic> workingLogics;

    public AtomicUpdateByPk(SingleTableAtomicLogic atomicLogic, TableDescribe<Cacheable> tableDesc, EntityCacheManager cache, SimpleTaskExecutor<MergingFutureTask<?>>[] nonSelectExecutors, Map<CacheKey, WorkingLogic> workingLogics){
        this.atomicLogic = atomicLogic;
        this.tableDesc = tableDesc;
        this.cache = cache;
        this.nonSelectExecutors = nonSelectExecutors;
        this.workingLogics = workingLogics;
    }

    @Override
    public void handle(CacheKey key, List<CacheKey> keyList, Cacheable entity, List<Cacheable> entities) {
        WorkingLogic workingLogic = workingLogics.get(key);
        int curState = getCurState(workingLogic);
        if(entity._getStatus()==EntityState.DELETED||entity._getStatus()==EntityState.GET_READY){   // 数据异常
            throw new RuntimeException("更新数据状态异常！table:" + tableDesc.entityName + ", CacheKey:" + (key==null?"null":key.toString()) + ", stateType:" + entity._getStatus());
        }
        if(curState == EntityState.DELETED){    // 当前状态不为已删除状态, 才做更新操作
            log.warn("异步更新实体对象已被删除！table:" + tableDesc.entityName + ", CacheKey:" + (key==null?"null":key.toString()));
        }
        entity._setStatus(EntityState.LATEST);
        cache.cacheCore(tableDesc, key, entity);
        if(workingLogic == null){
            workingLogic = new WorkingLogic();
            workingLogics.put(key, workingLogic);
        }
        workingLogic.entity = entity;
        workingLogic.updatingCount ++;
        workingLogic.otherExecutor = workingLogic.otherExecutor == null ? getLoadLowestExecutor(nonSelectExecutors, tableDesc) : workingLogic.otherExecutor;
        TaskContext context = new TaskContext();
        MergingFutureTask<Cacheable> task = MergingTaskFactory.createUpdateTask(atomicLogic, workingLogic.otherExecutor, tableDesc, context, key, entity);
        workingLogic.otherExecutor.put(task);
        futureTaskLocal.set(task);
    }

}
