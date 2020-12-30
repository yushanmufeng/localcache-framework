package yushanmufeng.localcache.atomic;

import yushanmufeng.localcache.util.SimpleTaskExecutor;
import yushanmufeng.localcache.CacheKey;
import yushanmufeng.localcache.Cacheable;
import yushanmufeng.localcache.EntityCacheManager;
import yushanmufeng.localcache.SingleTableAtomicLogic;
import yushanmufeng.localcache.TableDescribe;
import yushanmufeng.localcache.task.TaskContext;
import yushanmufeng.localcache.WorkingLogic;
import yushanmufeng.localcache.task.MergingFutureTask;
import yushanmufeng.localcache.task.MergingTaskFactory;

import java.util.List;
import java.util.Map;

/**
 * 根据主键插入
 */
public class AtomicInsertByPk implements IAtomicLogic{

    private final SingleTableAtomicLogic atomicLogic;
    private final TableDescribe<Cacheable> tableDesc;
    private final EntityCacheManager cache;
    /** 所有要从DB中查询数据的任务队列(主键查询和条件查询)、增删改任务队列 */
    private final SimpleTaskExecutor<MergingFutureTask<?>>[] nonSelectExecutors;
    /** 主键对应数据的当前状态 pk-ConcurrentStatus;仅在exec0方法中检测和操作此状态 */
    private final Map<CacheKey, WorkingLogic> workingLogics;

    public AtomicInsertByPk(SingleTableAtomicLogic atomicLogic, TableDescribe<Cacheable> tableDesc, EntityCacheManager cache, SimpleTaskExecutor<MergingFutureTask<?>>[] nonSelectExecutors, Map<CacheKey, WorkingLogic> workingLogics){
        this.atomicLogic = atomicLogic;
        this.tableDesc = tableDesc;
        this.cache = cache;
        this.nonSelectExecutors = nonSelectExecutors;
        this.workingLogics = workingLogics;
    }

    @Override
    public void handle(CacheKey key, Cacheable entity, List<Cacheable> entities) {
        WorkingLogic workingLogic = workingLogics.get(key);
        Cacheable cacheEntity = cache.getByPK(tableDesc, key, false);
        int curState = getCurState(workingLogic);
        TaskContext context = new TaskContext();
        // 优先检测冲突状态：插入和删除。查询不改变冲突状态仅做合并来提升并发查询效率
        if(curState == EntityState.LATEST || (cacheEntity != null && cacheEntity._getStatus() == EntityState.LATEST)){   // 冲突，当前状态存在
            throw new RuntimeException( "禁止插入重复主键数据！table:" + tableDesc.entityName + ", CacheKey:" + (key==null?"null":key.toString()) );
        }else if(entity._getStatus() != EntityState.GET_READY){
            throw new RuntimeException( "插入新的数据必须是新的实体实例，不能复用其他实例引用！table:" + tableDesc.entityName + ", CacheKey:" + (key==null?"null":key.toString()) );
        }
        MergingFutureTask<Cacheable> insertTask = null;
        if(curState == EntityState.DELETED){    // 冲突，当前状态为已删除
            entity._setStatus(EntityState.LATEST);
            workingLogic.entity = entity;
            workingLogic.insertingCount ++;
            insertTask = MergingTaskFactory.createInsertTask(atomicLogic, workingLogic.otherExecutor, tableDesc, context, key, entity);
            workingLogic.otherExecutor.put(insertTask);
            cache.cacheCore(tableDesc, key, entity);
            cache.whenInsertEntity(tableDesc, entity);
        }else if(workingLogic != null && workingLogic.hasSelectTask()){ // 有查询任务
            entity._setStatus(EntityState.LATEST);
            workingLogic.entity = entity;
            workingLogic.insertingCount ++;
            workingLogic.otherExecutor = getLoadLowestExecutor(nonSelectExecutors, tableDesc);
            insertTask = MergingTaskFactory.createInsertTask(atomicLogic, workingLogic.otherExecutor, tableDesc, context, key, entity);
            workingLogic.otherExecutor.put(insertTask);
            cache.cacheCore(tableDesc, key, entity);
            cache.whenInsertEntity(tableDesc, entity);
        }else {   // 无任何进行中的任务
            entity._setStatus(EntityState.LATEST);
            SimpleTaskExecutor<MergingFutureTask<?>> executor = getLoadLowestExecutor(nonSelectExecutors, tableDesc);
            insertTask = MergingTaskFactory.createInsertTask(atomicLogic, executor, tableDesc, context, key, entity);
            workingLogics.put( key, WorkingLogic.newInsert(insertTask, executor, entity) );
            cache.cacheCore(tableDesc, key, entity);
            cache.whenInsertEntity(tableDesc, entity);
        }
        entityLocal.set(entity);
        futureTaskLocal.set(insertTask);
    }

}
