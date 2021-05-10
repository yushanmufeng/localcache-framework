package yushanmufeng.localcache.atomic;

import yushanmufeng.localcache.CacheKey;
import yushanmufeng.localcache.Cacheable;
import yushanmufeng.localcache.EntityCacheManager;
import yushanmufeng.localcache.SingleTableAtomicLogic;
import yushanmufeng.localcache.TableDescribe;
import yushanmufeng.localcache.task.TaskContext;
import yushanmufeng.localcache.task.MergingFutureTask;
import yushanmufeng.localcache.task.MergingTaskFactory;
import yushanmufeng.localcache.util.SimpleTaskExecutor;

import java.util.List;
import java.util.Map;

/**
 * 根据主键删除
 */
public class AtomicDeleteByPk implements IAtomicLogic{

    private final SingleTableAtomicLogic atomicLogic;
    private final TableDescribe<Cacheable> tableDesc;
    private final EntityCacheManager cache;
    /** 所有要从DB中查询数据的任务队列(主键查询和条件查询)、增删改任务队列 */
    private final SimpleTaskExecutor<MergingFutureTask<?>>[] nonSelectExecutors;
    /** 主键对应数据的当前状态 pk-ConcurrentStatus;仅在exec0方法中检测和操作此状态 */
    private final Map<CacheKey, WorkingLogic> workingLogics;

    public AtomicDeleteByPk(SingleTableAtomicLogic atomicLogic, TableDescribe<Cacheable> tableDesc, EntityCacheManager cache, SimpleTaskExecutor<MergingFutureTask<?>>[] nonSelectExecutors, Map<CacheKey, WorkingLogic> workingLogics){
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
        // 优先检测冲突状态：插入和删除。查询不改变冲突状态仅做合并来提升并发查询效率
        if(entity._getStatus()==EntityState.DELETED||entity._getStatus()==EntityState.GET_READY){   // 数据异常
            throw new RuntimeException("删除数据状态异常！table:" + tableDesc.entityName + ", CacheKey:" + (key==null?"null":key.toString()) + ", stateType:" + entity._getStatus());
        }
        if(curState == EntityState.DELETED){     // 冲突，当前状态为已删除
            log.warn("重复删除主键数据,已合并删除请求！请检查相关逻辑：table:" + tableDesc.entityName + ", CacheKey:" + (key==null?"null":key.toString()), new Exception());
        }else{  // 冲突，当前状态存在; 或进行查询任务中
            if(workingLogic == null){
                workingLogic = new WorkingLogic();
                workingLogics.put(key, workingLogic);
            }
            entity._setStatus(EntityState.DELETED);
            workingLogic.entity = entity;
            workingLogic.deletingCount ++;
            workingLogic.otherExecutor = workingLogic.otherExecutor == null ? getLoadLowestExecutor(nonSelectExecutors, tableDesc) : workingLogic.otherExecutor;
            MergingFutureTask<Cacheable> deleteTask = MergingTaskFactory.createDeleteTask(atomicLogic, workingLogic.otherExecutor, tableDesc, new TaskContext(), key, entity);
            workingLogic.otherExecutor.put(deleteTask);
            futureTaskLocal.set(deleteTask);
            // 删除缓存
            cache.unloadCache(tableDesc, key);
            cache.whenDeleteEntity(tableDesc, entity);
        }
    }

}
