package yushanmufeng.localcache.atomic;

import yushanmufeng.localcache.CacheKey;
import yushanmufeng.localcache.Cacheable;
import yushanmufeng.localcache.EntityCacheManager;
import yushanmufeng.localcache.TableDescribe;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 根据主键查询成功
 */
public class AtomicSelectByPkFinish implements IAtomicLogic{

    private final TableDescribe<Cacheable> tableDesc;
    private final EntityCacheManager cache;
    /** 主键对应数据的当前状态 pk-ConcurrentStatus;仅在exec0方法中检测和操作此状态 */
    private final Map<CacheKey, WorkingLogic> workingLogics;

    public AtomicSelectByPkFinish(TableDescribe<Cacheable> tableDesc, EntityCacheManager cache, Map<CacheKey, WorkingLogic> workingLogics){
        this.tableDesc = tableDesc;
        this.cache = cache;
        this.workingLogics = workingLogics;
    }

    @Override
    public void handle(CacheKey key, Cacheable entity, List<Cacheable> entities) {
        WorkingLogic workingLogic = workingLogics.get(key);
        int curState = getCurState(workingLogic);
        // 优先检测冲突状态：插入和删除。查询不改变冲突状态仅做合并来提升并发查询效率
        if(curState == EntityState.DELETED){    // 冲突，当前状态为删除
            // 无数据返回
        }else if(curState == EntityState.LATEST){   // 冲突，当前状态为插入或更新
            log.warn("并发逻辑异常，查询未结束时进行插入或更新操作，请检查相关代码！table：" + tableDesc.entityName + ", key:" + key.toString(), new Exception());
            entityLocal.set(workingLogic.entity);
        }else if(curState == EntityState.DETACHED){ // 状态为不存在
            if(entity != null){
                entity._setStatus(EntityState.LATEST);
                cache.cacheCore(tableDesc, key, entity);
                entityLocal.set(entity);
            }
        }
        workingLogic.selectingCount --;
        if(workingLogic.clearTempAttrByTaskCount()){   // 没有进行中的查询操作和增删改操作，可以删掉整个记录对象
            workingLogics.remove(key);
        }
        // 触发并删除所有回调
        if(workingLogic.selectCallback.size() > 0){
            List<Runnable> callbacks = new ArrayList<>(workingLogic.selectCallback);
            workingLogic.selectCallback.clear();
            for(Runnable callback : callbacks){
                callback.run();
            }
        }
    }

}
