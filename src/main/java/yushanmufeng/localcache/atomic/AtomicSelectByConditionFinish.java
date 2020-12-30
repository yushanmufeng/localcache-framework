package yushanmufeng.localcache.atomic;

import yushanmufeng.localcache.CacheKey;
import yushanmufeng.localcache.Cacheable;
import yushanmufeng.localcache.EntityCacheManager;
import yushanmufeng.localcache.TableDescribe;
import yushanmufeng.localcache.WorkingLogic;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 根据主键查询成功
 */
public class AtomicSelectByConditionFinish implements IAtomicLogic{

    private final TableDescribe<Cacheable> tableDesc;
    private final EntityCacheManager cache;
    /** 主键对应数据的当前状态 pk-ConcurrentStatus;仅在exec0方法中检测和操作此状态 */
    private final Map<CacheKey, WorkingLogic> workingLogics;

    public AtomicSelectByConditionFinish(TableDescribe<Cacheable> tableDesc, EntityCacheManager cache, Map<CacheKey, WorkingLogic> workingLogics){
        this.tableDesc = tableDesc;
        this.cache = cache;
        this.workingLogics = workingLogics;
    }

    @Override
    public void handle(CacheKey key, Cacheable entity, List<Cacheable> entities) {
        List<Object> pks = cache.getByCondition(tableDesc, key, true);    // 优先使用缓存
        if(pks == null){   // 缓存未命中, 使用db查询结果并更新缓存
            pks = new ArrayList<>();
            if(entities != null){
                for(Cacheable entityFromDb : entities){
                    Object pk = tableDesc.tableStrategy.getPrimaryKey(entityFromDb);
                    pks.add(pk);
                    CacheKey entityKey = new CacheKey(true, pk);
                    if(cache.getByPK(tableDesc, entityKey, false) == null){    // 如果实体类也无法命中，则将实体类也加入缓存
                        entityFromDb._setStatus(EntityState.LATEST);
                        cache.cacheCore(tableDesc, entityKey, entityFromDb);
                    }
                }
            }
            cache.cacheCondition(tableDesc, key, pks);
        }
        WorkingLogic workingLogic = workingLogics.get(key);
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
        pksLocal.set(pks);
    }

}
