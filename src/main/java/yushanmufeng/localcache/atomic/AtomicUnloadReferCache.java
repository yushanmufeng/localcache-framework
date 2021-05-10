package yushanmufeng.localcache.atomic;

import yushanmufeng.localcache.CacheKey;
import yushanmufeng.localcache.Cacheable;
import yushanmufeng.localcache.EntityCacheManager;
import yushanmufeng.localcache.TableDescribe;

import java.util.List;

/**
 * 卸载entity相关联的缓存：主缓存和条件查询缓存
 * 参数只要传entity即可
 */
public class AtomicUnloadReferCache implements IAtomicLogic{

    private final TableDescribe<Cacheable> tableDesc;
    private final EntityCacheManager cache;

    public AtomicUnloadReferCache(TableDescribe<Cacheable> tableDesc, EntityCacheManager cache){
        this.tableDesc = tableDesc;
        this.cache = cache;
    }

    @Override
    public void handle(CacheKey key, List<CacheKey> keyList, Cacheable entity, List<Cacheable> entities) {
        // 删除条件查询缓存
        List<CacheKey> conditionKeys = tableDesc.tableStrategy.getConditionKeys(entity);
        if(conditionKeys != null && conditionKeys.size() > 0){
            for(CacheKey conditionKey : conditionKeys){
                cache.unloadCache(tableDesc, conditionKey);
            }
        }
        // 删除主键缓存
        cache.unloadCache(tableDesc, new CacheKey(true, tableDesc.tableStrategy.getPrimaryKey(entity)));
    }

}
