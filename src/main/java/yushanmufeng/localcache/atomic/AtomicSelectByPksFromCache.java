package yushanmufeng.localcache.atomic;

import yushanmufeng.localcache.CacheKey;
import yushanmufeng.localcache.Cacheable;
import yushanmufeng.localcache.EntityCacheManager;
import yushanmufeng.localcache.TableDescribe;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 根据主键查询（多个主键，仅从缓存中获取）
 */
public class AtomicSelectByPksFromCache implements IAtomicLogic{

    private final TableDescribe<Cacheable> tableDesc;
    private final EntityCacheManager cache;

    public AtomicSelectByPksFromCache(TableDescribe<Cacheable> tableDesc, EntityCacheManager cache){
        this.tableDesc = tableDesc;
        this.cache = cache;
    }

    @SuppressWarnings(value={"unchecked", "rawtypes"})
    @Override
    public void handle(CacheKey key, List<CacheKey> keyList, Cacheable entity, List<Cacheable> entities) {
        Map<Object, Cacheable> results = new HashMap<>();
        for(CacheKey cacheKey : keyList){
            Cacheable entityFromCache = cache.getByPK(tableDesc, cacheKey, true);
            if(entityFromCache != null){    // 命中缓存，加入返回列表
                results.put(cacheKey.keys[0], entityFromCache);
            }
        }
        entitiesLocal.set(results);
    }

}

