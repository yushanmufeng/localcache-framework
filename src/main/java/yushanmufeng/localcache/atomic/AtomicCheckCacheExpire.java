package yushanmufeng.localcache.atomic;

import yushanmufeng.localcache.CacheKey;
import yushanmufeng.localcache.Cacheable;
import yushanmufeng.localcache.EntityCacheManager;
import yushanmufeng.localcache.TableDescribe;

import java.util.List;

/**
 * 检测缓存过期
 */
public class AtomicCheckCacheExpire implements IAtomicLogic{

    private final TableDescribe<Cacheable> tableDesc;
    private final EntityCacheManager cache;

    public AtomicCheckCacheExpire(TableDescribe<Cacheable> tableDesc, EntityCacheManager cache){
        this.tableDesc = tableDesc;
        this.cache = cache;
    }

    @Override
    public void handle(CacheKey key, Cacheable entity, List<Cacheable> entities) {
        cache.checkExpire(tableDesc);
    }

}
