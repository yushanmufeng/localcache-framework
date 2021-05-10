package yushanmufeng.localcache.atomic;

import yushanmufeng.localcache.CacheKey;
import yushanmufeng.localcache.Cacheable;
import yushanmufeng.localcache.EntityCacheManager;
import yushanmufeng.localcache.TableDescribe;

import java.util.List;

/**
 * 汇总占用缓存的大小
 */
public class AtomicSumMemBytes implements IAtomicLogic{

    private final TableDescribe<Cacheable> tableDesc;
    private final EntityCacheManager cache;

    public AtomicSumMemBytes(TableDescribe<Cacheable> tableDesc, EntityCacheManager cache){
        this.tableDesc = tableDesc;
        this.cache = cache;
    }

    @Override
    public void handle(CacheKey key, List<CacheKey> keyList, Cacheable entity, List<Cacheable> entities) {
        cache.sumMem(tableDesc);
    }

}
