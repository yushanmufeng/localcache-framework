package yushanmufeng.localcache;

import yushanmufeng.localcache.config.LocalCacheConfig;
import org.apache.lucene.util.RamUsageEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yushanmufeng.localcache.util.MapRandomAccessUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Entity的缓存管理, 是实际缓存实体对象的地方
 * 不使用LRU的方式(使用的类似方式，取一部分数据然后将移除其中最濒临过期的数据)，因为 1.比较难以控制缓存区域的大小； 2.无法针对每张表进行单独优化
 */
public class EntityCacheManager {

    private static final Logger log = LoggerFactory.getLogger(EntityCacheManager.class);

    /** 缓存可用的总字节数，用于限制内存占用 */
    public final long MAX_WEIGHT_BYTES;

    /**
     * 已经使用了的缓存与最大缓存的占比，逻辑上分为三档：0%~75%; 75~90%; 95%~，处于不同挡时会应用不同的策略，详见 {@link TableDescribe}
     * 根据此占比会计算出过期权重，用于控制缓存的内存空间占用
     */
    public volatile int memRatio = 0;

    /** 缓存map的初始参数，防止频繁发生扩容 */
    private final int INITIAL_CAPACITY;
    private static final float LOAD_FACTOR = 0.98f;

    /** 缓存key,因为从缓存中读取时无法获得真正的key; 所以移除主缓存和查询缓存时要同时移除key的缓存 */
    private final Map<TableDescribe<Cacheable>, Map<CacheKey, CacheKey>> keyCache = new ConcurrentHashMap<>(128, LOAD_FACTOR);
    /** 主缓存 [表， [主键cacheKey， 实体对象]] */
    private final Map<TableDescribe<Cacheable>, Map<CacheKey, Cacheable>> coreCache = new HashMap<>(128, LOAD_FACTOR);
    /** 条件查询缓存 [表， [查询条件cacheKey, [实体对象主键数组]]] */
    private final Map<TableDescribe<Cacheable>, Map<CacheKey, List<Object>>> conditionCache = new HashMap<>(128, LOAD_FACTOR);

    /** 上次计算汇总所有表的内存占用的时间，单位毫秒 */
    private final AtomicLong lastSumMemTime = new AtomicLong(System.currentTimeMillis());
    /** 计算汇总所有表的内存占用操作的时间间隔 */
    private final long SUM_MEM_MS;
    /** 单轮检测过期最大条目数，越大则所需时间越长 */
    private final int ONE_ROLL_CHECK_MAX;
    /** 配置项 */
    private final LocalCacheConfig config;

    /**
     * @param config 可配置参数
     */
    public EntityCacheManager(LocalCacheConfig config){
        this.config = config;
        MAX_WEIGHT_BYTES = config.maxCacheBytes;
        INITIAL_CAPACITY = config.entitiesInitialCapacity;
        ONE_ROLL_CHECK_MAX = config.oneRollCheckMax;
        SUM_MEM_MS = config.sumAllTableMemMs;
    }

    /**
     * 为每张表单独初始化缓存空间
     */
    public void initTableCache(TableDescribe<Cacheable> tableDesc){
        keyCache.put(tableDesc, new ConcurrentHashMap<>(INITIAL_CAPACITY, LOAD_FACTOR));
        coreCache.put(tableDesc, new ConcurrentHashMap<>(INITIAL_CAPACITY, LOAD_FACTOR));
        conditionCache.put(tableDesc, new ConcurrentHashMap<>(INITIAL_CAPACITY, LOAD_FACTOR));
    }

    /**
     * 从核心缓存中查询数据
     * @param tableDesc
     * @param key
     * @param isStatistic 是否统计命中率; 一般做容错和检查缓存时不统计进命中率
     * @return 无缓存时返回null
     */
    public Cacheable getByPK(TableDescribe<Cacheable> tableDesc, CacheKey key, boolean isStatistic) {
        long queryTime = System.currentTimeMillis();
        Cacheable cacheEntity = coreCache.get(tableDesc).get(key);
        CacheKey realKey = keyCache.get(tableDesc).get(key);
        if(cacheEntity != null && tableDesc.tableStrategy.strictExpireMode(config)){   // 如果严格检测过期，则要在查询到过期数据时将其移除
            if(checkExpireAndRemove(tableDesc, realKey, queryTime)){
                cacheEntity = null;
            }
        }
        // 记录统计信息
        if(isStatistic){
            tableDesc.visit(queryTime);
            if (cacheEntity != null) {
                resetExpireTime(tableDesc, realKey, true); // 有访问缓存，续期缓存时间
                tableDesc.hit(queryTime); // 缓存命中, 统计命中率
            }
        }
        return cacheEntity;
    }

    /**
     * 从条件查询缓存中查询关联列表
     *
     * @param tableDesc
     * @param conditionKey
     * @param isStatistic 是否统计命中率; 一般做容错和检查缓存时不统计进命中率
     * @return 已缓存的主键列表
     */
    public List<Object> getByCondition(TableDescribe<Cacheable> tableDesc, CacheKey conditionKey, boolean isStatistic){
        long queryTime = System.currentTimeMillis();
        List<Object> pks = conditionCache.get(tableDesc).get(conditionKey);
        CacheKey realKey = keyCache.get(tableDesc).get(conditionKey);
        if(pks != null && tableDesc.tableStrategy.strictExpireMode(config)){   // 如果严格检测过期，则要在查询到过期数据时将其移除
            if(checkExpireAndRemove(tableDesc, realKey, queryTime)){
                pks = null;
            }
        }
        if(isStatistic && pks != null){
            resetExpireTime(tableDesc, realKey, true);    // 有访问缓存，续期缓存时间
        }
        return pks;
    }

    /** 缓存调优系数默认值 */
    private static final double STD_ADAPT_RATE = 0.95;
    /** 缓存调优系数，大于0小于1，根据负载情况动态适应, */
    private volatile static double adaptRate = STD_ADAPT_RATE;
    /** 自适应系数步长 */
    private static final double ADAPT_STEP = 0.001;
    /** 自适应系数动态范围 */
    private static final int ADAPT_MIN = 0, ADAPT_MAX = 1;

    /**
     * 计算初始过期时间
     *
     * 初始过期时间 = ((表权重)^动态调优系数) * 常量
     * 续期时间 = ((表权重)^动态调优系数) * 常量 * 表续期权重
     * 负载系数根据负载情况动态自适应，作用于所有表
     *
     * 表级权重系数用于针对各表的数据访问特点，动态自适应, 具体适应规则：
     *
     * 情况： 数据量远低于缓存区大小；长期处于缓存空间不足；
     * 数据量远低于缓存区大小时，提升权重，看命中率是否提升；未达预期，则回滚操作；内存充足情况:权重会稳定到一定程度
     * 表级权重系数-关联-命中率，
     *
     * @param tableDesc
     * @return  过期的毫秒数
     */
    public <T extends Cacheable> double calcStdExpireTime(TableDescribe<T> tableDesc){
        // double loadsRate = memRatio < 60 ? 1.5 : memRatio < 80 ? 1 : 0.5; // 根据缓存负载设置负载系数 0~60宽松失效时间；60~80标准失效时间；大于80严格失效时间 * loadsRate
        int x = tableDesc.expireRate;
        long stdSeconds = tableDesc.tableStrategy.expireSeconds(config);
        double y;
        if(x >= 0){
            y = Math.pow(x, adaptRate) + stdSeconds;
        }else{
            y = -Math.pow(-x, adaptRate) + stdSeconds;
        }
        return y < 0 ? 0 : y * 1000;
    }

    /**
     * 设置查询缓存过期时间
     * @param tableDesc
     * @param key
     * @param isRenewal 是否为续期
     */
    private void resetExpireTime(TableDescribe<Cacheable> tableDesc, CacheKey key, boolean isRenewal){
        final long currentTime = System.currentTimeMillis();
        long expireTime;
        double stdExpireTime = calcStdExpireTime(tableDesc);
        if(isRenewal){  // 是否为续期
            expireTime = (long)(stdExpireTime * tableDesc.tableStrategy.renewalRate(config));
            // 如果续期时间未超过初始的过期时间，则无需增加时间
            expireTime = currentTime + expireTime >= key.expireTime ? expireTime : key.expireTime - currentTime;
        }else{  // 初始过期时间
            expireTime = (long)(stdExpireTime);
        }
        if(expireTime > 0){
            key.expireTime = currentTime + expireTime;
        }
    }

    /**
     * 条件查询缓存保存数据
     *
     * @param tableDesc
     * @param key
     * @param pks
     */
    public void cacheCondition(TableDescribe<Cacheable> tableDesc, CacheKey key, List<Object> pks){
        CacheKey realKey = keyCache.get(tableDesc).get(key);
        if(realKey == null){
            keyCache.get(tableDesc).put(key, key);
            realKey = key;
        }
        resetExpireTime(tableDesc, realKey, false);
        conditionCache.get(tableDesc).put(realKey, pks);
        realKey.bytes = RamUsageEstimator.sizeOfObject(pks);
    }

    /**
     * 主缓存保存数据
     *
     * @param <T>
     * @param tableDesc
     * @param key
     * @param entity 实体类
     * @return 是否缓存成功，如果数据不需要缓存，返回false
     */
    public <T extends Cacheable> void cacheCore(TableDescribe<Cacheable> tableDesc, CacheKey key, T entity){
        CacheKey realKey = keyCache.get(tableDesc).get(key);
        if(realKey == null){
            keyCache.get(tableDesc).put(key, key);
            realKey = key;
        }
        resetExpireTime(tableDesc, realKey, false);
        coreCache.get(tableDesc).put(realKey, entity);
        realKey.bytes = tableDesc.calcMemCache(entity);
    }

    /** 卸载缓存 */
    public void unloadCache(TableDescribe<Cacheable> tableDesc, CacheKey key){
        key = keyCache.get(tableDesc).remove(key);
        if(key != null){
            if(key.isPK){
                coreCache.get(tableDesc).remove(key);
            }else{
                conditionCache.get(tableDesc).remove(key);
            }
        }
    }

    /** 插入实体类的同时，更新条件查询 */
    public <T extends Cacheable> void whenInsertEntity(TableDescribe<Cacheable> tableDesc, T entity){
        List<CacheKey> conditionKeys = tableDesc.tableStrategy.getConditionKeys(entity);
        if(conditionKeys != null && conditionKeys.size() > 0){
            Object pk = tableDesc.tableStrategy.getPrimaryKey(entity);
            Map<CacheKey, List<Object>> conditionMapping = conditionCache.get(tableDesc);
            Map<CacheKey, CacheKey> keyMapping = keyCache.get(tableDesc);
            for(CacheKey conditionKey : conditionKeys){
                conditionKey = keyMapping.get(conditionKey);
                if(conditionKey != null){
                    List<Object> newPks = new ArrayList<>(conditionMapping.get(conditionKey));
                    newPks.add(pk);
                    conditionMapping.put(conditionKey, newPks);
                }
            }
        }
    }

    /**
     * 删除实体类的同时，更新条件查询
     */
    public <T extends Cacheable> void whenDeleteEntity(TableDescribe<Cacheable> tableDesc, T entity){
        List<CacheKey> conditionKeys = tableDesc.tableStrategy.getConditionKeys(entity);
        if(conditionKeys != null && conditionKeys.size() > 0){
            Object pk = tableDesc.tableStrategy.getPrimaryKey(entity);
            Map<CacheKey, List<Object>> conditionMapping = conditionCache.get(tableDesc);
            Map<CacheKey, CacheKey> keyMapping = keyCache.get(tableDesc);
            for(CacheKey conditionKey : conditionKeys){
                conditionKey = keyMapping.get(conditionKey);
                if(conditionKey != null){
                    List<Object> newPks = new ArrayList<>(conditionMapping.get(conditionKey));
                    newPks.remove(pk);
                    conditionMapping.put(conditionKey, newPks);
                }
            }
        }
    }

    /**
     * 检测过期
     * 采用与redisExpire类似的随机过期机制，检测随机一部分数据
     *
     * @param tableDesc
     */
    public void checkExpire(TableDescribe<Cacheable> tableDesc){
        long startCheckTime = System.currentTimeMillis();

        // 随机检查部分缓存数据是否过期
        int totalCacheCount = 0, removeCacheCount = 0, checkKeyCount = ONE_ROLL_CHECK_MAX;
        Map<CacheKey, CacheKey> cacheKeyMapping = keyCache.get(tableDesc);
        if( checkKeyCount >= (totalCacheCount = cacheKeyMapping.size()) ) checkKeyCount = 1 + totalCacheCount/2;
        if(totalCacheCount > 0){
            Map<CacheKey, CacheKey> randomMap = MapRandomAccessUtil.getRandomEntrys(cacheKeyMapping, checkKeyCount);
            for(Map.Entry<CacheKey, CacheKey> entry : randomMap.entrySet()){
                CacheKey cacheKey = entry.getValue();
                // 检测主缓存是否过期，卸载过期数据
                if( cacheKey != null && checkExpireAndRemove(tableDesc, cacheKey, startCheckTime) ){
                    removeCacheCount ++;
                }
            }
        }
        // 自适应优化表级权重
        if(tableDesc.tableStrategy.useDynamicRate(config)){
            tableDesc.adaptRate(memRatio, ThreadLocalRandom.current(), startCheckTime);
        }
        // 打印统计日志
        StringBuilder logBuilder = new StringBuilder("本轮检查过期缓存完成, table：")
                .append(tableDesc.entityName)
                .append(", 表权重系数：").append(tableDesc.expireRate)
                .append(", 初始过期时间：").append((long)(calcStdExpireTime(tableDesc)/1000))
                .append("s, 耗时：").append(System.currentTimeMillis() - startCheckTime).append("ms")
                .append("\r\n        - 剩余缓存键值数量：")
                .append(totalCacheCount).append("-").append(removeCacheCount).append("=").append(totalCacheCount - removeCacheCount)
                .append("\r\n        - 每小时命中率统计：").append(tableDesc.toStringEveryHourStats())
                .append("\r\n        - 每10分钟命中率统计：").append(tableDesc.toStringEvery10MinStats());
        log.debug(logBuilder.toString());


        // if(!isTimeOut) isTimeOut = startCheckTime + timeoutMs <= System.currentTimeMillis();
        // 内存达到一定阈值，则除了过期数据，还会额外随机卸载部分快要过期的数据
        int outRate;
        if( ( outRate = memRatio - 95 ) > 0 && (100.0*removeCacheCount/totalCacheCount < outRate) ){
            startCheckTime = System.currentTimeMillis();
            removeCacheCount = 0;
            int forceRmc = 1 +  totalCacheCount/33;  // 强制从9%的缓存中最多移除3%濒临过期数据
            Map<CacheKey, CacheKey> randomMap = MapRandomAccessUtil.getRandomEntrys(cacheKeyMapping, forceRmc * 3);
            CacheKey[] entries = randomMap.values().toArray(new CacheKey[randomMap.size()]);
            Arrays.sort( entries, ((o1, o2) -> (int)(o1.expireTime - o2.expireTime)) );
            for(int i = 0; i < entries.length && i <= forceRmc; i++){
                checkExpireAndRemove(tableDesc, entries[i], Long.MAX_VALUE);
                removeCacheCount++;
            }
            StringBuilder logExtraBuilder = new StringBuilder("本轮检查过期缓存时,负载过高, table：")
                    .append(tableDesc.entityName)
                    .append(", 额外移除键值数量：").append(removeCacheCount).append(", 额外耗时：").append(System.currentTimeMillis() - startCheckTime).append("ms");
            log.warn(logExtraBuilder.toString());
        }

    }

    /**
     * 检查查询缓存是否过期，如果数据过期，移除数据
     *
     * @return 数据是否过期
     */
    private boolean checkExpireAndRemove(TableDescribe<Cacheable> tableDesc, CacheKey conditionKey, long currentTime){
        if(conditionKey.expireTime <= currentTime){
            unloadCache(tableDesc, conditionKey);
            return true;
        }
        return false;
    }


    /**
     * 汇总缓存使用内存情况
     */
    public <T extends Cacheable> void sumMem(TableDescribe<Cacheable> tableDesc){
        // 计算汇总单张表的内存占用
        long totalMem = 0L;
        for(CacheKey cacheKey : keyCache.get(tableDesc).values()){
            totalMem += cacheKey.bytes;
        }
        tableDesc.totalMemBytes = totalMem;
        // 汇总所有表的总内存占用情况
        long lastSumMs = lastSumMemTime.get();
        long currentMs = System.currentTimeMillis();
        boolean isTimeout = currentMs - lastSumMs >= SUM_MEM_MS;
        if(isTimeout && lastSumMemTime.compareAndSet(lastSumMs, currentMs)){
            statisticMemRatio();
        }
    }

    /**
     * 计算缓存使用内存情况
     */
    private void statisticMemRatio(){
        long startTime = System.currentTimeMillis();
        long totalSize = 0; // 总占用字节数
        for(TableDescribe<Cacheable> tableDesc : keyCache.keySet()){
            totalSize += tableDesc.totalMemBytes;
        }
        memRatio = (int)(120.0 * totalSize/MAX_WEIGHT_BYTES);   // 组件基础结构也有一定的内存占用，暂时设为额外20%用于基础组件占用的内存
        log.info( String.format("========缓存已使用内存%d%s：%.1fMB/%.1fMB,优化系数：%.3f", memRatio, "%", totalSize/1024.0/1024.0, MAX_WEIGHT_BYTES/1024.0/1024.0, adaptRate ) + "耗时：" + (System.currentTimeMillis() - startTime) + "ms========");
        // 自适应优化过期算法曲线 TODO 暂时屏蔽
//        if(memRatio >= 95){
//            adaptRate -= ADAPT_STEP;
//        }else if(memRatio <= 50){
//            adaptRate += ADAPT_STEP;
//        }
//        if(adaptRate >= ADAPT_MAX) adaptRate = ADAPT_MAX - ADAPT_STEP;
//        if(adaptRate <= ADAPT_MIN) adaptRate = ADAPT_MIN + ADAPT_STEP;

    }

}
