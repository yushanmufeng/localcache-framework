package yushanmufeng.localcache.config;

/**
 * 全局缓存配置项
 */
public class LocalCacheConfig extends SingleTableCacheConfig{

    /** ===================== 基础配置 start ===================== */
    /** 总可缓存的最大字节数, 默认128M */
    public long maxCacheBytes;
    /** 执行查询任务线程数 */
    public int selectThreadCount;
    /** 执行增删改任务线程数 */
    public int nonSelectThreadCount;
    /** 执行查询任务线程名字前缀 */
    public String selectThreadPreName;
    /** 执行增删改任务线程名字前缀 */
    public String nonSelectThreadPreName;
    /** 单表的缓存实体空间map初始长度，设置为稍大的值可减少map扩容次数 */
    public int entitiesInitialCapacity;
    /** ===================== 基础配置 end ===================== */

    /** ===================== 过期相关配置 start ===================== */
    /** 多久计算汇总一次单表的内存占用 */
    public long sumOneTableMemMs;
    /** 多久计算汇总一次所有表的内存占用 */
    public long sumAllTableMemMs;
    /** 多久检查一次缓存过期(检查缓存过期同时会计算自适应系数) */
    public long checkExpireMs;
    /** 单轮检测过期最大条目数，配置的越大则会越及时的移除过期数据，但单轮检测所需的时间会更长 */
    public int oneRollCheckMax;
    /** ===================== 过期相关配置 end ===================== */

    /** ===================== 自适应过期时间相关配置 start ===================== */
    /**
     * 状态转换规则：
     * 假设负载75%为档位1，负载90%为档位2（档位可配置）：
     * 负载低于75，随机循环尝试提升权重和降低权重; 负载75-90，大概率尝试降低权重,小概率尝试提升权重; 负载大于90，切换为稳定态，不做调整和预期
     * 稳定状态->提升权重状态：持续一段时间处于负载小于75%; 则状态波动
     * 稳定状态->降低权重状态：持续一段时间处于负载75-90%; 则状态波动, 大概率变为降低权重状态
     * 提升权重状态->稳定状态：负载大于90% or 尝试提升权重发现命中率无提升: 则回滚权重，变为稳定状态
     * 降低权重状态->稳定状态：负载大于90% or 尝试降低权重发现命中率下降: 回滚权重，变为稳定状态
     * 做检测过期操作时会进行状态计算
     */
    /** 自适应调整步长每轮增长值, 越大则时间调整幅度越大;1点步长略小于1s;默认快增慢减 */
    public int upAdaptStep;
    /** 自适应调整步长每轮降低值, 越大则时间调整幅度越大;1点步长略小于1s;默认快增慢减 */
    public int downAdaptStep;
    /** 保持权重系数稳定的时间， 相当于每一轮权重调整的间隔时间 */
    public long stableTimeMs;
    /** 每一轮测试权重调整的时间; 设置的时间越长则调整越准确，但调整速度更慢 */
    public long testTimeMs;
    /** 测试警戒档位1, 缓存空间负载百分比 */
    public int testPercentL1;
    /** 测试警戒档位2, 缓存空间负载百分比 */
    public int testPercentL2;
    /** ===================== 自适应过期时间相关配置 end ===================== */

    public LocalCacheConfig(){
        super();
        this.maxCacheM(128)
                .selectThreadCount(3)
                .nonSelectThreadCount(3)
                .selectThreadPreName("LocalCache-Select-Tasks-Thread")
                .nonSelectThreadPreName("LocalCache-NonSelect-Tasks-Thread")
                .entitiesInitialCapacity(1024)
                .sumOneTableMemMinutes(1)
                .sumAllTableMemMinutes(5)
                .checkExpireMinutes(5)
                .oneRollCheckMax(100)
                .upAdaptStep(300)
                .downAdaptStep(60)
                .stableTimeMinutes(10)
                .testTimeMinutes(60)
                .testPercentL1(75)
                .testPercentL2(90)
        ;
    }

    // =================== 可以单表单独配置的参数 start ===================

    /** 是否使用严格过期模式。因为数据过期后不会及时从内存中卸载，严格过期模式下这些数据会在过期后不可用，并在下次访问时从缓存中移除；非严格模式下，过期数据如果还未被卸载，再次被访问时依然可用 */
    public LocalCacheConfig strictExpireMode(boolean strictExpireMode){
        this.strictExpireMode = strictExpireMode;
        return this;
    }

    /** 是否开启自适应过期权重系数优化 */
    public LocalCacheConfig useDynamicRate(boolean useDynamicRate){
        this.useDynamicRate = useDynamicRate;
        return this;
    }

    /** 设置过期时间、数据失效时间。如果表重写了过期时间方法，则会覆盖此配置 */
    public LocalCacheConfig expireMinutes(long minutes){
        expireSeconds = minutes * 60;
        return this;
    }

    /** 设置过期时间、数据失效时间。如果表重写了过期时间方法，则会覆盖此配置 */
    public LocalCacheConfig expireSeconds(long seconds){
        expireSeconds = seconds;
        return this;
    }

    /** 缓存续期系数 ，访问数据时会做续期, 续期时间=标准时间x续期系数； 如果为0则不续期缓存时间 */
    public LocalCacheConfig renewalRate(double rate){
        renewalRate = rate;
        return this;
    }
    // =================== 可以单表单独配置的参数 end ===================

    /** 执行查询任务线程数 */
    public LocalCacheConfig selectThreadCount(int threadCount){
        selectThreadCount = threadCount;
        return this;
    }

    /** 执行增删改任务线程数 */
    public LocalCacheConfig nonSelectThreadCount(int threadCount){
        nonSelectThreadCount = threadCount;
        return this;
    }

    /** 执行查询任务线程名字前缀 */
    public LocalCacheConfig selectThreadPreName(String threadPreName){
        selectThreadPreName = threadPreName;
        return this;
    }

    /** 执行增删改任务线程名字前缀 */
    public LocalCacheConfig nonSelectThreadPreName(String threadPreName){
        nonSelectThreadPreName = threadPreName;
        return this;
    }

    /** 设置可用缓存空间兆数 */
    public LocalCacheConfig maxCacheM(int M){
        maxCacheBytes = M * 1024L * 1024;
        return this;
    }

    /** 单表的缓存实体空间map初始长度，设置为稍大的值可减少map扩容次数 */
    public LocalCacheConfig entitiesInitialCapacity(int initialCapacity){
        entitiesInitialCapacity = initialCapacity;
        return this;
    }

    /** 单轮检测过期最大条目数，配置的越大则会越及时的移除过期数据，但单轮检测所需的时间会更长 */
    public LocalCacheConfig oneRollCheckMax(int checkCount){
        oneRollCheckMax = checkCount;
        return this;
    }

    /** 多久计算汇总一次单表的内存占用 */
    public LocalCacheConfig sumOneTableMemMinutes(long minutes){
        sumOneTableMemMs = minutes * 60 * 1000;
        return this;
    }

    /** 多久计算汇总一次所有表的内存占用 */
    public LocalCacheConfig sumAllTableMemMinutes(long minutes){
        sumAllTableMemMs = minutes * 60 * 1000;
        return this;
    }

    /** 多久检查一次缓存过期 */
    public LocalCacheConfig checkExpireMinutes(long minutes){
        checkExpireMs = minutes * 60 * 1000;
        return this;
    }

    /** 自适应调整步长增长值, 越大则时间调整幅度越大;1点步长略小于1s;默认快增慢减 */
    public LocalCacheConfig upAdaptStep(int step){
        upAdaptStep = step;
        return this;
    }

    /** 自适应调整步长降低值, 越大则时间调整幅度越大;1点步长略小于1s;默认快增慢减 */
    public LocalCacheConfig downAdaptStep(int step){
        downAdaptStep = step;
        return this;
    }

    /** 保持权重系数稳定的时间， 相当于每一轮权重调整的间隔时间 */
    public LocalCacheConfig stableTimeMinutes(long minutes){
        stableTimeMs = minutes * 60 * 1000;
        return this;
    }

    /** 每一轮测试权重调整时间; 设置的时间越长则调整越准确，但调整速度更慢 */
    public LocalCacheConfig testTimeMinutes(long minutes){
        testTimeMs = minutes * 60 * 1000;
        return this;
    }

    /** 测试警戒档位1, 缓存空间负载百分 */
    public LocalCacheConfig testPercentL1(int percent){
        testPercentL1 = percent;
        return this;
    }

    /** 测试警戒档位2, 缓存空间负载百分 */
    public LocalCacheConfig testPercentL2(int percent){
        testPercentL2 = percent;
        return this;
    }

}
