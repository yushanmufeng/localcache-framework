package yushanmufeng.localcache.config;

/**
 * 单表缓存配置项
 */
public class SingleTableCacheConfig {

    /** 数据失效时长 */
    public long expireSeconds;
    /** 缓存续期系数 ，访问数据时会做续期, 续期时间=标准时间x续期系数； 如果为0则不续期缓存时间 */
    public double renewalRate;
    /** 是否开启自适应过期权重系数优化 */
    public boolean useDynamicRate;
    /** 是否使用严格过期模式。因为数据过期后不会及时从内存中卸载，严格过期模式下这些数据会在过期后不可用，并在下次访问时从缓存中移除；非严格模式下，过期数据如果还未被卸载，再次被访问时依然可用 */
    public boolean strictExpireMode;


    public SingleTableCacheConfig(){
        this.expireMinutes(60)
                .strictExpireMode(false)
                .useDynamicRate(false)
                .renewalRate(0.75)
        ;
    }

    /** 是否使用严格过期模式。因为数据过期后不会及时从内存中卸载，严格过期模式下这些数据会在过期后不可用，并在下次访问时从缓存中移除；非严格模式下，过期数据如果还未被卸载，再次被访问时依然可用 */
    public SingleTableCacheConfig strictExpireMode(boolean strictExpireMode){
        this.strictExpireMode = strictExpireMode;
        return this;
    }

    /** 是否开启自适应过期权重系数优化 */
    public SingleTableCacheConfig useDynamicRate(boolean useDynamicRate){
        this.useDynamicRate = useDynamicRate;
        return this;
    }

    /** 设置过期时间、数据失效时间。如果表重写了过期时间方法，则会覆盖此配置 */
    public SingleTableCacheConfig expireMinutes(long minutes){
        expireSeconds = minutes * 60;
        return this;
    }

    /** 设置过期时间、数据失效时间。如果表重写了过期时间方法，则会覆盖此配置 */
    public SingleTableCacheConfig expireSeconds(long seconds){
        expireSeconds = seconds;
        return this;
    }

    /** 缓存续期系数 ，访问数据时会做续期, 续期时间=标准时间x续期系数； 如果为0则不续期缓存时间 */
    public SingleTableCacheConfig renewalRate(double rate){
        renewalRate = rate;
        return this;
    }

}
