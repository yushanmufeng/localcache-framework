package yushanmufeng.localcache;

import yushanmufeng.localcache.config.LocalCacheConfig;
import yushanmufeng.localcache.task.TaskContext;

import java.util.List;

/**
 * 配置可缓存的表数据源
 *
 * @param <T>
 */
public interface TableDataSource<T extends Cacheable> {

    /** 实体class */
    Class<T> getEntityClass();

    /** 从实体类中获取主键 */
    Object getPrimaryKey(T entity);

    /** 从实体类中，获取所有的条件查询集合的键值; 如果没有，可以返回空 */
    default List<CacheKey> getConditionKeys(T entity){
        return null;
    }

    /** 默认实现根据主键查询方法; 主键对应的实体类只能有一个 */
    default T selectByPK(Object primaryKey){
        List<T> results = select( new CacheKey(true, primaryKey) );
        if(results != null && results.size() > 0){
            return results.get(0);
        }
        return null;
    }

    /** 根据缓存键值查询数据，需要对所有支持的情况都重写 */
    List<T> select(CacheKey key);

    /**
     * 插入数据
     * @param contexts 操作相关的上下文对象
     * @param entities 要插入的实体对象, 不能为空
     */
    void insert(List<TaskContext> contexts, List<T> entities);

    /**
     * 更新数据
     * @param contexts 操作相关的上下文对象
     * @param entities 实体对象
     */
    void update(List<TaskContext> contexts, List<T> entities);

    /**
     * 删除数据
     * @param contexts 操作相关的上下文对象
     * @param entities 实体对象
     */
    void delete(List<TaskContext> contexts, List<T> entities);

    /**
     * 过期时间，优先级大于全局配置，覆写此方法可以覆盖全局过期时间
     * @return 默认过期时间, 单位秒
     */
    default long expireSeconds(LocalCacheConfig config){
        return config.expireSeconds;
    }

    /**
     * 缓存续期系数 ，访问数据时会做续期, 续期时间=标准时间x续期系数； 如果为0则不续期缓存时间
     * 优先级大于全局配置，覆写此方法可以覆盖全局过期时间
     * @return 默认过期时间, 单位秒
     */
    default double renewalRate(LocalCacheConfig config){
        return config.renewalRate;
    }


    /**
     * 是否自适应过期权重系数,优先级大于全局配置，覆写此方法可以覆盖全局配置
     * @return 是否开启自适应过期权重系数优化
     */
    default boolean useDynamicRate(LocalCacheConfig config){
        return config.useDynamicRate;
    }

    /**
     * 是否使用严格过期模式,优先级大于全局配置，覆写此方法可以覆盖全局配置
     * @return 是否使用使用严格过期模式
     */
    default boolean strictExpireMode(LocalCacheConfig config){
        return config.strictExpireMode;
    }

}
