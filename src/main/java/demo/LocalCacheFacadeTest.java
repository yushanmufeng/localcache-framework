package demo;

import yushanmufeng.localcache.CacheKey;
import yushanmufeng.localcache.Cacheable;
import yushanmufeng.localcache.LocalCacheFacade;
import yushanmufeng.localcache.datasource.TableDataSource;
import yushanmufeng.localcache.config.LocalCacheConfig;
import yushanmufeng.localcache.task.TaskContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LocalCacheFacadeTest {



    public static void main(String[] args) throws Exception{
        // 设置string实体类型的数据源。测试仅使用内存缓存，不持久化
        TableDataSource<StringEntity> stringDataSource = new TableDataSource<StringEntity>() {
            @Override
            public Class<StringEntity> getEntityClass() {
                return StringEntity.class;
            }
            @Override
            public Object getPrimaryKey(StringEntity entity) {
                return entity.key;
            }
            // 因为只使用内存缓存数据, 所以不会从数据源查到数据。 如果需要持久化，则在此处实现通过第三方数据源增删改查数据，如Mysql，Redis等
            @Override
            public List<StringEntity> select(CacheKey key) {
                System.out.println("Handle select : " + key);
                return null;
            }
            @Override
            public void insert(List<TaskContext> contexts, List<StringEntity> entities) {
                System.out.println("Handle insert : " + entities);
            }
            @Override
            public void update(List<TaskContext> contexts, List<StringEntity> entities) {
                System.out.println("Handle update : " + entities);
            }
            @Override
            public void delete(List<TaskContext> contexts, List<StringEntity> entities) {
                System.out.println("Handle delete : " + entities);
            }
        };
        List<TableDataSource<?>> tableSources = new ArrayList<>();
        tableSources.add(stringDataSource);
        // 自定义配置参数
        LocalCacheConfig config = new LocalCacheConfig()
                .expireSeconds(5)   // 数据有效期为5秒
                .strictExpireMode(true) // 访问缓存中已过期但未卸载的数据时，移除缓存
                .renewalRate(0) // 访问数据不续期
                ;
        // 创建缓存组件
        LocalCacheFacade localCacheFacade = new LocalCacheFacade(tableSources, config);
        // 使用缓存组件存取数据
        StringEntity testString1 = new StringEntity("key1", "Hello World!");
        localCacheFacade.insert(testString1);
        StringEntity testString2 = new StringEntity("key2", "Hello LocalCache!");
        localCacheFacade.insert(testString2);
        System.out.println("Get key1 cache : " + localCacheFacade.getByPK(StringEntity.class, "key1"));
        System.out.println("Get key2 cache : " + localCacheFacade.getByPK(StringEntity.class, "key2"));
        Thread.sleep(5001);
        System.out.println("Get key1 cache : " + localCacheFacade.getByPK(StringEntity.class, "key1"));
        System.out.println("Get key2 cache : " + localCacheFacade.getByPK(StringEntity.class, "key2"));
        // 关闭缓存组件
        localCacheFacade.shutdown();
    }

}

class StringEntity extends Cacheable {
    public String key, value;
    StringEntity(String key, String value){
        this.key = key;
        this.value = value;
    }
    @Override
    public String toString() {
        return "StringEntity{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}


class TestDataSource implements TableDataSource<StringEntity>{
    @Override
    public Class<StringEntity> getEntityClass() {
        return StringEntity.class;
    }
    @Override
    public Object getPrimaryKey(StringEntity entity) {
        return entity.key;
    }
    // 因为只使用内存缓存数据, 所以不会从数据源查到数据。 如果需要持久化，则在此处实现通过第三方数据源增删改查数据，如Mysql，Redis等
    @Override
    public List<StringEntity> select(CacheKey key) {
        System.out.println("Handle select : " + key);
        return null;
    }
    @Override
    public void insert(List<TaskContext> contexts, List<StringEntity> entities) {
        System.out.println("Handle insert : " + entities);
    }
    @Override
    public void update(List<TaskContext> contexts, List<StringEntity> entities) {
        System.out.println("Handle update : " + entities);
    }
    @Override
    public void delete(List<TaskContext> contexts, List<StringEntity> entities) {
        System.out.println("Handle delete : " + entities);
    }
}