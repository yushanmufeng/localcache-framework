localcache-framework
============

----
一个简单高效的本地进程内缓存**框架**，规范了访问缓存数据的方式，需要使用者具体实现数据源的访问逻辑，如Mysql，Redis，MongoDB等，降低缓存方案与业务代码间的耦合。

---
## 特点
* 1：简单高效：使用进程内存缓存数据，可以当其为对ConcurrentHashMap的封装，响应速度极快。通过对数据源的代理可实现类似”常驻内存“的效果。
* 2：异步操作数据源：增加、更新、删除操作调用方法时会以缓存中状态为准，直接响应返回，同时为其创建异步任务在后台任务队列执行。
* 3：批量操作合并：同数据源的相同操作并发，会自动合并成批量操作。多线程高并发下可以大幅度提升操作真实数据源的性能。
* 4：多种过期规则：多种可配置的过期选项，设置可用的缓存空间大小。

```java
// 测试实体类
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
// 自定义数据源
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

public static void main(String[] args) throws Exception{
    // 设置数据源。测试仅使用内存缓存，不持久化
    List<TableDataSource<?>> tableSources = new ArrayList<>();
    tableSources.add(new TestDataSource());
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
    System.out.println("Get key1 cache : " + localCacheFacade.getByPK(StringEntity.class, "key1"));
    Thread.sleep(5001);
    System.out.println("Get key1 cache : " + localCacheFacade.getByPK(StringEntity.class, "key1"));
    // 关闭缓存组件
    localCacheFacade.shutdown();
}

```

# 性能测试 
因为是使用的个人电脑测试,所以结果仅供参考 <br>
电脑配置16G内存，cpu-i3-8100, mariadb-10.4.17, redis-3.0.504 <br>
测试样本：单表10w行数据，5个bigint字段; 运行多次取中间值<br>
### 无缓存预热，<br>
&emsp;&emsp;3并发线程，各1w次主键查询、更新操作, 总耗时：14007ms <br>
&emsp;&emsp;3并发线程，各10w次主键查询、更新操作, 总耗时：32049ms <br>
&emsp;&emsp;4并发线程，各1w次主键查询、更新操作, 总耗时：14476ms <br>
&emsp;&emsp;4并发线程，各10w次主键查询、更新操作, 总耗时：31690ms <br>
### 有缓存预热，<br>
&emsp;&emsp;预热：1000次条件查询, 10w结果，耗时：1103ms <br>
&emsp;&emsp;单线程，1w次查询、更新操作, 总耗时：54ms <br>
&emsp;&emsp;2线程并发，各1w次主键查询、更新操作, 总耗时：83ms <br>
&emsp;&emsp;3线程并发，各1w次主键查询、更新操作, 总耗时：148ms <br>
&emsp;&emsp;4线程并发，各1w次主键查询、更新操作, 总耗时：180ms <br>
&emsp;&emsp;4线程并发，各10w次主键查询、更新操作, 总耗时：4743ms <br>
    
### 对比:Redis String类型，10W K-V随机读写，无序列化操作： <br>
&emsp;&emsp;单线程，1w次查询查询、更新, 总耗时：1654ms <br>
&emsp;&emsp;2线程并发，各1w次查询、更新, 总耗时：1775ms <br>
&emsp;&emsp;3线程并发，各1w次查询、更新, 总耗时：2033ms <br>
&emsp;&emsp;3线程并发，各10w次查询、更新, 总耗时：15203ms <br>
&emsp;&emsp;4线程并发，各1w次查询、更新, 总耗时：2004ms <br>
&emsp;&emsp;4线程并发，各10w次查询、更新, 总耗时：15999ms <br>

