package yushanmufeng.localcache;

/**
 * 缓存的键值
 * 比如遇到多个同位数键的情况，可自行划分子键作为标识位，用于区分
 */
public class CacheKey {

    public final Object[] keys;
    // 拼接所有key，用于计算hash
    public final String unionStr;
    // 是否为主键，如果为主键，keys的长度必须为1
    public final boolean isPK;
    // 占用的空间，单位字节数
    public long bytes = 0L;
    // 过期时间
    public long expireTime;

    public CacheKey(boolean isPK, Object... keys){
        if(keys == null || keys.length == 0){
            throw new RuntimeException("keys can't be empty!");
        }
        this.isPK = isPK;
        this.keys = keys;
        StringBuilder sb = new StringBuilder(isPK?"pk":"ck");   // primarykey; conditionKey
        for(Object s : keys){
            sb.append("-").append(s.toString());
        }
        unionStr = sb.toString();
    }

    @Override
    public int hashCode() {
        return unionStr.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj != null && obj instanceof CacheKey){
            CacheKey target = (CacheKey)obj;
            Object[] tarKeys = target.keys;
            if(target.isPK == isPK && tarKeys.length == keys.length){
                for(int i = 0; i < keys.length; i++ ){
                    if(!tarKeys[i].equals(keys[i])){
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "CacheKey{" +
                "unionStr='" + unionStr + '\'' +
                ", expireTime=" + expireTime +
                '}';
    }
}
