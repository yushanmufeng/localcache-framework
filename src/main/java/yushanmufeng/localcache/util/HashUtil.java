package yushanmufeng.localcache.util;

public class HashUtil {

    /**
     * 通过hash算法散列
     *
     * @param res 要计算hash的对象
     * @param range hash范围, 0~range, 不包含range
     */
    public static final int hash(Object res, int range){
        return (range - 1) & hash(res);
    }

    /** 算法同jdk HashMap */
    public static final int hash(Object res) {
        int h;
        return (res == null) ? 0 : (h = res.hashCode()) ^ (h >>> 16);
    }


}
