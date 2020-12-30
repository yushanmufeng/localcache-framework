package yushanmufeng.localcache.atomic;

/** 实体数据状态常量 */
public interface EntityState{
    /** 待初始化状态 */
    int GET_READY = -1;
    /** 最新，表示存在，可能是更新中或是插入中 */
    int LATEST = 2;
    /** 已删除，表示不存在，确定数据已经被删除了 */
    int DELETED = 3;
    /** 未知，无法确定数据的存在状态 */
    int DETACHED = 4;

}
