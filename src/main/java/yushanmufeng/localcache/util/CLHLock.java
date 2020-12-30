package yushanmufeng.localcache.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * 公平自旋锁
 * CLH锁是一种基于链表的可扩展、高性能、公平的自旋锁，申请线程只在本地变量上自旋，它不断轮询前驱的状态，如果发现前驱释放了锁就结束自旋，获得锁
 * CLH的发明人是：Craig，Landin and Hagersten。
 * 代码参考：http://ifeve.com/java_lock_see2/
 * !!!!注意是不可重入锁
 */
public class CLHLock {

    public static final Logger log = LoggerFactory.getLogger(CLHLock.class);

    /** 定义节点 默认状态为true */
    public static class CLHNode {
        private volatile boolean isLocked = true;
    }
    /** 当前线程对应的节点 */
    private ThreadLocal<CLHNode> contextNode = new ThreadLocal<>();
    /** 尾部节点,值用一个节点 */
    private final AtomicReference<CLHNode> TAIL = new AtomicReference<>();

    /** 自旋超时时间，发生死锁时打印日志 */
    private static final long TIMEOUT_COUNT = 5_000_000_000L, ERROR_MS = 500;

    /**
     * 加锁,获取锁
     * @return 是否有前驱节点，即是锁是否被其他线程占用，当前线程经过自旋后才获得锁。可以用来评估是否繁忙
     */
    public boolean lock() {
        boolean hasPreNode = false;
        // 上下文对象，相当于钥匙。一个锁可以有多把钥匙，加锁解锁都要用同一把钥匙
        CLHNode context = new CLHNode();
        // 新建节点并将节点与当前线程保存起来
        contextNode.set(context);
        // 将新建的节点设置为尾部节点，并返回旧的节点（原子操作），这里旧的节点实际上就是当前节点的前驱节点
        CLHNode preNode = TAIL.getAndSet(context);
        if (preNode != null) {
            hasPreNode = true;
            // 前驱节点不为null表示当锁被其他线程占用，通过不断轮询判断前驱节点的锁标志位等待前驱节点释放锁
            long startMs = System.currentTimeMillis();
            long l = 0;
            while (preNode.isLocked) {
                if(++l >= TIMEOUT_COUNT){
                    l = 0;
                    if(System.currentTimeMillis() - startMs >= ERROR_MS){
                        log.error("自旋时间过长！请检查是否发生死锁或优化程序,ms:" + (System.currentTimeMillis() - startMs), new Exception());
                        startMs = System.currentTimeMillis();
                    }
                }
            }
            preNode = null;
        }
        // 如果不存在前驱节点，表示该锁没有被其他线程占用，则当前线程获得锁
        return hasPreNode;
    }

    /**
     * 解锁,释放锁
     *
     * @return 是否有后继节点。可以用来评估是否繁忙
     */
    public boolean unlock() {
        boolean hasTailNode = false;
        // 获取当前线程对应的节点
        CLHNode context = contextNode.get();
        // 如果tail节点等于node，则将tail节点更新为null，同时将node的lock状态职位false，表示当前线程释放了锁
        if (!TAIL.compareAndSet(context, null)) {
            hasTailNode = true;
            context.isLocked = false;
        }
        context = null;
        contextNode.remove();
        return hasTailNode;
    }

}

