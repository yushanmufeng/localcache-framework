package yushanmufeng.localcache.task;

import java.util.concurrent.FutureTask;

/**
 * 针对异步DB查询任务的带返回值任务
 * 增删改操作的异步任务允许对操作进行合并，通过batch操作的方式减少访问db次数，提升高并发下的效率
 *
 * @param <V>
 */
public class MergingFutureTask<V> extends FutureTask<V> {

    /** 可以合并的异步函数 */
    private MergingCallable<V> mergingCallable;

    public MergingFutureTask(MergingCallable<V> mergingCallable) {
        super(mergingCallable);
        this.mergingCallable = mergingCallable;
    }

    public MergingCallable<V> getMergingCallable(){
        return mergingCallable;
    }

}
