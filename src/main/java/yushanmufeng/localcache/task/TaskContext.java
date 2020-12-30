package yushanmufeng.localcache.task;

/**
 * 异步任务的上下文对象
 * 用于保存和传递相关参数
 */
public class TaskContext {

    /** 任务的提交时间，单位毫秒 */
    public long commitTime = System.currentTimeMillis();

    /** 无用的默认taskContext */
    public static TaskContext DEFAULT_CONTEXT = new TaskContext();

}
