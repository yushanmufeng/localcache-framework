package yushanmufeng.localcache.util;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 简单的任务执行器，将启用一个线程来持续执行任务
 */
public class SimpleTaskExecutor<T extends Runnable> {

    private final Thread thread;
    private LinkedBlockingQueue<T> taskQueue;
    private boolean startup;
    public final String threadName;
    private CountDownLatch countDownLatch;  // 用于结束执行器时计数

    public SimpleTaskExecutor(String name, boolean startup){
        this(name, startup, new CountDownLatch(1));
    }
    /**
     *
     * @param name 启动的线程名字
     * @param startup 是否同时启动
     * @param countDownLatch 结束计数器
     */
    public SimpleTaskExecutor(String name, boolean startup, CountDownLatch countDownLatch){
        this.thread = new Thread(new ConsumeTask(), name);
        this.taskQueue = new LinkedBlockingQueue<>();
        this.threadName = name;
        this.countDownLatch = countDownLatch;
        this.startup = startup;
        if(startup){
            this.thread.start();
        }
    }

    /**
     * 启动后台线程
     */
    public void start(){
        if(!startup){
            startup = true;
            this.thread.start();
        }
    }

    /**
     * 结束后台线程,会等待执行线程结束(队列任务数为空时，线程结束)
     * @param endTask 结束时会向队列中加入一个空任务，防止任务队列一直等待
     */
    public void stop(T endTask){
        startup = false;
        try{
            // 加入一个空任务，防止任务队列一直等待
            taskQueue.put(endTask);
        }catch (Exception e){
            System.out.println(thread.getName() + "结束线程发生异常: " + e.toString());
        }

    }

    public void put(T runnable){
        try {
            taskQueue.put(runnable);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public boolean isEmpty(){
        return taskQueue.isEmpty();
    }

    /** 返回当前队列大小 */
    public int size(){
        return taskQueue.size();
    }

    /** 返回队列的迭代器 */
    public Iterator<T> iterator(){
        return taskQueue.iterator();
    }

    class ConsumeTask implements Runnable{
        @Override
        public void run() {
            while (startup) {
                try {
                    Runnable task = taskQueue.take();
                    task.run();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            // 线程已经结束，等待队列中的所有任务执行完毕，结束线程
            while(true){
                Runnable task = taskQueue.poll();
                if(task != null){
                    try {
                        task.run();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }else{
                    break;
                }
            }
            countDownLatch.countDown();
        }
    }

    /**
     * 返回负载最低的执行器
     * @param executors 执行器列表
     * @return
     */
    public static <V extends Runnable> SimpleTaskExecutor<V> getLoadLowestExecutor(SimpleTaskExecutor<V>[] executors){
        return getLoadLowestExecutor(executors, -1);
    }

    /**
     * 返回负载最低的执行器
     * 如果所有执行器均负载较高，则每张表会使用固定下标的执行器，用来便于优化sql批量执行
     *
     * @param executors 执行器列表
     * @param bandIndex 负载较高情况时固定使用的执行器下标
     */
    public static <V extends Runnable> SimpleTaskExecutor<V> getLoadLowestExecutor(SimpleTaskExecutor<V>[] executors, int bandIndex){
        // 没有空闲的则返回一个负载最低的
        int minTasks = Integer.MAX_VALUE;
        SimpleTaskExecutor<V> lowestExecutor = null;
        for(SimpleTaskExecutor<V> executor : executors){
            if(executor.size() < minTasks){
                minTasks = executor.size();
                lowestExecutor = executor;
            }
            if(executor.isEmpty()){
                return executor;
            }
        }
        if(minTasks > 2 && bandIndex != -1){
            return executors[bandIndex];
        }else{
            return lowestExecutor;
        }

    }

}
