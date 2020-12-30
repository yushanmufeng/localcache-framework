package yushanmufeng.localcache.task;

import yushanmufeng.localcache.CacheKey;
import yushanmufeng.localcache.Cacheable;
import yushanmufeng.localcache.TableDescribe;
import yushanmufeng.localcache.atomic.IAtomicLogic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yushanmufeng.localcache.util.SimpleTaskExecutor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * 针对异步DB查询任务在函数体
 * 增删改操作的异步任务允许对操作进行合并，通过batch操作的方式减少访问db次数，提升高并发下的效率
 * 将函数拆分成两部分，第一部分为对数据库的异步操作，第二部分为针对返回值的自定义处理。第一部分允许合并, 被合并过的任务可以直接执行第二部分
 * 限制：可以合并的db任务为同表的连续相同操作，比如连续的的对A表的更新操作、连续的对B表的插入操作等
 * @param <V>
 */
public abstract class MergingCallable<V> implements Callable<V> {

    public static Logger log = LoggerFactory.getLogger(MergingCallable.class);

    /** 异步任务类型，增删改查等 */
    protected int taskType;
    /** 此异步任务所使用的执行器 */
    private SimpleTaskExecutor<MergingFutureTask<?>> executor;

    protected TableDescribe<Cacheable> tableDesc;
    protected TaskContext context;
    protected CacheKey key;
    protected Cacheable entity;

    /** s1是否已经被合并处理, 如果已经被前面的任务合并，则等队列排到此任务实际执行时可以跳过s1 */
    private boolean isS1Finish;

    /**
     *
     * @param taskType
     */
    public MergingCallable(int taskType, SimpleTaskExecutor<MergingFutureTask<?>> executor, TableDescribe<Cacheable> tableDesc, TaskContext context, CacheKey key, Cacheable entity){
        this.taskType = taskType;
        this.executor = executor;
        this.tableDesc = tableDesc;
        this.context = context;
        this.key = key;
        this.entity = entity;
        // 只对同表的增加、更新、删除操作进行合并
        if(taskType == IAtomicLogic.INSERT_BY_PK || taskType == IAtomicLogic.UPDATE_BY_PK || taskType == IAtomicLogic.DELETE_BY_PK){
            isS1Finish = false;
        }else{
            isS1Finish = true;
        }
    }



    @Override
    public V call() {
        if(!isS1Finish){    // s1已被前面的批量执行合并，不需要重复执行
            subCall1();
        }
        return subCall2();
    }

    /**
     * 第一步, 所有类型的异步任务首先执行的方法, 所有允许合并的操作放在这里
     * 此方法默认实现对db操作的合并方法
     */
    public void subCall1(){
        // 合并连续的同表同操作的查询
        List<TaskContext> contexts = new ArrayList<>(Arrays.asList(context));
        List<Cacheable> entities = new ArrayList<>(Arrays.asList(entity));
        Iterator<MergingFutureTask<?>> iterator = executor.iterator();
        int count = 0;  // 限制批量SQL最大条目数
        while (iterator.hasNext()){
            count ++;
            MergingFutureTask<?> task = iterator.next();
            MergingCallable<?> otherCallable = task.getMergingCallable();
            if(count < 2000 && otherCallable.taskType == taskType && otherCallable.tableDesc == tableDesc){
                contexts.add(otherCallable.context);
                entities.add(otherCallable.entity);
                otherCallable.isS1Finish = true;
            }else{
                break;
            }
        }
        if(contexts.size() > 1){
            String taskTypeName = taskType == IAtomicLogic.INSERT_BY_PK ? "插入" : taskType == IAtomicLogic.UPDATE_BY_PK ? "更新" : "删除";
            log.debug("合并异步[" + taskTypeName + "]操作,table:" + tableDesc.entityName + ",count:" + contexts.size());
        }
        // 增加
        if(taskType == IAtomicLogic.INSERT_BY_PK){
            try {
                tableDesc.tableStrategy.insert(contexts, entities); // 增加插入方法
            }catch(Exception e){
                log.error("异步插入实体对象发生异常！table:" + tableDesc.entityName + ", CacheKey:" + (key==null?"null":key.toString()), e);
            }
        }
        // 更新
        else if(taskType == IAtomicLogic.UPDATE_BY_PK){
            try {
                tableDesc.tableStrategy.update(contexts, entities);
            }catch(Exception e){
                log.error("异步更新实体对象发生异常！table:" + tableDesc.entityName + ", CacheKey:" + (key==null?"null":key.toString()), e);
            }
        }
        // 删除
        else if(taskType == IAtomicLogic.DELETE_BY_PK){
            try {
                tableDesc.tableStrategy.delete(contexts, entities);
            }catch(Exception e){
                log.error("异步删除实体对象发生异常！table:" + tableDesc.entityName + ", CacheKey:" + (key==null?"null":key.toString()), e);
            }
        }
    }

    /**
     * 第二步, 各类型的异步任务根据具体需求重写, 此操作一定会被执行不会被合并
     */
    public abstract V subCall2();

}
