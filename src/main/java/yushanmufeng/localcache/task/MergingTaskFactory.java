package yushanmufeng.localcache.task;

import yushanmufeng.localcache.util.SimpleTaskExecutor;
import yushanmufeng.localcache.CacheKey;
import yushanmufeng.localcache.Cacheable;
import yushanmufeng.localcache.SingleTableAtomicLogic;
import yushanmufeng.localcache.TableDescribe;
import yushanmufeng.localcache.atomic.IAtomicLogic;

import java.util.List;

/** 创建可合并的异步任务-工厂 */
public class MergingTaskFactory {

    /** 创建空任务 */
    public static MergingFutureTask<?> createEmptyTask(SimpleTaskExecutor<MergingFutureTask<?>> executor, TableDescribe<Cacheable> tableDesc, TaskContext context){
        return new MergingFutureTask<>(new MergingCallable<Cacheable>(IAtomicLogic.EMPTY_TASK, executor, tableDesc, context, null, null) {
            @Override
            public Cacheable subCall2() {
                return null;
            }
        });
    }

    /** 创建查询任务 */
    public static MergingFutureTask<Cacheable> createSelectTask(SimpleTaskExecutor<MergingFutureTask<?>> executor, TableDescribe<Cacheable> tableDesc, TaskContext context, CacheKey key, Cacheable entity){
        return new MergingFutureTask<>(new MergingCallable<Cacheable>(IAtomicLogic.SELECT_BY_PK, executor, tableDesc, context, key, entity) {
            @Override
            public Cacheable subCall2() {
                return tableDesc.tableStrategy.selectByPK(key.keys[0]);
            }
        });
    }

    /** 创建合并查询任务 */
    public static MergingFutureTask<Cacheable> createMergingSelectTask(SimpleTaskExecutor<MergingFutureTask<?>> executor, TableDescribe<Cacheable> tableDesc, TaskContext context, CacheKey key, Cacheable entity, MergingFutureTask<Cacheable> realSelectTask){
        return new MergingFutureTask<>(new MergingCallable<Cacheable>(IAtomicLogic.SELECT_BY_PK, executor, tableDesc, context, key, entity) {
            @Override
            public Cacheable subCall2() {
                Cacheable result = null;
                try{
                    result = realSelectTask.get();
                }catch(Exception e){
                    e.printStackTrace();
                }
                return result;
            }
        });
    }

    /** 创建条件查询任务 */
    public static MergingFutureTask<?> createSelectByConditionTask(SimpleTaskExecutor<MergingFutureTask<?>> executor, TableDescribe<Cacheable> tableDesc, TaskContext context, CacheKey key, Cacheable entity){
        return new MergingFutureTask<>(new MergingCallable<List<Cacheable>>(IAtomicLogic.SELECT_BY_CONDITION, executor, tableDesc, context, key, entity) {
            @Override
            public List<Cacheable> subCall2() {
                return tableDesc.tableStrategy.select(key);
            }
        });
    }

    /** 创建条件查询合并任务 */
    public static MergingFutureTask<?> createMergingSelectByConditionTask(SimpleTaskExecutor<MergingFutureTask<?>> executor, TableDescribe<Cacheable> tableDesc, TaskContext context, CacheKey key, Cacheable entity, MergingFutureTask<List<Cacheable>> realSelectTask){
        return new MergingFutureTask<>(new MergingCallable<List<Cacheable>>(IAtomicLogic.SELECT_BY_CONDITION, executor, tableDesc, context, key, entity) {
            @Override
            public List<Cacheable> subCall2() {
                List<Cacheable> result = null;
                try{
                    result = realSelectTask.get();
                }catch(Exception e){
                    e.printStackTrace();
                }
                return result;
            }
        });
    }

    /** 创建更新任务 */
    public static MergingFutureTask<Cacheable> createUpdateTask(SingleTableAtomicLogic atomicLogic, SimpleTaskExecutor<MergingFutureTask<?>> executor, TableDescribe<Cacheable> tableDesc, TaskContext context, CacheKey key, Cacheable entity){
        return new MergingFutureTask<>(new MergingCallable<Cacheable>(IAtomicLogic.UPDATE_BY_PK, executor, tableDesc, context, key, entity) {
            @Override
            public Cacheable subCall2() {
                atomicLogic.exec(IAtomicLogic.UPDATE_BY_PK_FINISH, key, null, entity, null);
                return null;
            }
        });
    }

    /** 创建插入任务 */
    public static MergingFutureTask<Cacheable> createInsertTask(SingleTableAtomicLogic atomicLogic, SimpleTaskExecutor<MergingFutureTask<?>> executor, TableDescribe<Cacheable> tableDesc, TaskContext context, CacheKey key, Cacheable entity){
        return new MergingFutureTask<>(new MergingCallable<Cacheable>(IAtomicLogic.INSERT_BY_PK, executor, tableDesc, context, key, entity) {
            @Override
            public Cacheable subCall2() {
                atomicLogic.exec(IAtomicLogic.INSERT_BY_PK_FINISH, key, null, entity, null);
                return null;
            }
        });
    }

    /** 创建删除任务 */
    public static MergingFutureTask<Cacheable> createDeleteTask(SingleTableAtomicLogic atomicLogic, SimpleTaskExecutor<MergingFutureTask<?>> executor, TableDescribe<Cacheable> tableDesc, TaskContext context, CacheKey key, Cacheable entity){
        return new MergingFutureTask<>(new MergingCallable<Cacheable>(IAtomicLogic.DELETE_BY_PK, executor, tableDesc, context, key, entity) {
            @Override
            public Cacheable subCall2() {
                atomicLogic.exec(IAtomicLogic.DELETE_BY_PK_FINISH, key, null, entity, null);
                return null;
            }
        });
    }

    /** 创建内存合计任务 */
    public static MergingFutureTask<?> createSumMemBytesTask(SingleTableAtomicLogic atomicLogic, SimpleTaskExecutor<MergingFutureTask<?>> executor, TableDescribe<Cacheable> tableDesc, TaskContext context){
        return new MergingFutureTask<>(new MergingCallable<Cacheable>(IAtomicLogic.SUM_MEM_BYTES, executor, tableDesc, context, null, null) {
            @Override
            public Cacheable subCall2() {
                atomicLogic.exec(IAtomicLogic.SUM_MEM_BYTES, null, null, null, null);
                return null;
            }
        });
    }

    /** 创建检查缓存过期任务 */
    public static MergingFutureTask<?> createCheckCacheExpireTask(SingleTableAtomicLogic atomicLogic, SimpleTaskExecutor<MergingFutureTask<?>> executor, TableDescribe<Cacheable> tableDesc, TaskContext context){
        return new MergingFutureTask<>(new MergingCallable<Cacheable>(IAtomicLogic.CHECK_CACHE_EXPIRE, executor, tableDesc, context, null, null) {
            @Override
            public Cacheable subCall2() {
                atomicLogic.exec(IAtomicLogic.CHECK_CACHE_EXPIRE, null, null, null, null);
                return null;
            }
        });
    }

}
