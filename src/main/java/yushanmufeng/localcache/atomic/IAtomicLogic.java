package yushanmufeng.localcache.atomic;

import yushanmufeng.localcache.util.HashUtil;
import yushanmufeng.localcache.util.SimpleTaskExecutor;
import yushanmufeng.localcache.CacheKey;
import yushanmufeng.localcache.Cacheable;
import yushanmufeng.localcache.TableDescribe;
import yushanmufeng.localcache.WorkingLogic;
import yushanmufeng.localcache.task.MergingFutureTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

/** 核心原子操作逻辑,每种操作类型都要分别实现此接口用于处理各种操作,且方法中的操作均为内存操作，耗时DB任务要提交到处理线程中执行 */
public interface IAtomicLogic {

    Logger log = LoggerFactory.getLogger(IAtomicLogic.class);

    /** 原子操作的返回值使用线程绑定变量传递 */
    ThreadLocal<MergingFutureTask<?>> futureTaskLocal = new ThreadLocal<>();
    ThreadLocal<Cacheable> entityLocal = new ThreadLocal<>();
    ThreadLocal<List<Object>> pksLocal = new ThreadLocal<>();

    static void clearLocal(){
        futureTaskLocal.remove();
        entityLocal.remove();
        pksLocal.remove();
    }

    /** 空任务,用于标志结束队列 */
    int EMPTY_TASK = 0;
    /** 增 */
    int INSERT_BY_PK = 1;
    /** 增加完成 */
    int INSERT_BY_PK_FINISH = 2;
    /** 删 */
    int DELETE_BY_PK = 3;
    /** 删除完成 */
    int DELETE_BY_PK_FINISH = 4;
    /** 改 */
    int UPDATE_BY_PK = 5;
    /** 修改完成 */
    int UPDATE_BY_PK_FINISH = 6;
    /** 根据主键查单条 */
    int SELECT_BY_PK = 7;
    /** 根据主键查单完成 */
    int SELECT_BY_PK_FINISH = 8;
    /** 根据条件查询一组数据 */
    int SELECT_BY_CONDITION = 9;
    /** 根据条件查询一组数据完成 */
    int SELECT_BY_CONDITION_FINISH = 10;
    /** 卸载关联的缓存 */
    int UNLOAD_REFER_CACHE = 11;
    /** 计算汇总缓存使用的内存大小 */
    int SUM_MEM_BYTES = 12;
    /** 检测缓存过期 */
    int CHECK_CACHE_EXPIRE = 13;

    /** 处理原子操作方法 */
    void handle(CacheKey key, Cacheable entity, List<Cacheable> entities);

    /**
     * 计算当前最新状态
     *
     * @return 返回可能三种情况：数据存在/数据被删除/未被装载未知; <br> 当处于未知状态时， workingLogic==null表示查询结果已被处理，当前处理的回调为合并查询; 否则为查询任务返回
     */
    default int getCurState(WorkingLogic workingLogic){
        if(workingLogic == null || workingLogic.entity == null){
            return EntityState.DETACHED;    // 数据未被装载，一定不是插入、删除冲突状态, 但可能处于查询中
        }
        return workingLogic.entity._getStatus();
    }

    /** 返回负载最低的执行器 */
    default <V extends Runnable> SimpleTaskExecutor<V> getLoadLowestExecutor(SimpleTaskExecutor<V>[] executors, TableDescribe<Cacheable> tableDesc){
        return SimpleTaskExecutor.getLoadLowestExecutor(executors, HashUtil.hash(tableDesc, executors.length));
    }



}
