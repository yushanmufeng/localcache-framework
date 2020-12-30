package yushanmufeng.localcache;

import yushanmufeng.localcache.task.MergingFutureTask;
import yushanmufeng.localcache.util.SimpleTaskExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.FutureTask;

/** 记录单主键实体对象的并发操作冲突, 再原子操作方法中判断和更新 */
public class WorkingLogic {

    public int selectingCount = 0;      // 正在进行的查询任务数量
    public int insertingCount = 0;      // 正在进行的插入数量
    public int deletingCount = 0;       // 正在进行的删除数量
    public int updatingCount = 0;       // 正在进行的更新数量
    public Cacheable entity;            // 状态为最新的实体类
    public FutureTask<?> selectTask;    // 正在执行的查询任务,因为查询任务最多只有一个，对相同实体对象的查询会合并为一个请求
    public List<Runnable> selectCallback = new ArrayList<>();   // 查询任务执行完成后的回调函数
    public SimpleTaskExecutor<MergingFutureTask<?>> selectExecutor;   // 正在执行的查询任务使用的执行器
    public SimpleTaskExecutor<MergingFutureTask<?>> otherExecutor;    // 正在执行非查询任务使用的执行器

    /** 初始化一个查询原子操作记录 */
    public static WorkingLogic newSelect(MergingFutureTask<?> selectTask, SimpleTaskExecutor<MergingFutureTask<?>> selectExecutor){
        WorkingLogic workingLogic = new WorkingLogic();
        workingLogic.selectingCount = 1;
        workingLogic.selectTask = selectTask;
        workingLogic.selectExecutor = selectExecutor;
        selectExecutor.put(selectTask); // 同时启动异步任务
        return workingLogic;
    }

    /** 初始化一个插入原子操作记录 */
    public static WorkingLogic newInsert(MergingFutureTask<?> insertTask, SimpleTaskExecutor<MergingFutureTask<?>> otherExecutor, Cacheable entity){
        WorkingLogic workingLogic = new WorkingLogic();
        workingLogic.insertingCount = 1;
        workingLogic.otherExecutor = otherExecutor;
        workingLogic.entity = entity;
        otherExecutor.put(insertTask);  // 同时启动异步任务
        return workingLogic;
    }

    /** 进行中的任务是否有查询 */
    public boolean hasSelectTask(){
        return selectingCount > 0;
    }

    /** 进行中的任务是否有增删改 */
    public boolean hasOtherTask(){
        return insertingCount > 0 || deletingCount > 0 || updatingCount > 0;
    }

    /**
     * 根据当前的任务数，清理临时属性
     *
     * @return 是否被完全清空(即没有进行中的任务了)
     */
    public boolean clearTempAttrByTaskCount(){
        boolean hasSelectTask =  hasSelectTask();
        boolean hasOtherTask = hasOtherTask();
        if(!hasSelectTask){
            selectExecutor = null;
        }
        if(!hasOtherTask){
            entity = null;
            otherExecutor = null;
        }
        return hasSelectTask == false && hasOtherTask == false;
    }

}
