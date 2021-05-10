package yushanmufeng.localcache.atomic;

import yushanmufeng.localcache.CacheKey;
import yushanmufeng.localcache.Cacheable;

import java.util.List;
import java.util.Map;

/**
 * 根据主键删除完成
 */
public class AtomicDeleteByPkFinish implements IAtomicLogic{

    /** 主键对应数据的当前状态 pk-ConcurrentStatus;仅在exec0方法中检测和操作此状态 */
    private final Map<CacheKey, WorkingLogic> workingLogics;

    public AtomicDeleteByPkFinish(Map<CacheKey, WorkingLogic> workingLogics){
        this.workingLogics = workingLogics;
    }

    @Override
    public void handle(CacheKey key, List<CacheKey> keyList, Cacheable entity, List<Cacheable> entities) {
        WorkingLogic workingLogic = workingLogics.get(key);
        // 删除掉一条删除记录
        workingLogic.deletingCount --;
        if(workingLogic.clearTempAttrByTaskCount()){   // 没有进行中的查询操作和增删改操作，可以删掉整个记录对象
            workingLogics.remove(key);
        }
    }

}
