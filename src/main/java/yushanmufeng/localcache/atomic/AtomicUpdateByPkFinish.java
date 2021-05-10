package yushanmufeng.localcache.atomic;

import yushanmufeng.localcache.CacheKey;
import yushanmufeng.localcache.Cacheable;

import java.util.List;
import java.util.Map;

/**
 * 根据主键更新-完成
 */
public class AtomicUpdateByPkFinish implements IAtomicLogic{

    private final Map<CacheKey, WorkingLogic> workingLogics;

    public AtomicUpdateByPkFinish(Map<CacheKey, WorkingLogic> workingLogics){
        this.workingLogics = workingLogics;
    }

    @Override
    public void handle(CacheKey key, List<CacheKey> keyList, Cacheable entity, List<Cacheable> entities) {
        WorkingLogic workingLogic = workingLogics.get(key);
        // 删除掉一条更新记录
        workingLogic.updatingCount --;
        if(workingLogic.clearTempAttrByTaskCount()){   // 没有进行中的查询操作和增删改操作，可以删掉整个记录对象
            workingLogics.remove(key);
        }
    }

}
