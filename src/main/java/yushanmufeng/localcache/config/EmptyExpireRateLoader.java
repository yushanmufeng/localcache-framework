package yushanmufeng.localcache.config;

import yushanmufeng.localcache.Cacheable;
import yushanmufeng.localcache.IExpireRateLoader;
import yushanmufeng.localcache.TableDescribe;

/** 自适应权重持久化策略的空实现 */
public class EmptyExpireRateLoader implements IExpireRateLoader {

    /**
     * 初始化组件
     */
    @Override
    public void initiation() {
    }

    /**
     * 加载权重系数
     *
     * @param tableDesc
     * @return
     */
    @Override
    public void load(TableDescribe<Cacheable> tableDesc) {
    }

    /**
     * 保存权重系数
     *
     * @param tableDesc 表描述
     */
    @Override
    public void save(TableDescribe<Cacheable> tableDesc) {
    }

}
