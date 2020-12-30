package yushanmufeng.localcache;

/**
 * 表权重的读取保存接口
 */
public interface IExpireRateLoader {

    /**
     * 初始化组件
     */
    public void initiation();

    /**
     * 加载权重系数
     * @return
     */
    public void load(TableDescribe<Cacheable> tableDesc);

    /**
     * 保存权重系数
     * @param tableDesc 表描述
     */
    public void save(TableDescribe<Cacheable> tableDesc);

}
