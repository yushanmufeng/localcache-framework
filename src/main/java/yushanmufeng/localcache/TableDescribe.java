package yushanmufeng.localcache;

import yushanmufeng.localcache.config.LocalCacheConfig;
import org.apache.lucene.util.RamUsageEstimator;

import java.lang.reflect.Field;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 单表所包含的信息与可配置的策略
 * 每一张要被此缓存组件管理的表，都会为其创建一个此对象的实例
 */
public class TableDescribe<T extends Cacheable> {

    private final EntityCacheManager cache;

    /** 实体类的classname */
    public final String entityName;

    // 字符串类型属性个数。因为字符串类型的内存占用需要单独计算
    private int stringFieldCount = -1;
    private Field[] stringFields;

    /** 表过期时间权重系数,自动动态调整,范围：大于等于0 */
    public volatile int expireRate = 0;
    /** 表续期权重系数,自动动态调整,范围：大于0 */
    public final double renewalRate;
    /** 自适应系数步长 */
    private final int UP_ADAPT_STEP, DOWN_ADAPT_STEP;

    /** 用于调整权重系数的状态机 */
    public TableStateMachine stateMachine;

    /** 10分钟毫秒数常量 */
    private static final int TIME_10_MIN_MS = 10 * 60 * 1000;
    /** 1小时毫秒数常量 */
    private static final int TIME_60_MIN_MS = 60 * 60 * 1000;
    /** 10分钟统计数据历史记录最大长度、1小时统计数据历史记录最大长度 */
    private static final int MAX_10_MIN_SIZE = 12, MAX_60_MIN_SIZE = 24;

//    /** 缓存查询计数器，用于统计每查询n次执行一次过期移除 */
//    public AtomicInteger queryCount = new AtomicInteger();

    /** 此表所有的缓存占用内存大小的字节数 */
    public long totalMemBytes = 0L;

    /** 入栈操作标记位,当滚动到下一个时间段时，单线程入栈，允许此时忽略统计其他线程的访问统计数据 */
    private AtomicBoolean isPushingHourStats = new AtomicBoolean(), isPushing10MinStats = new AtomicBoolean();
    /** 最近24小时每个小时的访问统计 */
    private LinkedList<Stats> everyHourStats = new LinkedList<>();
    /** 最近120分钟每10分钟的访问统计 */
    private LinkedList<Stats> every10MinStats = new LinkedList<>();

    /** 一段时间的统计信息 */
    public class Stats{
        /** 开始统计时间 */
        public final long recordTime;
        /** 结束统计时间 */
        public final long endTime;
        /** 缓存命中次数 */
        public AtomicLong hitCount = new AtomicLong();
        /** 数据访问次数 */
        public AtomicLong visitCount = new AtomicLong();
        public Stats(long currentTime, int interval){
            this.recordTime = currentTime;
            this.endTime = recordTime + interval;
        }
        /** 命中率 */
        public double getHitRate(){
            return 100.0*hitCount.get()/visitCount.get();
        }

        @Override
        public String toString() {
            return "{" + hitCount + "/" + visitCount + "=" + (new DecimalFormat("#.00").format(getHitRate())) + "%}";
        }
    }

    /** 每张表的定制信息 */
    public final TableDataSource<T> tableStrategy;
    /** 缓存配置项 */
    private LocalCacheConfig config;


    public TableDescribe(LocalCacheConfig config, TableDataSource<T> tableStrategy, EntityCacheManager cache){
        this.renewalRate = config.renewalRate;
        this.UP_ADAPT_STEP = config.upAdaptStep;
        this.DOWN_ADAPT_STEP = config.downAdaptStep;
        this.config = config;
        this.cache = cache;
        this.tableStrategy = tableStrategy;
        this.entityName = tableStrategy.getEntityClass().getSimpleName();
        this.stateMachine = new TableStateMachine(this);
        initStringFields(tableStrategy.getEntityClass());
        // 初始化历史访问记录
        long curTime = System.currentTimeMillis();
        for(long i = curTime - (MAX_10_MIN_SIZE-1)*TIME_10_MIN_MS; i <= curTime ; i += TIME_10_MIN_MS){
            every10MinStats.addFirst(new Stats(i, TIME_10_MIN_MS));
        }

        for(long i = curTime - (MAX_60_MIN_SIZE-1)*TIME_60_MIN_MS; i <= curTime ; i += TIME_60_MIN_MS){
            everyHourStats.addFirst(new Stats(i, TIME_60_MIN_MS));
        }

    }

    private void initStringFields(Class<?> entityClass){
        List<Field> fieldList = new ArrayList<>();
        List<Field> stringFieldList = new ArrayList<>();
        while(entityClass != null){
            fieldList.addAll( Arrays.asList(entityClass.getDeclaredFields()) );
            entityClass = entityClass.getSuperclass();
        }
        for(Field field : fieldList){
            if(field.getType() == String.class){
                field.setAccessible(true);
                stringFieldList.add(field);
            }
        }
        if(stringFieldList.size() > 0){
            stringFields = new Field[stringFieldList.size()];
            stringFields = stringFieldList.toArray(stringFields);
            stringFieldCount = stringFieldList.size();
        }else{
            stringFieldCount = 0;
        }
    }

    /**
     * 计算单个实体类的内存占用, 单位字节
     */
    public long calcMemCache(T entity){
        long totalMem = RamUsageEstimator.sizeOfObject(entity);
        try {
            for(int i = 0; i < stringFieldCount; i++){
                String s = (String)stringFields[i].get(entity);
                long stringMem = s != null ? RamUsageEstimator.sizeOfObject(s) : 0;
                totalMem += s != null ? stringMem : 0;

            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return totalMem;
    }

    /**
     * 统计请求缓存(不管是否命中)
     */
    public void visit(long curTime){
        scrollRecordStats(curTime, every10MinStats, MAX_10_MIN_SIZE, isPushing10MinStats, TIME_10_MIN_MS, false);
        scrollRecordStats(curTime, everyHourStats, MAX_60_MIN_SIZE, isPushingHourStats, TIME_60_MIN_MS, false);
    }

    /**
     * 统计命中缓存
     */
    public void hit(long curTime){
        scrollRecordStats(curTime, every10MinStats, MAX_10_MIN_SIZE, isPushing10MinStats, TIME_10_MIN_MS, true);
        scrollRecordStats(curTime, everyHourStats, MAX_60_MIN_SIZE, isPushingHourStats, TIME_60_MIN_MS, true);
    }

    /**
     * 拼接全部每10分钟滚动的日志字符串
     */
    public String toStringEvery10MinStats(){
        return copyScrollStats(every10MinStats, isPushing10MinStats).toString();
    }

    /**
     * 拼接全部每小时滚动的日志字符串
     */
    public String toStringEveryHourStats(){
        return copyScrollStats(everyHourStats, isPushingHourStats).toString();
    }

    /**
     * 根据已记录的访问命中信息，分析计算出一个实时的命中得分,用于进一步的分析
     * 分数是一个与命中率正相关的值，可以反应出数据的命中情况
     *
     * @return  -1表示无效，暂无有效统计数据
     */
    public int getVisitScore(long maxTime){
        // 累加最近的访问记录计算命中率作为得分
        long visitCount = 0, hitCount = 0;
        LinkedList<Stats> minLogs = copyScrollStats(everyHourStats, isPushingHourStats);
        Iterator<Stats> minLogIterator = minLogs.iterator();
        Stats log1 = minLogIterator.next();
        Stats log2 = minLogIterator.next();
        Stats log3 = minLogIterator.next();
        Stats log4 = minLogIterator.next();
        Stats log5 = minLogIterator.next();
        Stats log6 = minLogIterator.next();
        // 繁忙模式
        if(log1.visitCount.get() > 0 || (log2.visitCount.get() > 0 && log3.visitCount.get() > 0) ){
            visitCount = log1.visitCount.get() + log2.visitCount.get() + log3.visitCount.get();
            hitCount = log1.hitCount.get() + log2.hitCount.get() + log3.hitCount.get();
        }
        // 较少访问
        else if( (log4.visitCount.get() > 0 || log5.visitCount.get() > 0 || log6.visitCount.get() > 0) && (log4.recordTime >= maxTime || log5.recordTime >= maxTime || log6.recordTime >= maxTime) ){
            visitCount = log1.visitCount.get() + log2.visitCount.get() + log3.visitCount.get() + log4.visitCount.get()+ log5.visitCount.get()+ log6.visitCount.get();
            hitCount = log1.hitCount.get() + log2.hitCount.get() + log3.hitCount.get() + log4.hitCount.get()+ log5.hitCount.get()+ log6.hitCount.get();
        }else{
            return -1;
        }
        return (int)(10000.0 * hitCount / visitCount);
    }

    /**
     * 获取全部滚动的日志的拷贝镜像
     * @param scrollList 统计信息记录栈
     * @param isPushing 乐观锁，统计日志滚动入栈时保证只有一个线程成功,防止重复入栈
     * @return
     */
    private LinkedList<Stats> copyScrollStats(LinkedList<Stats> scrollList, AtomicBoolean isPushing){
        LinkedList<Stats> copyList;
        while(isPushing.compareAndSet(false, true)){}
        try{
            copyList = new LinkedList<>(scrollList);
        }finally {
            isPushing.set(false);
        }
        return copyList;
    }

    /**
     * 滚动记录访问日志
     * @param curTime   当前时间
     * @param scrollList 统计信息记录栈
     * @param maxSize   栈的最大长度
     * @param isPushing 乐观锁，统计日志滚动入栈时保证只有一个线程成功,防止重复入栈
     * @param interval  时间间隔
     * @param isHit true为统计命中，false为统计访问
     */
    private void scrollRecordStats(long curTime, LinkedList<Stats> scrollList, int maxSize, AtomicBoolean isPushing, int interval, boolean isHit){
        Stats curStats = scrollList.getFirst();
        // 滚动记录
        if(curTime >= curStats.endTime){
            if(isPushing.compareAndSet(false, true)){
                try {
                    while (curTime >= curStats.endTime) {
                        Stats newStats = new Stats(curStats.endTime, interval);
                        curStats = newStats;
                        scrollList.addFirst(newStats);
                        if (scrollList.size() > maxSize) {
                            scrollList.removeLast();
                        }
                    }
                }finally {
                    isPushing.set(false);
                }
            }else return; // 滚动记录时发生冲突，舍弃个别的访问统计
        }
        if(isHit) curStats.hitCount.getAndIncrement();
        else curStats.visitCount.getAndIncrement();
    }

    /**
     * 根据内存占用情况动态调整权重系数
     */
    public void adaptRate(int memRatio, ThreadLocalRandom random, long curTime){
        stateMachine.update(memRatio, random, curTime);
    }

    /**
     * 表状态机
     *
     * 每张表都有一个状态机，会根据状态去动态优化各表的缓存权重系数
     *
     */
    public class TableStateMachine {

        /** 尝试提升权重状态 */
        public static final int GROWING_RATE_STATE = 1;
        /** 尝试降低权重状态 */
        public static final int REDUCING_RATE_STATE = 2;
        /** 稳定状态 */
        public static final int STABLE_STATE = 3;

        /**
         * 当前状态。
         * 状态转换规则：
         * 负载低于75，随机循环尝试提升权重和降低权重; 负载75-90，大概率尝试降低权重,小概率尝试提升权重; 负载大于90，切换为稳定态，不做调整和预期
         *
         * 稳定状态->提升权重状态：持续一段时间处于负载小于75%; 则状态波动
         * 稳定状态->降低权重状态：持续一段时间处于负载75-90%; 则状态波动, 大概率变为降低权重状态
         * 提升权重状态->稳定状态：负载大于90% or 尝试提升权重发现命中率无提升: 则回滚权重，变为稳定状态
         * 降低权重状态->稳定状态：负载大于90% or 尝试降低权重发现命中率下降: 回滚权重，变为稳定状态
         */
        private int state = STABLE_STATE;

        private final int PERCENT_75 = config.testPercentL1;   // 阈值1：默认百分之75
        private final int PERCENT_90 = config.testPercentL2;   // 阈值2：默认百分之90

        private long subStartTime = 0;  // 子状态起始时间
        private int subState = 0;   // 子状态
        public int subHistoryRate = 0; // 记录修改为临时权重之前的值，用于不满足预期时还原权重
        private int subHistoryScore = 0; // 记录修改为临时权重之前的命中分数，根据分数变动调整权重

        private static final int CLEAR_SUB_0 = 0;   // 无意义，表示当前为初始的子状态

        private static final int STABLE_SUB_1 = 1;   // 稳定状态时，处于区间范围1
        private static final int STABLE_SUB_2 = 2;   // 稳定状态时，处于区间范围2
        private static final int STABLE_SUB_3 = 3;   // 稳定状态时，处于区间范围3

        private static final int GROWING_SUB_1 = 11; // 增长状态
        private static final int GROWING_SUB_2 = 12; // 增长状态结束，准备进入下一轮增长

        private static final int REDUCING_SUB_1 = 21; // 降低状态
        private static final int REDUCING_SUB_2 = 22; // 降低状态结束，准备进入下一轮降低

        private TableDescribe<T> tableDesc;

        TableStateMachine(TableDescribe<T> tableDesc){
            this.tableDesc = tableDesc;
        }

        /**
         * 更新
         *
         * @param memRatio 当前负载
         */
        private void update(int memRatio, ThreadLocalRandom random, long curTime){
            int newState = 0;
            if(state == STABLE_STATE){
                newState = onStable(memRatio, random, curTime);
            }else if(state == GROWING_RATE_STATE){
                newState = onGrowing(memRatio, curTime);
            }else if(state == REDUCING_RATE_STATE){
                newState = onReducing(memRatio, curTime);
            }
            if(newState != state){
                state = newState;
            }
        }

        /** ================ 处于稳定状态 ================ */
        // 会在增长和降低之间波动,循环尝试调整
        private int lastChangeState = REDUCING_RATE_STATE;
        private final long MAX_STABLE_TIME = config.stableTimeMs;    // 状态切换间隔，单位ms，检测时超过此时间会尝试调整状态
        private int onStable(int memRatio, ThreadLocalRandom random, long curTime){
            if(memRatio < PERCENT_75){  // 负载低于75： 随机循环尝试提升权重和降低权重
                if(subState != STABLE_SUB_1){
                    subState = STABLE_SUB_1;
                    subStartTime = curTime;
                }
                if(curTime - subStartTime >= MAX_STABLE_TIME){    // 稳定状态达到一定时间，变换
                    // 如果过期时间等于0,不会尝试降低权重，只尝试提升
                    if(cache.calcStdExpireTime(tableDesc) <= 0){
                        lastChangeState = GROWING_RATE_STATE;
                    }else{
                        // 尝试的与上次相反的
                        lastChangeState = lastChangeState == REDUCING_RATE_STATE ? GROWING_RATE_STATE : REDUCING_RATE_STATE;
                    }
                    subState = CLEAR_SUB_0;
                    return lastChangeState;
                }
            }else if(memRatio < PERCENT_90){    // 负载处于75-90：  大概率尝试降低权重，小概率尝试提升权重
                if(subState != STABLE_SUB_2){
                    subState = STABLE_SUB_2;
                    subStartTime = curTime;
                }
                if(curTime - subStartTime >= MAX_STABLE_TIME){    // 稳定状态达到一定时间，变换
                    // 如果过期时间等于0,不会尝试降低权重，只尝试提升
                    if(cache.calcStdExpireTime(tableDesc) <= 0){
                        lastChangeState = GROWING_RATE_STATE;
                    }else if(random.nextInt(100) < 20) {  // 小概率尝试提升权重
                        lastChangeState = GROWING_RATE_STATE;
                    }else{  // 大概率尝试降低权重
                        lastChangeState = REDUCING_RATE_STATE;
                    }
                    subState = CLEAR_SUB_0;
                    return lastChangeState;
                }
            }else{  // 负载大于90： 不做任何调整
                if(subState != STABLE_SUB_3){
                    subState = STABLE_SUB_3;
                }
            }
            return STABLE_STATE;
        }

        /** ================ 处于增长状态 ================ */
        private final long TEST_TIME = config.testTimeMs;    // 测试增长权重的持续时间，单位ms
        private int onGrowing(int memRatio, long curTime){
            // 负载大于90%, 则回滚权重，变为稳定状态
            if(memRatio >= PERCENT_90){
                if(subState == GROWING_SUB_1){
                    expireRate = subHistoryRate;
                }
                subState = CLEAR_SUB_0;
                return STABLE_STATE;
            }else{  // 尝试提升权重， 如果发现命中率无提升, 则回滚权重，变为稳定状态
                if(subState != GROWING_SUB_1){
                    subHistoryScore = getVisitScore(curTime);
                    if(subHistoryScore == -1){
                        return GROWING_RATE_STATE;
                    }
                    subStartTime = curTime;
                    subState = GROWING_SUB_1;
                    // 记录当前的权重用于回滚、记录当前的命中率用于将来比较增加权重的效果
                    subHistoryRate = expireRate;
                    // 临时增加权重(快增慢减)
                    expireRate += UP_ADAPT_STEP;
                    LocalCacheFacade.log.debug("【测试提升权重】, table：" + tableStrategy.getEntityClass().getSimpleName() + ",当前分数:"+ subHistoryScore
                            +", 新权重:" + expireRate + ", 旧权重：" + subHistoryRate);
                }
                if(curTime - subStartTime >= TEST_TIME){    // 测试时间结束，检查是否达到预期，不满足预期则回滚权重，达到预期则进行下一轮的测试
                    int newScore = getVisitScore(curTime);
                    if(newScore == -1){
                        return GROWING_RATE_STATE;
                    }
                    if(newScore <= subHistoryScore){
                        LocalCacheFacade.log.debug("【测试提升权重】结果失败,回滚权重, table：" + tableStrategy.getEntityClass().getSimpleName() + ",当前分数:"+ newScore
                                + ",历史分数:" + subHistoryScore +", 回滚权重:" + subHistoryRate);
                        expireRate = subHistoryRate;    // 回滚，并切回稳定状态
                        return STABLE_STATE;
                    }else{
                        LocalCacheFacade.log.debug("【测试提升权重】结果成功,进行下一轮 table：" + tableStrategy.getEntityClass().getSimpleName() + ",当前分数:"+ newScore
                                + ",历史分数:" + subHistoryScore + ", 新权重:" + expireRate + ",历史权重:" + subHistoryRate);
                        subHistoryRate = expireRate;    // 成功，准备下一轮
                        subState = GROWING_SUB_2;
                    }

                }
            }
            return GROWING_RATE_STATE;
        }

        /** ================ 处于降低状态 ================ */
        private int onReducing(int memRatio, long curTime){
            // 负载大于90%, 则回滚权重，变为稳定状态
            if(memRatio >= PERCENT_90){
                if(subState == REDUCING_SUB_1){
                    expireRate = subHistoryRate;
                }
                subState = CLEAR_SUB_0;
                return STABLE_STATE;
            }else{  // 尝试降低权重， 如果发现命中率降低了, 则回滚权重，变为稳定状态
                if(subState != REDUCING_SUB_1){
                    subHistoryScore = getVisitScore(curTime);
                    if(subHistoryScore == -1){
                        return GROWING_RATE_STATE;
                    }
                    subStartTime = curTime;
                    // 记录当前的权重用于回滚、记录当前的命中率用于将来比较降低权重的效果
                    subHistoryRate = expireRate;
                    subState = REDUCING_SUB_1;

                    // 临时降低权重(快增慢减)
                    expireRate -= DOWN_ADAPT_STEP;
                    LocalCacheFacade.log.debug("【测试降低权重】, table：" + tableStrategy.getEntityClass().getSimpleName() + ",当前分数:"+ subHistoryScore
                            +", 新权重:" + expireRate + ", 旧权重：" + subHistoryRate);
                }
                if(curTime - subStartTime >= TEST_TIME){    // 测试时间结束，检查是否达到预期，不满足预期则回滚权重，达到预期则进行下一轮的测试
                    int newScore = getVisitScore(curTime);
                    if(newScore == -1){
                        return REDUCING_RATE_STATE;
                    }
                    if(newScore <= subHistoryScore){
                        LocalCacheFacade.log.debug("【测试降低权重】结果失败,回滚权重, table：" + tableStrategy.getEntityClass().getSimpleName() + ",当前分数:"+ newScore
                                + ",历史分数:" + subHistoryScore +", 回滚权重:" + subHistoryRate);
                        expireRate = subHistoryRate;    // 回滚，并切回稳定状态
                        return STABLE_STATE;
                    }else{
                        LocalCacheFacade.log.debug("【测试降低权重】结果成功,进行下一轮 table：" + tableStrategy.getEntityClass().getSimpleName() + ",当前分数:"+ newScore
                                + ",历史分数:" + subHistoryScore + ", 新权重:" + expireRate + ",历史权重:" + subHistoryRate );
                        subHistoryRate = expireRate;
                        subState = REDUCING_SUB_2;
                    }
                }
            }
            return REDUCING_RATE_STATE;
        }

    }

}
