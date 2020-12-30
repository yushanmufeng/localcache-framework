package yushanmufeng.localcache.util;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 可以随机访问Map内元素的工具类
 * 在较大数据量的情况下，相较于遍历每个元素比对，可以提升效率
 * 目前支持jdk8以上、支持ConcurrentHashMap
 *
 * @author xuefeng.zhou
 */
public class MapRandomAccessUtil {

    private static Field tableField = null;
    private static Field nextField = null;

    public static void init(){
        try{
            tableField = ConcurrentHashMap.class.getDeclaredField("table");
            tableField.setAccessible(true);
            ConcurrentHashMap<String, String> tempMap = new ConcurrentHashMap<String, String>();
            tempMap.put("test", "test");
            Map.Entry<String, String>[] table = getTable(tempMap);
            for(Map.Entry<String, String> entry : table){
                if(entry != null){
                    nextField = entry.getClass().getDeclaredField("next");
                    nextField.setAccessible(true);
                    break;
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 获取map的底层保存entity数组
     * @param map
     * @return
     */
    @SuppressWarnings(value={"unchecked", "rawtypes"})
    private static<K, V> Map.Entry<K, V>[] getTable(Map<K, V> map){
        try{
            return (Map.Entry<K, V>[]) tableField.get(map);
        }catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 随机获取一定数量的元素
     * @param map
     * @param getCount 获取的数量
     * @raturn hashMap，要注意由于随机取得的元素重复，或元素值超出map大小，导致返回的键值对数量可能会少于getCount
     */
    public static <K, V> Map<K, V> getRandomEntrys(Map<K, V> map, int getCount){
        Map<K, V> resultMap = new HashMap<>(getCount);
        Map.Entry<K, V>[] table = getTable(map);
        Map.Entry<K, V> entry = null;
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for(int i = 0; i < getCount; i++){
            if( (entry = getRandomEntry(table, random)) != null){
                resultMap.put(entry.getKey(), entry.getValue());
            }
        }
        return resultMap;
    }

    /** 随机获取一个元素 */
    @SuppressWarnings(value={"unchecked", "rawtypes"})
    private static <K, V> Map.Entry<K, V> getRandomEntry(Map.Entry<K, V>[] table, ThreadLocalRandom random){
        Map.Entry<K, V> entry = null;
        Map.Entry<K, V> nextEntry;

        try {
            // 随机entry如果为空，则会尝试向后推移2次
            for(int i = random.nextInt(table.length), ctrl = 0; i < table.length && ctrl <= 3; ctrl++){
                entry = table[i];
                // 链表转化为红黑树的概率较低，按照默认负载因子0.75，概率小于百万分之一, 所以暂不处理红黑树的节点，只管链表节点。TODO 将来可以优化为支持红黑树
                if (entry != null && entry.getValue() != null && entry.getKey() != null) {
                    while (true) {
                        // 有一定概率会尝试取链表的下一节点。如果下一节点为空，则使用当前节点作为结果
                        if (random.nextInt(100) > 70) break;
                        nextEntry = (Map.Entry<K, V>) nextField.get(entry);
                        if (nextEntry == null || nextEntry.getKey() == null || nextEntry.getValue() == null) {
                            break;
                        } else {
                            entry = nextEntry;
                        }
                    }
                    break;
                }else{
                    i++;
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return entry;
    }


    /**
     * 性能测试
     *
     * 在10w条数据情况下，遍历全部数据耗时20-30ms，随机取1000条数据耗时1-2ms
     * 在100w条数据情况下，遍历全部数据耗时40-50ms，随机取1000条数据耗时1-2ms
     * 数据量越大，随机读取的场景下的性能优势越大
     */
    @SuppressWarnings(value={"unchecked", "rawtypes"})
    public static void main(String[] args) throws Exception{
        init();
        Map<String, Integer> map1 = new ConcurrentHashMap<>();
        long t1 = System.currentTimeMillis();
        int size = 100000;
        for(int i = 0; i < size; i++){
            map1.put(String.format("%08d", i), i);
        }
        System.out.println("准备数据耗时：" + (System.currentTimeMillis() - t1) + "ms");
        t1 = System.currentTimeMillis();
        Map<String, Integer> randomEntrys = getRandomEntrys(map1, 1000);
        for(Map.Entry<String, Integer> entry : randomEntrys.entrySet()){
            if(entry.getValue() <= System.currentTimeMillis()){
                entry.getKey();
            }
        }
        System.out.println("随机获取n条数据耗时：" + (System.currentTimeMillis() - t1) + "ms");
        System.out.println("randomEntrys:" + randomEntrys.toString());
        t1 = System.currentTimeMillis();
        for(Map.Entry<String, Integer> entry : map1.entrySet()){
            if(entry.getValue() <= System.currentTimeMillis()){
                entry.getKey();
            }
        }
        System.out.println("遍历全部数据耗时：" + (System.currentTimeMillis() - t1) + "ms");
    }

}
