package yushanmufeng.localcache;

import yushanmufeng.localcache.atomic.EntityState;
import com.google.gson.Gson;

/** 可缓存的实体类 */
public class Cacheable {

    /** 记录数据状态 */
    private volatile int _status = EntityState.GET_READY;

    public int _getStatus(){
        return _status;
    }
    public void _setStatus(int status){
        this._status = status;
    }

    public String toJsonStr(){
        Gson gson = new Gson();
        return gson.toJson(this);
    }

}
