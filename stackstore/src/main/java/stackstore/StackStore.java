package stackstore;

import com.google.gson.Gson;
import com.leansoft.bigqueue.BigArrayImpl;
import com.leansoft.bigqueue.IBigArray;
import kvstore.RocksDbKV;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by suresh on 13/1/18.
 */
public class StackStore {
    IBigArray bigArray;
    RocksDbKV kvstore;
    Gson gson = new Gson();
    public StackStore(String dbPath) {
        try {
            this.bigArray = new BigArrayImpl(dbPath,"RealTimeQueue");
            kvstore = new RocksDbKV(dbPath+"liststorekv");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public boolean createStack(String stackName){
        try {
            if(kvstore.getDb().get(stackName.getBytes())!=null){
                StackNode stackNode = new StackNode("ROOT");
                stackNode.setPrev(-1l);
                stackNode.setListName(stackName);
                stackNode.setData(new HashMap<String,Object>());
                long evtId=-1l;
                try {
                    evtId = bigArray.append(stackNode.toString().getBytes());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                kvstore.putLong(stackName+"ROOT",evtId);
                kvstore.putLong(stackName+"CUR",evtId);
            }else{
                return false;
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return true;
    }

    public synchronized long addEvent(String stackName, String eventName,HashMap<String,Object> data){
        Long cur = kvstore.getLong(stackName+"CUR");
        if (cur==null){
            return -1;
        }else{
            StackNode stackNode = new StackNode(eventName);
            stackNode.setPrev(cur);
            stackNode.setListName(stackName);
            stackNode.setData(data);
            long evtId=-1l;
            try {
                evtId = bigArray.append(stackNode.toString().getBytes());
                kvstore.putLong(stackName+"CUR",evtId);
                return evtId;
            } catch (IOException e) {
                e.printStackTrace();
                return -1;
            }
        }
    }

    public synchronized StackNode getPrevNode(long evtId){
        if (evtId==-1){
         return null;
        }
        try {
            StackNode stackNode = gson.fromJson(new String(bigArray.get(evtId)),StackNode.class);
            return gson.fromJson(new String(bigArray.get(stackNode.getPrev())),StackNode.class);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
