package stackstore;

import com.google.gson.Gson;

import java.util.HashMap;

/**
 * Created by suresh on 13/1/18.
 */
public class StackNode {
    private String name;
    private String listName;

    public String getListName() {
        return listName;
    }

    public void setListName(String listName) {
        this.listName = listName;
    }


    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    public StackNode(String name) {
        this.name = name;
    }

    private long prev;
    private HashMap<String,Object> data;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getPrev() {
        return prev;
    }

    public void setPrev(long prev) {
        this.prev = prev;
    }

    public HashMap<String, Object> getData() {
        return data;
    }

    public void setData(HashMap<String, Object> data) {
        this.data = data;
    }
}
