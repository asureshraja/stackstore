import stackstore.StackStore;

import java.util.HashMap;

/**
 * Created by suresh on 2/3/18.
 */
public class Test {
    public static void main(String[] args) {
        StackStore ss = new StackStore("/tmp/test");
        HashMap<String,Object> data = new HashMap<String,Object>();
        ss.createStack("newStack");
        ss.addEvent("newStack","t1(can be anything/extra data)",data);
        ss.createStack("anotherStack");
    }
}
