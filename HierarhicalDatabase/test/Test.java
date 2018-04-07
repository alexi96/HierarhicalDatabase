
import utilities.MemoryMap;


public class Test {

    public static void main(String[] args) {
        MemoryMap m = new MemoryMap();
        
        m.add(0, 10);
        m.add(10, 20);
        m.add(20, 30);
        m.add(30, 40);
        m.remove(20, 30);
        
        System.out.println(m.findFree(10));
    }
}
