
import java.io.File;
import java.io.IOException;
import utilities.HierarhicalDatabase;


public class Test {

    public static void main(String[] args) {
        try (HierarhicalDatabase db = new HierarhicalDatabase(new File("dbTest"))) {
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
