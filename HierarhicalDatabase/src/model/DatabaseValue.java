package model;

import java.util.ArrayList;
import java.util.Map;

public class DatabaseValue {

    private final String value;
    private final ArrayList<Map.Entry<String, DatabaseValue>> children;

    public DatabaseValue(String value) {
        this.value = value;
        this.children = null;
    }

    public DatabaseValue(ArrayList<Map.Entry<String, DatabaseValue>> children) {
        this.children = children;
        this.value = null;
    }

    public String getValue() {
        return value;
    }

    public ArrayList<Map.Entry<String, DatabaseValue>> getChildren() {
        return children;
    }
}
