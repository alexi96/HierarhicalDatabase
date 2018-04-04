package model;

import java.util.TreeMap;

public class DatabaseValue {

    private final String value;
    private final TreeMap<String, DatabaseValue> children;

    public DatabaseValue(String value) {
        this.value = value;
        this.children = null;
    }

    public DatabaseValue(TreeMap<String, DatabaseValue> children) {
        this.children = children;
        this.value = null;
    }

    public String getValue() {
        return value;
    }

    public TreeMap<String, DatabaseValue> getChildren() {
        return children;
    }
}
