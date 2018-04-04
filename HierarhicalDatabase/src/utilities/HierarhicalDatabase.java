package utilities;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.TreeMap;
import model.DatabaseValue;

public class HierarhicalDatabase implements AutoCloseable {

    private RandomAccessFile stream;
    private long nodeAdress;

    public HierarhicalDatabase(File file) throws IOException {
        this.stream = new RandomAccessFile(file, "rw");
    }

    private HierarhicalDatabase(RandomAccessFile stream, long adr) {
        this.stream = stream;
        this.nodeAdress = adr;
    }

    private HierarhicalDatabase(RandomAccessFile stream) {
        this(stream, 0);
    }

    @Override
    public void close() throws IOException {
        this.stream.close();
    }

    public String name() throws IOException {
        this.stream.seek(this.nodeAdress);
        short nameSize = this.stream.readShort();
        if (nameSize < 0) {
            nameSize = (short) -nameSize;
        }

        byte[] nameBytes = new byte[nameSize];
        this.stream.read(nameBytes);
        return new String(nameBytes);
    }

    public DatabaseValue readValue() throws IOException {
        this.stream.seek(this.nodeAdress);
        short nameSize = this.stream.readShort();
        if (nameSize > 0) {
            this.stream.skipBytes(nameSize);
            short valSize = this.stream.readShort();
            byte[] valueBytes = new byte[valSize];
            this.stream.read(valueBytes);
            return new DatabaseValue(new String(valueBytes));
        }

        nameSize = (short) -nameSize;
        this.stream.skipBytes(nameSize);

        DatabaseValue res = new DatabaseValue(new TreeMap<>());

        int listSize = 3;
        while (true) {
            for (int i = 0; i < listSize; i++) {
                long adr = this.stream.readLong();
                if (adr == 0) {
                    continue;
                }
                long filePointer = this.stream.getFilePointer();
                HierarhicalDatabase hd = new HierarhicalDatabase(this.stream, adr);
                res.getChildren().put(hd.name(), hd.readValue());
                this.stream.seek(filePointer);
            }

            long nextAdr = this.stream.readLong();
            if (nextAdr == 0) {
                break;
            }
            listSize = 10;
        }

        return res;
    }

    protected HierarhicalDatabase addParent(String name) throws IOException {
        this.stream.seek(this.nodeAdress);
        short nameSize = this.stream.readShort();
        nameSize = (short) -nameSize;
        this.stream.skipBytes(nameSize);

        int listSize = 3;
        while (true) {
            for (int i = 0; i < listSize; i++) {
                long slot = this.stream.readLong();
                if (slot == 0) {
                    this.stream.seek(this.stream.getFilePointer() - Long.BYTES);
                    long newAdr = this.newAdress();
                    this.stream.writeLong(newAdr);
                    this.createParent(newAdr, name);
                    return new HierarhicalDatabase(stream, newAdr);
                }
            }

            listSize = 10;
            long nextAdr = this.stream.readLong();
            if (nextAdr == 0) {
                long newAdr = this.newAdress();
                this.createList(newAdr, listSize);
                this.stream.seek(newAdr);
            } else {
                this.stream.seek(nextAdr);
            }
        }
    }

    protected HierarhicalDatabase addLeaf(String name, String value) throws IOException {
        this.stream.seek(this.nodeAdress);
        short nameSize = this.stream.readShort();
        nameSize = (short) -nameSize;
        this.stream.skipBytes(nameSize);

        int listSize = 3;
        while (true) {
            for (int i = 0; i < listSize; i++) {
                long slot = this.stream.readLong();
                if (slot == 0) {
                    this.stream.seek(this.stream.getFilePointer() - Long.BYTES);
                    long newAdr = this.newAdress();
                    this.stream.writeLong(newAdr);
                    this.createLeaf(newAdr, name, value);
                    return new HierarhicalDatabase(stream, newAdr);
                }
            }

            listSize = 10;
            long nextAdr = this.stream.readLong();
            if (nextAdr == 0) {
                long newAdr = this.newAdress();
                this.createList(newAdr, listSize);
                this.stream.seek(newAdr);
            } else {
                this.stream.seek(nextAdr);
            }
        }
    }

    public HierarhicalDatabase findChild(String name) throws IOException {
        this.stream.seek(this.nodeAdress);
        short nameSize = this.stream.readShort();
        nameSize = (short) -nameSize;
        this.stream.skipBytes(nameSize);

        int listSize = 3;
        while (true) {
            for (int i = 0; i < listSize; i++) {
                long slot = this.stream.readLong();
                if (slot == 0) {
                    continue;
                }

                long filePointer = this.stream.getFilePointer();
                HierarhicalDatabase child = new HierarhicalDatabase(stream, slot);
                if (child.name().equals(name)) {
                    return child;
                }
                this.stream.seek(filePointer);
            }

            listSize = 10;
            long nextAdr = this.stream.readLong();
            if (nextAdr == 0) {
                return null;
            } else {
                this.stream.seek(nextAdr);
            }
        }
    }

    public long newAdress() throws IOException {
        return this.stream.length();
    }

    protected void createParent(long adress, String name) throws IOException {
        this.stream.seek(adress);
        this.stream.writeShort(-name.length());

        this.stream.write(name.getBytes());

        this.createList(this.stream.getFilePointer(), 3);
    }

    protected void createLeaf(long adress, String name, String value) throws IOException {
        this.stream.seek(adress);
        this.stream.writeShort(name.length());
        this.stream.write(name.getBytes());
        this.stream.writeShort(value.length());
        this.stream.write(value.getBytes());
    }

    public void createList(long adress, int slots) throws IOException {
        for (int i = 0; i < slots; i++) {
            this.stream.writeLong(0);
        }
        this.stream.writeLong(0);
    }

    public static void main(String[] args) {
        new File("dbTest").delete();
        try (HierarhicalDatabase db = new HierarhicalDatabase(new File("dbTest"))) {
            db.createParent(0, "root");
            HierarhicalDatabase users = db.addParent("users");
            HierarhicalDatabase u = users.addParent("1");
            u.addLeaf("username", "username1");
            u.addLeaf("password", "pass2");
            u = users.addParent("2");
            u.addLeaf("username", "username2");
            u.addLeaf("password", "pass2");
            u = users.addParent("3");
            u.addLeaf("username", "username3");
            u.addLeaf("password", "pass3");

            DatabaseValue v = db.readValue();
            print(db.name(), v, 0);

            v = db.findChild("users").findChild("2").readValue();
            print(db.findChild("users").findChild("2").name(), v, 0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void print(String name, DatabaseValue v, int tabs) {
        for (int i = 0; i < tabs; i++) {
            System.out.print("  ");
        }

        if (v.getValue() != null) {
            System.out.println(name + ": " + v.getValue());
            return;
        }

        System.out.println(name);
        v.getChildren().entrySet()
                .forEach(e -> {
                    print(e.getKey(), e.getValue(), tabs + 1);
                });
    }
}
