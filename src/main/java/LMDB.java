package tuplejump.lmdb;

import com.sun.prism.shader.Solid_TextureYV12_AlphaTest_Loader;
import org.fusesource.lmdbjni.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.fusesource.lmdbjni.Constants.*;


public class LMDB {
    String dbPath;

    public LMDB(String dbPath) {
        this.dbPath = dbPath;
    }

    public void write(String key, String value) {
        Env env = new Env(dbPath);
        Database db = env.openDatabase();
        Transaction tx = env.createWriteTransaction();
        // cursors must close before write transactions!
        BufferCursor cursor = db.bufferCursor(tx);
        cursor.first();
        // write utf-8 ending with NULL byte
        cursor.keyWriteUtf8(key);
        cursor.valWriteUtf8(value);
        cursor.overwrite();
        cursor.close();

        // commit changes or try-with-resources will auto-abort
        tx.commit();
        db.close();

        env.close();
    }

    public void write(int key, String value) {
        Env env = new Env(dbPath);
        Database db = env.openDatabase();
        Transaction tx = env.createWriteTransaction();
        // cursors must close before write transactions!
        BufferCursor cursor = db.bufferCursor(tx);
        cursor.first();
        // write utf-8 ending with NULL byte
        cursor.keyWriteInt(key);
        //  cursor.keyWriteUtf8(key);
        cursor.valWriteUtf8(value);
        cursor.overwrite();
        cursor.close();
        // commit changes or try-with-resources will auto-abort
        tx.commit();
        db.close();

        env.close();

    }

    public String read(String key) {
        String value;
        Env env = new Env(dbPath);
        Database db = env.openDatabase();
        Transaction tx = env.createReadTransaction();
        BufferCursor cursor = db.bufferCursor(tx);
        // iterate from first item and forwards
        cursor.first();
        // find first key greater than or equal to specified key.
        cursor.seek(bytes(key));

        // read utf-8 string from position until NULL byte
        cursor.keyUtf8(0);
        value = cursor.valUtf8(0).getString().toString();

        db.close();
        env.close();

        return value;
    }


    public void byteWrite(String key, byte[] value) {
        Env env = new Env(dbPath);
        Database db = env.openDatabase();
        Transaction tx = env.createWriteTransaction();
        // cursors must close before write transactions!
        BufferCursor cursor = db.bufferCursor(tx);
        cursor.first();
        // write utf-8 ending with NULL byte
        cursor.keyWriteUtf8(key);
        cursor.valWriteBytes(value);

        cursor.overwrite();
        cursor.close();

        // commit changes or try-with-resources will auto-abort
        tx.commit();
        db.close();

        env.close();

    }

    public byte[] byteRead(String key) {
        byte[] value;
        Env env = new Env(dbPath);
        Database db = env.openDatabase();
        Transaction tx = env.createReadTransaction();
        BufferCursor cursor = db.bufferCursor(tx);
        // iterate from first item and forwards
        cursor.first();
        // find first key greater than or equal to specified key.
        cursor.seek(bytes(key));

        // read utf-8 string from position until NULL byte
        cursor.keyUtf8(0);
        value = cursor.valBytes();
        cursor.close();
        db.close();
        env.close();
        return value;

    }

    public void writeObject(String key, Object object) throws IOException {
        byte[] objectBytes = serialize(object);
        byteWrite(key, objectBytes);

    }

    public Object readObject(String key) throws IOException, ClassNotFoundException {
        byte[] objectBytes = byteRead(key);
        return deserialize(objectBytes);
    }


    private static byte[] serialize(Object object) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(object);
        return out.toByteArray();
    }

    private static Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        ObjectInputStream is = new ObjectInputStream(in);
        return is.readObject();
    }

    public void writeInt(String key, int value) {
        Env env = new Env(dbPath);
        Database db = env.openDatabase();
        Transaction tx = env.createReadTransaction();
        BufferCursor cursor = db.bufferCursor(tx);

        cursor.first();
        // write utf-8 ending with NULL byte
        cursor.keyWriteUtf8(key);
        //  cursor.keyWriteUtf8(key);
        cursor.valWriteInt(value);

        cursor.overwrite();
        cursor.close();

        db.close();
        env.close();
    }

    public int readInt(String key) {

        int value;
        Env env = new Env(dbPath);
        Database db = env.openDatabase();
        Transaction tx = env.createReadTransaction();
        BufferCursor cursor = db.bufferCursor(tx);
        // iterate from first item and forwards
        cursor.first();
        // find first key greater than or equal to specified key.
        cursor.seek(bytes(key));

        // read utf-8 string from position until NULL byte
        cursor.keyUtf8(0);
        value = cursor.valInt(0);
        cursor.close();
        db.close();
        env.close();

        return value;
    }

    public int keyCount() {
        int count = 0;
        Env env = new Env(dbPath);
        Database db = env.openDatabase();
        Transaction tx = env.createWriteTransaction();
        BufferCursor cursor = db.bufferCursor(tx);

        cursor.first();
        while (cursor.next()) {
            // read a position in buffer
            cursor.keyByte(0);
            cursor.valByte(0);
            count++;
        }
        cursor.close();
        db.close();
        env.close();

        return count + 1;
    }

    public ArrayList<byte[]> readNValues(int valuesCount) {
        Env env = new Env(dbPath);
        Database db = env.openDatabase();
        Transaction tx = env.createWriteTransaction();
        BufferCursor cursor = db.bufferCursor(tx);
        ArrayList<byte[]> records = new ArrayList<byte[]>(valuesCount);

        cursor.first();
        int i = 0;
        while (cursor.next() && i < valuesCount) {
            // read a position in buffer
            cursor.keyByte(0);
            cursor.keyUtf8(0);
            byte[] record;
            record = cursor.valBytes();
            records.add(record);
            i++;
        }
        cursor.close();
        db.close();
        env.close();
        return records;
    }

    public ArrayList<byte[]> readAllValues() {
        Env env = new Env(dbPath);
        Database db = env.openDatabase();
        Transaction tx = env.createWriteTransaction();
        BufferCursor cursor = db.bufferCursor(tx);
        ArrayList<byte[]> records = new ArrayList<byte[]>(keyCount() + 1);
        cursor.first();
        int i = 0;
        cursor.keyUtf8(0);
        byte[] record;
        record = cursor.valBytes();
        records.add(record);

        while (cursor.next() && i <= keyCount()) {
            // read a position in buffer
            //  cursor.keyByte(0);
            cursor.keyUtf8(0);

            record = cursor.valBytes();
            records.add(record);
            i++;
        }
        cursor.close();
        db.close();
        env.close();
        return records;
    }

    public void forwardTraverse() {
        Env env = new Env(dbPath);
        Database db = env.openDatabase();
        Transaction tx = env.createWriteTransaction();
        BufferCursor cursor = db.bufferCursor(tx);
        Object[] objects = new Object[keyCount()];
        cursor.first();
        while (cursor.next()) {
            // read a position in buffer
            cursor.keyUtf8(0);
            System.out.println(cursor.valUtf8(0).getString().toString());
        }
        cursor.close();
        db.close();
        env.close();

    }
/*
    public Object[] reverseTraverse{
        Env env = new Env(dbPath);
        Database db = env.openDatabase();
        Transaction tx = env.createWriteTransaction();
        BufferCursor cursor = db.bufferCursor(tx);
        Object[] objects= new Object[keyCount()];
        cursor.last();
        while (cursor.prev()) {
            // read a position in buffer
            cursor.keyByte(0);

        }
        cursor.close();
        db.close();
        env.close();
        return objects;

    }*/

    public void delete(String key) {
        Env env = new Env(dbPath);
        Database db = env.openDatabase();
        Transaction tx = env.createWriteTransaction();
        BufferCursor cursor = db.bufferCursor(tx);
        // iterate from first item and forwards
        cursor.seek(bytes(key));
        // delete cursor position
        cursor.delete();
        tx.commit();
        cursor.close();
        db.close();
        env.close();
    }
}
