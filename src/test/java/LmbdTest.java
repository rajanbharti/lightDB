
import tuplejump.lmdb.LMDB;

import static org.junit.Assert.*;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.json.simple.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;


public class LmbdTest {

    LMDB lmdbReadWrite;

    @Before
    public void setup() {
        Config conf = ConfigFactory.load();
        String dbPath = conf.getString("dbPath");
        lmdbReadWrite = new LMDB(dbPath);
    }

    @Test
    public void readTest1() {
        lmdbReadWrite.write("a", "b");
        lmdbReadWrite.write("p", "q");
        lmdbReadWrite.write("ab", "cd");
        lmdbReadWrite.write("p", "r");

        assertEquals("b", lmdbReadWrite.read("a"));
        assertEquals("cd", lmdbReadWrite.read("ab"));
        assertEquals("r", lmdbReadWrite.read("p"));
    }

    @Test
    public void byteReadWriteTest() throws UnsupportedEncodingException {
        String testValue = "world";
        byte[] value = testValue.getBytes();
        lmdbReadWrite.byteWrite("hello", value);
        String readValue = new String(lmdbReadWrite.byteRead("hello"), "UTF-8");
        assertEquals(testValue, readValue);
    }

    @Test
    public void objectReadWriteTest() throws IOException, ClassNotFoundException {
        Person testPerson = new Person(1, "John", 26, "Hyd");
        lmdbReadWrite.writeObject("key", testPerson);
        Object readObject = lmdbReadWrite.readObject("key");
        if (readObject instanceof Person) {
            assertEquals(testPerson.age, ((Person) readObject).age);
            assertEquals(testPerson.name, ((Person) readObject).name);
            assertEquals(testPerson.city, ((Person) readObject).city);
        }
    }

    @Test
    public void jsonTest() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name", "John");
        jsonObject.put("age", new Integer(23));

        assertEquals("John", jsonObject.get("name"));
        assertEquals(23, jsonObject.get("age"));
    }

    @Test
    public void jsonWriteReadTest() throws IOException, ClassNotFoundException {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name", "John");
        jsonObject.put("age", new Integer(23));

        lmdbReadWrite.writeObject("json", jsonObject);
        Object readObject = lmdbReadWrite.readObject("json");
        if (readObject instanceof JSONObject) {
            assertEquals(jsonObject.get("name"), ((JSONObject) readObject).get("name"));
            assertEquals(jsonObject.get("age"), ((JSONObject) readObject).get("age"));
        }
    }

    
}
