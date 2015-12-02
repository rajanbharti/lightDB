import java.io.Serializable;

public class Person implements Serializable {

    transient int id;
    String name;
    int age;
    String city;

    Person(int id, String name, int age, String city) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.city = city;
    }

}
