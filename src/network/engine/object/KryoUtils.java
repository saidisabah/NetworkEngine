package network.engine.object;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class KryoUtils {

    public static class DataPayload {
        private String content;
        public DataPayload() {}
        public DataPayload(String content) { this.content = content; }
        public String getContent() { return content; }
        public void setContent(String content) { this.content = content; }
    }

    public static class Message {
        private String content;
        private LocalDate date;
        public Message() {}
        public Message(String content, LocalDate date) {
            this.content = content;
            this.date = date;
        }
        public String getContent() { return content; }
        public LocalDate getDate() { return date; }
    }

    public static class UserProfile {
        private String name;
        private int age;
        private List<Message> messages;
        public UserProfile() {}
        public UserProfile(String name, int age, List<Message> messages) {
            this.name = name;
            this.age = age;
            this.messages = messages;
        }
        public String getName() { return name; }
        public int getAge() { return age; }
        public List<Message> getMessages() { return messages; }
    }

    public static class Person {
        private String name;
        private String bio;
        public Person() {}
        public Person(String name, String bio) {
            this.name = name;
            this.bio = bio;
        }
        public String getName() { return name; }
        public String getBio() { return bio; }
    }

    public static void serializeWithKryo(String path) {
        Kryo kryo = new Kryo();
        kryo.register(ArrayList.class);
        kryo.register(Person.class);

        List<Person> people = new ArrayList<>();
        String longBio = "Ceci est une ligne de bio très longue et descriptive.\n".repeat(20_000);

        for (int i = 0; i < 1000; i++) {
            people.add(new Person("Person_" + i, longBio));
        }

        long startTime = System.nanoTime();
        try (Output output = new Output(new FileOutputStream(path))) {
            kryo.writeObject(output, people);
            long endTime = System.nanoTime();
            long durationMs = (endTime - startTime) / 1_000_000;
            double durationSec = durationMs / 1000.0;

            System.out.println("✅ Liste de 1 Go sérialisée.");
            System.out.println("⏱️ Temps de sérialisation : " + durationMs + " ms (" + durationSec + " sec)");
        } catch (IOException e) {
            System.err.println("❌ Erreur de sérialisation : " + e.getMessage());
        }

        File f = new File(path);
        System.out.println("📦 Taille du fichier : " + (f.length() / (1024 * 1024)) + " Mo");
    }


    public static void deserializeWithKryo(String path) {
        Kryo kryo = new Kryo();
        kryo.register(ArrayList.class);
        kryo.register(Person.class);

        File file = new File(path);
        System.out.println("📥 Chargement du fichier : " + path);
        System.out.println("📦 Taille du fichier : " + (file.length() / (1024 * 1024)) + " Mo");

        long startTime = System.nanoTime();
        try (Input input = new Input(new FileInputStream(path))) {
            @SuppressWarnings("unchecked")
            List<Person> people = (List<Person>) kryo.readObject(input, ArrayList.class);
            long endTime = System.nanoTime();

            long durationMs = (endTime - startTime) / 1_000_000;
            double durationSec = durationMs / 1000.0;

            System.out.println("✅ Désérialisation réussie. Nombre de personnes : " + people.size());
            System.out.println("⏱️ Temps de désérialisation : " + durationMs + " ms (" + durationSec + " sec)");

            for (int i = 0; i < Math.min(3, people.size()); i++) {
                Person p = people.get(i);
                System.out.println("👤 " + p.getName() + " | 📄 Longueur bio : " + p.getBio().length() + " caractères");
            }
        } catch (IOException e) {
            System.err.println("❌ Erreur de désérialisation : " + e.getMessage());
        }
    }
}
