package network.engine.object;

import java.io.*;
import java.util.*;
import java.nio.charset.StandardCharsets;

public class Standard {
        private final String FILE_TO_SEND = "/home/ubuntu/mounted_vol/people_standard.bin";


    public static class PersonStd implements Serializable {
        private static final long serialVersionUID = 1L;

        private String name;
        private String bio;

        public PersonStd(String name, String bio) {
            this.name = name;
            this.bio = bio;
        }

        public String getName() {
            return name;
        }

        public String getBio() {
            return bio;
        }
    }

public static void serializeStandard(String outputFile) {
    List<PersonStd> people = new ArrayList<>();

    // chaque bio ≈ 1 080 000 caractères (~1 Mo)
    String baseLine = "Ceci est une ligne de bio très longue et descriptive.\n";
    int repeatCount = 20_000;

    for (int i = 0; i < 1000; i++) {
        // on rend chaque bio unique en ajoutant l’index à la fin
        String uniqueBio = baseLine.repeat(repeatCount) + "ID_" + i;
        people.add(new PersonStd("Person_" + i, uniqueBio));
    }

    long start = System.nanoTime();
    try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(outputFile))) {
        oos.writeObject(people);
        long end = System.nanoTime();
        long durationMs = (end - start) / 1_000_000;
        double durationSec = durationMs / 1000.0;

        System.out.println("✅ Sérialisation standard réussie.");
        System.out.println("⏱️ Temps de sérialisation standard : " + durationMs + " ms (" + durationSec + " sec)");
    } catch (IOException e) {
        System.err.println("❌ Erreur de sérialisation standard : " + e.getMessage());
    }

    File f = new File(outputFile);
    System.out.println("📦 Taille du fichier standard : " + (f.length() / (1024 * 1024)) + " Mo");
}





    public static void deserializeStandard(String inputFile) {
        long start = System.nanoTime();
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(inputFile))) {
            @SuppressWarnings("unchecked")
            List<PersonStd> people = (List<PersonStd>) ois.readObject();
            long end = System.nanoTime();
            long durationMs = (end - start) / 1_000_000;
            double durationSec = durationMs / 1000.0;

            System.out.println("✅ Désérialisation standard réussie. Taille : " + people.size());
            System.out.println("⏱️ Temps de désérialisation standard : " + durationMs + " ms (" + durationSec + " sec)");

            for (int i = 0; i < Math.min(3, people.size()); i++) {
                System.out.println("👤 " + people.get(i).getName() + " | 📄 bio : " + people.get(i).getBio().length() + " caractères");
            }
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("❌ Erreur de désérialisation standard : " + e.getMessage());
        }
    }
}
