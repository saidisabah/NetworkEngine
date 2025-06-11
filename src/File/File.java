package File;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class File {
    public static void main(String[] args) {
        String filePath = "/home/ubuntu/mounted_vol/fichier_2GO.bin";
        int blockSize = 1024 * 1024; // 1 Mo
        long totalSize = 2L * 1024 * 1024 * 1024; // 2 Go = 2 147 483 648 octets
        long totalBlocks = totalSize / blockSize; // 2048 blocs de 1 Mo

        byte[] block = new byte[blockSize];
        for (int i = 0; i < block.length; i++) {
            block[i] = 0x41; // 'A'
        }

        try (FileOutputStream fos = new FileOutputStream(filePath)) {
            for (long i = 0; i < totalBlocks; i++) {
                fos.write(block);
                if (i % 100 == 0) {
                    System.out.println("📦 Écrit : " + (i + 1) + " Mo");
                }
            }
            System.out.println("✅ Fichier de 2 Go créé : " + filePath);
        } catch (IOException e) {
            System.err.println("❌ Erreur lors de la création du fichier : " + e.getMessage());
        }
    }
}
