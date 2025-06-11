
package network.engine.broadcast;

import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import network.engine.object.Standard;
import java.time.LocalDate;
import network.engine.object.KryoUtils;

public class Cornet {

    // Champs partagés Master/Worker
    private final int PORT = 8002;
    private final int COMPLETION_PORT = 8500;
    private final int EXCHANGE_PORT = 8017;
    private final static int NOTIFICATION_PORT = 8300;
    private final int BLOCK_SIZE = 100 * 1024 * 1024;
    private final String FILE_TO_SEND = "/home/ubuntu/mounted_vol/fichier_1go.bin";
    private final String STORAGE_PATH = "/home/ubuntu/mounted_vol/";
    private final String WORKERS_FILE = "/home/ubuntu/pqdag/workers";
    private final String MASTER_IP = "192.168.165.27";
    private final int NbWorkers = 10;

    private long globalStartTime;
    private static Map<Integer, String> blockToIp = new LinkedHashMap<>();
    private static Map<String, Set<Integer>> ipToBlocks = new LinkedHashMap<>();
    private List<Socket> connectedWorkers = new ArrayList<>();
    private static Set<String> allowedWorkerIPs = new LinkedHashSet<>();
    private volatile static boolean distributionTerminée = false;

    private int workerID;
    private int totalBlocksExpected = -1;
    

    // ===================== MASTER LOGIC =====================

    public void runAsMaster() {
        //serializeStandard();
        //serializeWithKryo();
        loadWorkerIPs();
        startNotificationListener();
        startMasterServer();
        globalStartTime = System.currentTimeMillis();
        startCompletionListener();
        distributeBlocksRoundRobin();
    }

private  void loadWorkerIPs() {
        try (BufferedReader reader = new BufferedReader(new FileReader(WORKERS_FILE))) {
            String line;
            while ((line = reader.readLine()) != null) {
                allowedWorkerIPs.add(line.trim());
            }
            System.out.println("✅ Liste des Workers chargée.");
        } catch (IOException e) {
            System.err.println("❌ Erreur de lecture du fichier workers : " + e.getMessage());
        }
    }
    private void startMasterServer() {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("🔵 Master en attente de connexions sur le port " + PORT + "...");
            while (connectedWorkers.size() < NbWorkers) {
                Socket workerSocket = serverSocket.accept();
                String workerIP = workerSocket.getInetAddress().getHostAddress();
                if (allowedWorkerIPs.contains(workerIP)) {
                    connectedWorkers.add(workerSocket);
                    System.out.println("✅ Nouveau Worker connecté : " + workerIP);
                } else {
                    System.out.println("❌ Connexion refusée : " + workerIP);
                    workerSocket.close();
                }
            }
        } catch (IOException e) {
            System.err.println("❌ Erreur de démarrage du serveur Master : " + e.getMessage());
        }
    }
    
    private void startNotificationListener() {
    new Thread(() -> {
        Map<String, Set<Integer>> receivedByWorker = new HashMap<>();
        int notificationCount = 0;
        boolean initialDistributionComplete = false;

        try (ServerSocket serverSocket = new ServerSocket(NOTIFICATION_PORT)) {
            System.out.println("🟢 Serveur de notification prêt sur le port " + NOTIFICATION_PORT);

            while (true) {
                Socket notifSocket = serverSocket.accept();
                DataInputStream dis = new DataInputStream(notifSocket.getInputStream());

                int blockId = dis.readInt();
                String senderIP = notifSocket.getInetAddress().getHostAddress();

                // MAJ de la DHT centrale
                updateDHT(blockId, senderIP);

                // MAJ locale des blocs reçus par ce Worker
                receivedByWorker.computeIfAbsent(senderIP, k -> new HashSet<>()).add(blockId);
                System.out.println("📥 Notification : Worker " + senderIP + " a reçu le bloc " + blockId);

                notifSocket.close();

                // Vérifie si tous les Workers ont reçu au moins 1 bloc : distribution initiale terminée ?
                boolean everyoneHasAtLeastOne = true;
                for (String ip : allowedWorkerIPs) {
                    if (!receivedByWorker.containsKey(ip) || receivedByWorker.get(ip).isEmpty()) {
                        everyoneHasAtLeastOne = false;
                        break;
                    }
                }

                // Active le mode "affichage périodique" une fois que tout le monde a commencé
                if (!initialDistributionComplete && everyoneHasAtLeastOne) {
                    initialDistributionComplete = true;
                    System.out.println("🚀 Début des échanges entre Workers !");
                }

                // Pendant l'échange entre Workers : afficher toutes les 5 notifications
                if (distributionTerminée) {
    notificationCount++;
    if (notificationCount % 20 == 0) {
        System.out.println("📊 DHT après " + notificationCount + " notifications depuis le début des échanges :");
        printDHT(blockToIp.size());
    }
}



                // Fin : tous les Workers ont tout reçu
                boolean allComplete = true;
                for (String ip : allowedWorkerIPs) {
                    Set<Integer> received = receivedByWorker.getOrDefault(ip, new HashSet<>());
                    if (received.size() < blockToIp.size()) {
                        allComplete = false;
                        break;
                    }
                }

                if (allComplete) {
                    System.out.println("\n✅ Tous les Workers ont reçu tous les blocs !");
                    printDHT(blockToIp.size());
                    break;
                }
            }
        } catch (IOException e) {
            System.err.println("❌ Erreur serveur de notification : " + e.getMessage());
        }
    }).start();
}
private  void distributeBlocksRoundRobin() {
        File file = new File(FILE_TO_SEND);
        int blockId = 1;

        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] buffer = new byte[BLOCK_SIZE];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                byte[] blockData = Arrays.copyOf(buffer, bytesRead);
                int targetIndex = (blockId - 1) % connectedWorkers.size();
                Socket targetWorker = connectedWorkers.get(targetIndex);
                sendBlockToWorker(targetWorker, blockId, blockData, bytesRead);

                String ip = targetWorker.getInetAddress().getHostAddress();
                blockToIp.put(blockId, ip);
ipToBlocks.computeIfAbsent(ip, k -> new LinkedHashSet<>()).add(blockId);

                blockId++;
            }
            System.out.println("✅ Tous les blocs ont été envoyés !");
            int totalBlocks = blockId - 1; // nombre réel de blocs envoyés

for (Socket worker : connectedWorkers) {
    DataOutputStream dos = new DataOutputStream(worker.getOutputStream());
    dos.writeInt(-99); // signal spécial : nombre de blocs
    dos.writeInt(totalBlocks); // ➕ envoyer totalBlocks
    dos.flush();
}

            distributionTerminée = true; // ➕ Active le droit d'afficher la DHT

            printDHT(blockId - 1);
            sendExchangePlans();  // <--- AJOUT ICI !

        } catch (IOException e) {
            System.err.println("❌ Erreur pendant l'envoi des blocs : " + e.getMessage());
        }
    }
private void sendBlockToWorker(Socket workerSocket, int blockNumber, byte[] blockData, int dataSize) {
        try {
            DataOutputStream dos = new DataOutputStream(workerSocket.getOutputStream());
            dos.writeInt(blockNumber); // Envoi de l'ID du bloc
            dos.writeInt(dataSize);    // Puis la taille
            dos.write(blockData, 0, dataSize); // Puis les données
            dos.flush();
            System.out.println("📤 Bloc_" + blockNumber + " envoyé à " + workerSocket.getInetAddress());
        } catch (IOException e) {
            System.err.println("❌ Erreur d'envoi au Worker : " + e.getMessage());
        }
    }private static void printDHT(int totalBlocks) {
    System.out.println("\n📋 Table de la DHT :");
    System.out.println("+---------------------+--------------------------------------------+--------------------------------------------+");
    System.out.println("| Adresse IP Worker   |                  Blocs détenus             |           Blocs manquants                  |");
    System.out.println("+---------------------+--------------------------------------------+--------------------------------------------+");

    for (String ip : allowedWorkerIPs) {
       Set<Integer> ownedSet = ipToBlocks.getOrDefault(ip, new LinkedHashSet<>());
List<Integer> owned = new ArrayList<>(ownedSet);

        Collections.sort(owned);

        List<Integer> missing = new ArrayList<>();
        for (int i = 1; i <= totalBlocks; i++) {
            if (!ownedSet.contains(i)) {
                missing.add(i);
            }
        }

        System.out.printf("| %-19s | %-42s | %-42s |\n", ip, owned.toString(), missing.toString());
    }

    System.out.println("+---------------------+--------------------------------------------+--------------------------------------------+");
}

private static void updateDHT(int blockId, String ip) {
    blockToIp.put(blockId, ip);
    ipToBlocks.computeIfAbsent(ip, k -> new LinkedHashSet<>()).add(blockId);
}

private  void sendExchangePlans() {
    for (Socket worker : connectedWorkers) {
        String ip = worker.getInetAddress().getHostAddress();
Set<Integer> ownedSet = ipToBlocks.getOrDefault(ip, new LinkedHashSet<>());
List<Integer> owned = new ArrayList<>(ownedSet);
        Set<Integer> missing = new LinkedHashSet<>();
        for (int i = 1; i <= blockToIp.size(); i++) {
            if (!owned.contains(i)) missing.add(i);
        }

        try {
            DataOutputStream dos = new DataOutputStream(worker.getOutputStream());
            dos.writeInt(-1); // signal spécial : début plan d’échange
            dos.writeInt(missing.size());
            for (int blockId : missing) {
                String sourceIp = blockToIp.get(blockId);
                dos.writeInt(blockId);
                dos.writeUTF(sourceIp);
            }
            dos.flush();
        } catch (IOException e) {
            System.err.println("❌ Erreur d’envoi du plan à " + ip + " : " + e.getMessage());
        }
    }
}
private  void startCompletionListener() {
    new Thread(() -> {
        int completedWorkers = 0;
        try (ServerSocket serverSocket = new ServerSocket(COMPLETION_PORT)) {
            System.out.println("⏳ En attente des Workers terminés...");

            while (completedWorkers < NbWorkers) {
                Socket socket = serverSocket.accept();
                BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String msg = br.readLine();
                if ("DONE".equals(msg)) {
                    completedWorkers++;
                    System.out.println("✅ Worker terminé (" + completedWorkers + "/" + NbWorkers + ")");
                }
                socket.close();
            }

            long elapsed = System.currentTimeMillis() - globalStartTime;
            System.out.println("⏱️ Temps total Cornet : " + (elapsed / 1000.0) + " secondes");
        } catch (IOException e) {
            System.err.println("❌ Erreur de réception des fins : " + e.getMessage());
        }
    }).start();
}

    // ===================== WORKER LOGIC =====================

    public void runAsWorker(int id) {
        this.workerID = id;
        startWorkerServer();

        try (Socket socket = new Socket(MASTER_IP, PORT)) {
            DataInputStream dis = new DataInputStream(socket.getInputStream());
            Map<Integer, String> blocksToRequest = new LinkedHashMap<>();

            while (true) {
                int blockId = dis.readInt();
                if (blockId == -99) {
                    totalBlocksExpected = dis.readInt();
                    continue;
                } else if (blockId == -1) {
                    int count = dis.readInt();
                    for (int i = 0; i < count; i++) {
                        int missingBlock = dis.readInt();
                        String sourceIp = dis.readUTF();
                        blocksToRequest.put(missingBlock, sourceIp);
                    }
                    fetchMissingBlocks(blocksToRequest);
                    break;
                }

                int dataSize = dis.readInt();
                byte[] buffer = new byte[dataSize];
                dis.readFully(buffer);
                try (FileOutputStream fos = new FileOutputStream(STORAGE_PATH + "block_" + blockId + ".txt")) {
                    fos.write(buffer);
                }
                System.out.println("📥 Worker_" + workerID + " a reçu bloc " + blockId);
                sendNotificationToMaster(blockId);
            }
        } catch (IOException e) {
            System.err.println("❌ Worker_" + workerID + " erreur de réception : " + e.getMessage());
        }
    }

private void fetchMissingBlocks(Map<Integer, String> tasks) {
    ExecutorService executor = Executors.newFixedThreadPool(4);
    List<Future<?>> futures = new ArrayList<>();

    for (Map.Entry<Integer, String> entry : tasks.entrySet()) {
        int blockId = entry.getKey();
        String fromIp = entry.getValue();

        Future<?> future = executor.submit(() -> {
            try (Socket socket = new Socket(fromIp, EXCHANGE_PORT)) {
                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                dos.writeInt(blockId);

                DataInputStream dis = new DataInputStream(socket.getInputStream());
                int dataSize = dis.readInt();
                byte[] buffer = new byte[dataSize];
                dis.readFully(buffer);

                String filename = STORAGE_PATH + "block_" + blockId + ".txt";
                try (FileOutputStream fos = new FileOutputStream(filename)) {
                    fos.write(buffer);
                }

                sendNotificationToMaster(blockId);
                System.out.println("📦 Bloc_" + blockId + " récupéré depuis " + fromIp);
            } catch (IOException e) {
                System.err.println("❌ Échec récupération bloc_" + blockId + " depuis " + fromIp);
            }
        });

        futures.add(future);
    }

    executor.shutdown();

    // 🔒 Attendre que tous les blocs soient effectivement récupérés
    for (Future<?> f : futures) {
        try {
            f.get();  // attend que chaque thread finisse
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    System.out.println("🧩 Tous les blocs ont été reçus. Reconstruction en cours...");
    reassembleFile(totalBlocksExpected);
}


private void startWorkerServer() {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(EXCHANGE_PORT)) {
                while (true) {
                    Socket socket = serverSocket.accept();
                    DataInputStream dis = new DataInputStream(socket.getInputStream());
                    int requestedBlockId = dis.readInt();

                    String filename = STORAGE_PATH + "block_" + requestedBlockId + ".txt";
                    File file = new File(filename);

                    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                    if (file.exists()) {
                        byte[] buffer = Files.readAllBytes(file.toPath());
                        dos.writeInt(buffer.length);
                        dos.write(buffer);
                        System.out.println("📤 Bloc_" + requestedBlockId + " envoyé à " + socket.getInetAddress());
                    } else {
                        dos.writeInt(0);
                        System.err.println("❌ Bloc_" + requestedBlockId + " introuvable !");
                    }
                    dos.flush();
                    socket.close();
                }
            } catch (IOException e) {
                System.err.println("❌ Erreur serveur d'échange : " + e.getMessage());
            }
        }).start();
    }


private void sendNotificationToMaster(int blockId) {
        try (Socket socket = new Socket(MASTER_IP, NOTIFICATION_PORT)) {
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            dos.writeInt(blockId);
            dos.flush();
            System.out.println("📡 Notification envoyée au Master : bloc_" + blockId + " reçu.");
        } catch (IOException e) {
            System.err.println("❌ Impossible de notifier le Master : " + e.getMessage());
        }
    }

private void reassembleFile(int totalBlocks) {
    String outputFile = STORAGE_PATH + "fichier_reconstruit.bin";
    try (FileOutputStream fos = new FileOutputStream(outputFile)) {
        for (int i = 1; i <= totalBlocks; i++) {
            File blockFile = new File(STORAGE_PATH + "block_" + i + ".txt");
            if (!blockFile.exists()) {
                System.err.println("❌ Bloc manquant : " + blockFile.getName());
                return;
            }

            byte[] data = Files.readAllBytes(blockFile.toPath());
            fos.write(data);
        }
        System.out.println("✅ Worker_" + workerID + " a reconstruit le fichier : " + outputFile);
        notifyCompletionToMaster();
        //deserializeStandard();
        //deserializeWithKryo() ;
    } catch (IOException e) {
        System.err.println("❌ Erreur lors de la reconstruction du fichier : " + e.getMessage());
    }
}
private void notifyCompletionToMaster() {
    try (Socket socket = new Socket(MASTER_IP, COMPLETION_PORT);
         PrintWriter pw = new PrintWriter(socket.getOutputStream(), true)) {
        pw.println("DONE");
        System.out.println("📬 Notification de fin envoyée au Master.");
    } catch (IOException e) {
        System.err.println("❌ Erreur d'envoi de la notification de fin : " + e.getMessage());
    }
}










  /********************** Standard ************************/

private void serializeStandard() {
    String outputFile = "/home/ubuntu/mounted_vol/people_standard.bin";
    Standard.serializeStandard(outputFile);
}



private void deserializeStandard() {
    String inputFile = "/home/ubuntu/mounted_vol/fichier_reconstruit.bin";
    Standard.deserializeStandard(inputFile);
}


  /********************** KRYO ************************/
 
private void serializeWithKryo() {
String kryoOutputFile = "/home/ubuntu/mounted_vol/people_kryo.bin";
KryoUtils.serializeWithKryo(kryoOutputFile);
}




private void deserializeWithKryo() {
   String reconstructedPath = "/home/ubuntu/mounted_vol/fichier_reconstruit.bin";
KryoUtils.deserializeWithKryo(reconstructedPath);
}



}
