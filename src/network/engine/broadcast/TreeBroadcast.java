package network.engine.broadcast;
 import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.DataOutputStream;
import java.io.EOFException;

import java.io.InputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
 import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import network.engine.object.Standard;
import java.time.LocalDate;
import network.engine.object.KryoUtils;

public class TreeBroadcast {
    private Map<String, List<String>> treeStructure = new HashMap<>();
    private static final int PORT = 7506;
    private static final int BLOCK_SIZE = 250*1024;

    private List<String> allowedWorkerIPs = new ArrayList<>();
    private List<String> connectedWorkerIPs = new ArrayList<>();
    private Map<String, Boolean> workerStatus = new ConcurrentHashMap<>();
    private Map<String, Integer> workerNumbers = new HashMap<>();
    private Map<String, Socket> workerSockets = new ConcurrentHashMap<>();
    private ExecutorService workerPool = Executors.newCachedThreadPool();

    private static List<String> childrenIPs = new ArrayList<>();
    private static final String RECEIVED_FILE = "/home/ubuntu/mounted_vol/fichier_recu.bin";
    private static final String MASTER_IP = "192.168.165.27";  // Remplace par l'IP correcte du Master
    //private TreeBroadcast tree = new TreeBroadcast();

    /**************** Master ******************** */
    public void loadWorkerIPs(String workerFilePath) {
        try (BufferedReader reader = new BufferedReader(new FileReader(workerFilePath))) {
            String line;
            int workerNumber = 1;
            while ((line = reader.readLine()) != null) {
                String ip = line.trim();
                allowedWorkerIPs.add(ip);
                workerStatus.put(ip, false);
                workerNumbers.put(ip, workerNumber++);
            }
        } catch (IOException e) {
            System.err.println("❌ Erreur de lecture du fichier workers.txt: " + e.getMessage());
        }
    }

    public void startServer(int nbWorkers) {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("\n🚀 Master démarré sur le port " + PORT + "...");

            while (connectedWorkerIPs.size() < nbWorkers) {
                Socket workerSocket = serverSocket.accept();
                String workerIP = workerSocket.getInetAddress().getHostAddress();

                if (allowedWorkerIPs.contains(workerIP) && !workerStatus.get(workerIP)) {
                    workerStatus.put(workerIP, true);
                    connectedWorkerIPs.add(workerIP);
                    workerSockets.put(workerIP, workerSocket);
                    System.out.println("✅ Worker connecté depuis " + workerIP);
                    workerPool.execute(new WorkerHandler(workerSocket, workerIP));
                } else {
                    System.err.println("⚠️ Connexion non autorisée depuis " + workerIP);
                    workerSocket.close();
                }
            }
        } catch (IOException e) {
            System.err.println("❌ Erreur lors du démarrage du serveur Master: " + e.getMessage());
        }
    }

    public void buildTree(String masterIP, int childrenPerNode) {
        this.buildTheTreeStructure(connectedWorkerIPs, masterIP, childrenPerNode);
        System.out.println("\n🌲 Arbre de diffusion construit !");
        this.displayTree(masterIP, "", workerNumbers);
    }

    public void sendChildrenInfo(String masterIP) {
        for (String workerIP : connectedWorkerIPs) {
            try {
                Socket workerSocket = workerSockets.get(workerIP);
                PrintWriter output = new PrintWriter(workerSocket.getOutputStream(), true);
                List<String> children = this.getChildren(workerIP);

                if (children != null && !children.isEmpty()) {
                    String childrenInfo = "INFO: You have " + children.size() + " children: " + String.join(" ", children);
                    output.println(childrenInfo);
                    System.out.println("📩 Infos envoyées à " + workerIP + " -> " + childrenInfo);
                } else {
                    output.println("INFO: You are a leaf node. No children.");
                    System.out.println("🌿 Worker " + workerIP + " est une feuille.");
                }
            } catch (IOException e) {
                System.err.println("❌ Erreur d'envoi des infos à " + workerIP + ": " + e.getMessage());
            }
        }
    }

    public long startTimer() {
        System.out.println("\n📁 Début de l'envoi du fichier aux Workers...");
        return System.nanoTime();
    }

    public void sendFile(String masterIP, String fileToSend) {
    List<String> firstLevelWorkers = this.getChildren(masterIP);
    if (firstLevelWorkers.isEmpty()) {
        System.err.println("❌ Aucun Worker de premier niveau trouvé !");
        return;
    }

    File file = new File(fileToSend);

    for (String workerIP : firstLevelWorkers) {
        boolean fileSent = false;
        int retryCount = 0;

        while (!fileSent && retryCount < 3) {
            try (
                Socket socket = new Socket(workerIP, PORT);
                FileInputStream fis = new FileInputStream(file);
                // On utilise DataOutputStream pour écrire d’abord la longueur, puis les bytes
                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))
            ) {
                System.out.println("📤 Connexion à " + workerIP + " pour l'envoi du fichier...");
                byte[] buffer = new byte[BLOCK_SIZE];
                int bytesRead;
                int blockNumber = 1;

                while ((bytesRead = fis.read(buffer)) != -1) {
                    // 1) on écrit d'abord la taille réelle du bloc
                    dos.writeInt(bytesRead);
                    // 2) puis on écrit exactement 'bytesRead' octets
                    dos.write(buffer, 0, bytesRead);
                    dos.flush();
                    System.out.println("📦 Envoi bloc " + blockNumber + " (taille réelle = " + bytesRead + " octets)");

                    // On attend l'ACK correspondant
                    String response = reader.readLine();
                    if (response == null || !response.equals("ACK " + blockNumber)) {
                        throw new IOException("ACK non reçu pour le bloc " + blockNumber + " (réponse = " + response + ")");
                    }
                    blockNumber++;
                }

                // 3) on envoie un int 0 pour signaler la fin du fichier
                dos.writeInt(0);
                dos.flush();
                System.out.println("✅ Fichier envoyé avec succès à " + workerIP);
                fileSent = true;

            } catch (IOException e) {
                retryCount++;
                System.err.println("❌ Tentative " + retryCount + " échouée pour " + workerIP + " : " + e.getMessage());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        if (!fileSent) {
            System.err.println("❌ Échec de l'envoi du fichier à " + workerIP + " après 3 tentatives.");
        }
    }
}



    public void waitForCompletion(int nbWorkers) {
        try (ServerSocket completionServer = new ServerSocket(6100)) {
            System.out.println("🔵 Master attend la confirmation de fin des Workers...");
            int completedWorkers = 0;

            while (completedWorkers < nbWorkers) {
                try (Socket socket = completionServer.accept();
                     BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                    String message = reader.readLine();
                    if ("COMPLETED".equals(message)) {
                        completedWorkers++;
                        System.out.println("✅ Worker terminé (" + completedWorkers + "/" + nbWorkers + ")");
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("❌ Erreur lors de la réception des confirmations : " + e.getMessage());
        }
    }

    public void displayDuration(long startTime) {
        long end = System.nanoTime();
        long durationMs = (end - startTime) / 1_000_000;
        double durationSec = durationMs / 1000.0;
        System.out.println("📤 Temps total de diffusion : " + durationMs + " ms (" + durationSec + " secondes)");
    }

    private static class WorkerHandler implements Runnable {
        private final Socket socket;
        private final String ip;

        public WorkerHandler(Socket socket, String ip) {
            this.socket = socket;
            this.ip = ip;
        }

        @Override
        public void run() {
            try (BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                while (input.readLine() != null) {
                    // Garder la connexion ouverte
                }
            } catch (IOException e) {
                System.out.println("Worker déconnecté : " + ip);
            }
        }
    }
    /**************** Master ******************** */
/****************** worker************ */

public void configurePort(String[] args) {
    if (args.length > 0) {
        try {
            int port = Integer.parseInt(args[0]);
            System.out.println("🔧 Port configuré à : " + PORT);
        } catch (NumberFormatException e) {
            System.err.println("⚠️ Argument invalide, port par défaut utilisé : " + PORT);
        }
    }
}
    public  void connectToMaster() {
    try (Socket socket = new Socket(MASTER_IP, PORT);
         BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
 
        String message;
        while ((message = input.readLine()) != null) {
            System.out.println("📩 Message reçu du Master : " + message);
            System.out.println("🔍 Debug: Liste des enfants après connexion au Master: " + childrenIPs);
 
            if (message.startsWith("INFO: You have")) {
                int index = message.indexOf("children: ");
                if (index != -1) {
                    String childrenPart = message.substring(index + 9).trim(); // Récupérer la partie après "children: "
                    childrenIPs = Arrays.asList(childrenPart.split("\\s+")); // Séparer les IPs
                    System.out.println("✅ Enfants détectés : " + childrenIPs);
                } else {
                    System.out.println("⚠️ Problème de format du message du Master !");
                }
            } else if (message.equals("INFO: You are a leaf node. No children.")) {
                System.out.println("🌿 Aucun enfant détecté.");
            }
        }
 
    } catch (IOException e) {
        System.err.println("❌ Erreur de connexion au Master: " + e.getMessage());
    }
}
 
 
/**
 * Envoie le fichier complet (RECEIVED_FILE) à chacun des enfants listés dans childrenIPs.
 * Utilise le même protocole 'DataOutputStream + taille-bloc + données + ACK' que sendFile(...)
 */
private static void sendFileToChildren() {
    for (String childIP : childrenIPs) {
        boolean fileSent = false;
        int retryCount = 0;

        while (!fileSent && retryCount < 3) {
            try (
                Socket childSocket = new Socket(childIP, PORT);
                FileInputStream fis = new FileInputStream(RECEIVED_FILE);
                DataOutputStream dos = new DataOutputStream(childSocket.getOutputStream());
                BufferedReader childResponse = new BufferedReader(
                        new InputStreamReader(childSocket.getInputStream()))
            ) {
                System.out.println("📤 Envoi du fichier à " + childIP + "...");

                byte[] buffer = new byte[BLOCK_SIZE];
                int bytesRead;
                int blockNumber = 1;

                // 1) Pour chaque lecture du fichier, on envoie d'abord un int = taille réelle du bloc
                while ((bytesRead = fis.read(buffer)) != -1) {
                    dos.writeInt(bytesRead);
                    dos.write(buffer, 0, bytesRead);
                    dos.flush();
                    System.out.println("✔ Bloc " + blockNumber + " envoyé à " + childIP +
                                       " (taille réelle = " + bytesRead + " octets)");

                    // 2) On attend l'ACK du child
                    String response = childResponse.readLine();
                    if (response == null || !response.equals("ACK " + blockNumber)) {
                        throw new IOException("ACK non reçu pour le bloc " + blockNumber +
                                              " (réponse = " + response + ")");
                    }
                    blockNumber++;
                }

                // 3) On envoie l'entier 0 pour signaler la fin du fichier
                dos.writeInt(0);
                dos.flush();
                System.out.println("✅ Fichier envoyé avec succès à " + childIP);
                fileSent = true;

            } catch (IOException e) {
                retryCount++;
                System.err.println("❌ Erreur d'envoi à " + childIP + " : " + e.getMessage() +
                                   ". Tentative " + retryCount + "...");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        if (!fileSent) {
            System.err.println("❌ Échec de l'envoi du fichier à " + childIP + " après 3 tentatives.");
        }
    }
}

 
  public void startServer() {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("🚀 Worker prêt, écoute sur le port " + PORT + "...");
 
            while (true) {
                Socket socket = serverSocket.accept();
                System.out.println("🔗 Nouvelle connexion entrante !");
                new Thread(() -> handleFileReception(socket)).start();
            }
        } catch (IOException e) {
            System.err.println("❌ Erreur lors de l'attente du fichier : " + e.getMessage());
        }
    }
 
    private static void sendCompletionMessage() {
        int retryCount = 0;
        final int maxRetries = 1000;
   
        while (retryCount < maxRetries) {
            try (Socket socket = new Socket(MASTER_IP, 6100);
                 PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)) {
                writer.println("COMPLETED");
                System.out.println("📨 Worker a informé le Master qu'il a terminé.");
                return; // ✅ Succès, on quitte la boucle
            } catch (IOException e) {
                retryCount++;
                //System.err.println("❌ Tentative " + retryCount + " : impossible de contacter le Master. Nouvelle tentative dans 2s...");
                try {
                    Thread.sleep(500); // Attendre avant de réessayer
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
   
        System.err.println("❌ Impossible de contacter le Master après " + maxRetries + " tentatives.");
    }
   
 private static byte[] receiveBlock(DataInputStream inputStream, int expectedSize) throws IOException {
    ByteArrayOutputStream bufferStream = new ByteArrayOutputStream();
    byte[] buffer = new byte[expectedSize];
    int bytesRead = 0;

    while (bytesRead < expectedSize) {
        int result = inputStream.read(buffer, 0, expectedSize - bytesRead);
        if (result == -1) {
            break;
        }
        bufferStream.write(buffer, 0, result);
        bytesRead += result;
    }

    return bufferStream.toByteArray();
}

private void handleFileReception(Socket socket) {
    try (
        InputStream rawInput = socket.getInputStream();
        DataInputStream dis = new DataInputStream(rawInput);
        BufferedOutputStream fos = new BufferedOutputStream(new FileOutputStream(RECEIVED_FILE));
        PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)
    ) {
        int blockNumber = 1;
        System.out.println("📥 Début de la réception du fichier...");

        while (true) {
            // 1) on lit d'abord la taille du bloc
            int blockSize;
            try {
                blockSize = dis.readInt();
            } catch (EOFException e) {
                // si on arrive en EOF avant même de lire un entier, on considère que c'est fini
                break;
            }

            // 2) si blockSize <= 0 => signal de fin
            if (blockSize <= 0) {
                System.out.println("📩 Signal de fin de fichier reçu .");
                break;
            }

            // 3) on lit exactement 'blockSize' octets
            byte[] data = new byte[blockSize];
            dis.readFully(data);

            // 4) on écrit ces octets dans le fichier local
            fos.write(data);
            fos.flush();
            System.out.println("✔ Bloc " + blockNumber + " reçu (taille = " + blockSize + " octets)");

            // 5) on renvoie l’ACK pour ce bloc
            writer.println("ACK " + blockNumber);
            blockNumber++;
        }

        System.out.println("✅ Fichier reçu avec succès !");
        fos.close();

        // 6) une fois qu’on a tout reçu, on propage aux enfants
        sendFileToChildren();

        // 7) on informe le master que ce worker a terminé
        sendCompletionMessage();
        //deserializeStandard();
        deserializeWithKryo();

    } catch (IOException e) {
        System.err.println("⚠️ Erreur de réception du fichier : " + e.getMessage());
    }
}


/****************** worker************ */
    // Construire la structure de l'arbre
    public void buildTheTreeStructure(List<String> workerIPs, String masterIP, int childrenPerNode) {
        if (workerIPs.isEmpty()) {
            throw new IllegalArgumentException("❌ La liste des Workers est vide !");
        }
        if (childrenPerNode <= 0) {
            throw new IllegalArgumentException("❌ Nombre d'enfants par nœud doit être supérieur à zéro !");
        }
 
        treeStructure.put(masterIP, new ArrayList<>());
        int index = 0;
        Queue<String> queue = new LinkedList<>();
        queue.add(masterIP);
 
        while (index < workerIPs.size()) {
            String parent = queue.poll();
            List<String> children = new ArrayList<>();
 
            for (int i = 0; i < childrenPerNode && index < workerIPs.size(); i++) {
                String child = workerIPs.get(index++);
                children.add(child);
                queue.add(child);
            }
 
            treeStructure.put(parent, children);
        }
 
        // Debug: Affichage de l'arbre construit
        System.out.println("✅ Arbre construit avec succès !");
        treeStructure.forEach((parent, children) ->
            System.out.println(parent + " → " + children)
        );
    }
 
    // Afficher l'arbre
    public void displayTree(String root, String indent, Map<String, Integer> workerNumbers) {
        String nodeType = workerNumbers.containsKey(root) ? "Worker " + workerNumbers.get(root) : "Master";
        System.out.println(indent + "- " + nodeType + " : " + root);
 
        List<String> children = treeStructure.get(root);
        if (children != null) {
            for (String child : children) {
                displayTree(child, indent + "  ", workerNumbers);
            }
        }
    }
 
    // Récupérer les enfants d'un noeud donné
    public List<String> getChildren(String nodeIP) {
        return treeStructure.getOrDefault(nodeIP, new ArrayList<>());
    }
 
    // Implémentation d'une stratégie de diffusion de fichier
    public void strategizeFile(String filePath) {
        System.out.println("📂 Début de la stratégie de diffusion du fichier : " + filePath);
 
        for (String node : treeStructure.keySet()) {
            System.out.println("🔄 Fichier en cours d'envoi vers : " + node);
            // Simuler un envoi de fichier (ajouter une logique réelle ici)
        }
 
        System.out.println("✅ Fichier diffusé avec succès !");
    }


      /********************** Standard ************************/

private void serializeStandard() {
    String outputFile = "/home/ubuntu/mounted_vol/people_standard.bin";
    Standard.serializeStandard(outputFile);
}



private void deserializeStandard() {
    String inputFile = "/home/ubuntu/mounted_vol/fichier_recu.bin";
    Standard.deserializeStandard(inputFile);
}


  /********************** KRYO ************************/
 
private void serializeWithKryo() {
String kryoOutputFile = "/home/ubuntu/mounted_vol/people_kryo.bin";
KryoUtils.serializeWithKryo(kryoOutputFile);
}




private void deserializeWithKryo() {
   String reconstructedPath = "/home/ubuntu/mounted_vol/fichier_recu.bin";
KryoUtils.deserializeWithKryo(reconstructedPath);
}





  
}
 