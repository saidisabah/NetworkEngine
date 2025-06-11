
package network.engine.shuffle;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ShuffleOptimized {

    private static final int CONTROL_PORT = 9610;


    private static final String WORKERS_FILE = "/home/ubuntu/pqdag/workers";
    private static final int REQUEST_PORT = 9213;
private static final int DATA_PORT = 9310;
private static final int FIN_PORT = 9916;
private static final int BLOCK_SIZE = 250 * 1024 * 1024;
private static final int R_MULTITHREAD = 1;
private static final int S_MULTITHREAD = 1;

private static final String BASE_DIR = "/home/ubuntu/mounted_vol/";

private static final Queue<Request> requestQueue = new ConcurrentLinkedQueue<>();
private static final BlockingQueue<String> ackQueue = new ArrayBlockingQueue<>(100);
private static long startTime;
    private static long endTime;
 

    //*******************************************   master   ********************************************
    private static final int S = 3; // nombre de senders
    private static final int R = 3; // nombre de receivers

    private static final List<String> senderIPs = new ArrayList<>();
    private static final List<String> receiverIPs = new ArrayList<>();


    public static void loadWorkerIPs() throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(WORKERS_FILE))) {
            String line;
            int count = 0;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (count < S) senderIPs.add(line);
                else if (count < S + R) receiverIPs.add(line);
                count++;
            }
        }

    }

    public static void sendRolesAndPlans() {
        // 📦 Définition du plan de chaque sender
        Map<String, Map<String, Integer>> senderPlans = new HashMap<>();

        senderPlans.put(senderIPs.get(0), Map.of(
            receiverIPs.get(1), 3, 
            receiverIPs.get(2), 3  

        ));

        senderPlans.put(senderIPs.get(1), Map.of(
            receiverIPs.get(0), 2,  
            receiverIPs.get(2), 1   
        ));
        senderPlans.put(senderIPs.get(2), Map.of(
            receiverIPs.get(0), 2, 
            receiverIPs.get(1), 1   
        ));
        System.out.println("===========================================");
        
        int senderNum = 1;
        for (String senderIP : senderIPs) {
            System.out.println("📤 Sender " + senderNum + " : " + senderIP);
            Map<String, Integer> plan = senderPlans.get(senderIP);
            for (String receiverIP : receiverIPs) {
                if (plan.containsKey(receiverIP)) {
                    int blocCount = plan.get(receiverIP);
                    int receiverNum = receiverIPs.indexOf(receiverIP) + 1;
                    System.out.println("→ " + blocCount + " blocs à Receiver " + receiverNum + " (" + receiverIP + ")");
        }
    }
    System.out.println();
    senderNum++;
}
System.out.println("===========================================");

displayNonConflictPlan(senderPlans);

        // 🔄 Liste globale ordonnée des IPs du fichier
        List<String> workerOrder = new ArrayList<>();
        workerOrder.addAll(senderIPs);
        workerOrder.addAll(receiverIPs);

        // 📤 Envoi des rôles et plans triés aux senders
        for (int i = 0; i < S; i++) {
            String senderIP = senderIPs.get(i);
            String role = "sender" + (i + 1);
            Map<String, Integer> unsortedPlan = senderPlans.get(senderIP);
            Map<String, Integer> sortedPlan = sortPlanByWorkerOrder(unsortedPlan, workerOrder);
            sendToWorker(senderIP, role, sortedPlan);
        }

        // 📥 Envoi des plans triés aux receivers
        for (int i = 0; i < R; i++) {
            String receiverIP = receiverIPs.get(i);
            String role = "receiver" + (i + 1);
            Map<String, Integer> receivePlan = new HashMap<>();

            for (Map.Entry<String, Map<String, Integer>> entry : senderPlans.entrySet()) {
                String senderIP = entry.getKey();
                Map<String, Integer> subPlan = entry.getValue();
                if (subPlan.containsKey(receiverIP)) {
                    receivePlan.put(senderIP, subPlan.get(receiverIP));
                }
            }

            Map<String, Integer> sortedPlan = sortPlanByWorkerOrder(receivePlan, workerOrder);
            sendToWorker(receiverIP, role, sortedPlan);
        }
    }

    // 🔧 Trie les plans selon l'ordre défini dans le fichier workers
    private static Map<String, Integer> sortPlanByWorkerOrder(Map<String, Integer> plan, List<String> workerOrder) {
        Map<String, Integer> sorted = new LinkedHashMap<>();
        for (String ip : workerOrder) {
            if (plan.containsKey(ip)) {
                sorted.put(ip, plan.get(ip));
            }
        }
        return sorted;
    }

    // 📡 Envoi d’un rôle + plan à un worker via socket
    public static void sendToWorker(String ip, String role, Map<String, Integer> plan) {
        try (Socket socket = new Socket(ip, CONTROL_PORT);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {
            out.writeObject(role);
            out.writeObject(plan);
        } catch (IOException e) {
            System.err.println("❌ Erreur d'envoi à " + ip + " : " + e.getMessage());
        }
    }
    //*************************************************** master  ********************************************

    public static void listenForControl() {
        try (ServerSocket serverSocket = new ServerSocket(CONTROL_PORT)) {
            System.out.println("[Worker] En attente d'instructions (port " + CONTROL_PORT + ")...");
            while (true) {
                try (Socket socket = serverSocket.accept();
                     ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
    
                    String role = (String) in.readObject();
    
                    if (role.startsWith("sender")) {
                        startRequestListener();
                    
                    } else if (role.equals("receiver_exec")) {
                        @SuppressWarnings("unchecked")
                        List<String> execPlan = (List<String>) in.readObject();
                        System.out.println("📋 Plan local de réception :");
                        for (int i = 0; i < execPlan.size(); i++) {
                            System.out.println("Étape " + i + " : " + execPlan.get(i));
                        }
                        int totalExpected = (int) execPlan.stream().filter(s -> !s.equals("-")).count();
                        startDataReceiverServer(totalExpected);
                        sendRequestsFromExecutionPlan(execPlan);
                    }
    
                } catch (Exception e) {
                    System.err.println("❌ Erreur réception rôle/plan : " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("❌ Erreur serveur contrôle : " + e.getMessage());
        }
    }
    
    public static void displayNonConflictPlan(Map<String, Map<String, Integer>> senderPlans) {
        Map<String, Queue<String>> receiverToSenderQueue = new HashMap<>();
    
        // Construction de la file d'envoi pour chaque receiver
        for (String receiver : receiverIPs) {
            Queue<String> queue = new LinkedList<>();
            for (String sender : senderIPs) {
                Integer count = senderPlans.getOrDefault(sender, Map.of()).getOrDefault(receiver, 0);
                for (int i = 0; i < count; i++) {
                    queue.add(sender);
                }
            }
            receiverToSenderQueue.put(receiver, queue);
        }
    
        List<Map<String, String>> matrix = new ArrayList<>();
    
        while (true) {
            Set<String> usedSenders = new HashSet<>();
            Map<String, String> step = new LinkedHashMap<>();
    
            for (String receiver : receiverIPs) {
                Queue<String> queue = receiverToSenderQueue.get(receiver);
                String selectedSender = null;
                if (queue != null) {
                    for (String sender : queue) {
                        if (!usedSenders.contains(sender)) {
                            selectedSender = sender;
                            break;
                        }
                    }
                }
    
                if (selectedSender != null) {
                    step.put(receiver, selectedSender);
                    usedSenders.add(selectedSender);
                    queue.remove(selectedSender);
                } else {
                    step.put(receiver, "-");
                }
            }
    
            if (step.values().stream().allMatch(v -> v.equals("-"))) break;
            matrix.add(step);
        }
    
        // 🖨️ Affichage de la matrice
        System.out.println("\n======= 📋 Plan de réception sans conflits =======");
        System.out.print("Étape\t");
        for (int i = 0; i < receiverIPs.size(); i++) {
            System.out.print("Receiver " + (i + 1) + "\t");
        }
        System.out.println();
    
        int stepNumber = 0;
        for (Map<String, String> row : matrix) {
            System.out.print(stepNumber + "\t");
            for (String receiver : receiverIPs) {
                String senderIP = row.get(receiver);
                String label = senderIPs.contains(senderIP) ? "sender_" + (senderIPs.indexOf(senderIP) + 1) : senderIP;
                System.out.print((senderIP.equals("-") ? "-" : label) + "\t\t");
            }
            System.out.println();
            stepNumber++;
        }
        System.out.println("===============================================");

            // ✉️ Construction du plan par receiver
    Map<String, List<String>> receiverExecutionPlans = new HashMap<>();
    for (String receiver : receiverIPs) {
        receiverExecutionPlans.put(receiver, new ArrayList<>());
    }

    for (Map<String, String> step : matrix) {
        for (String receiver : receiverIPs) {
            String senderIP = step.get(receiver);
            receiverExecutionPlans.get(receiver).add(senderIP);
        }
    }

    // ✉️ Envoi à chaque receiver de son plan d’exécution
    for (String receiverIP : receiverIPs) {
        List<String> rawPlan = receiverExecutionPlans.get(receiverIP);
        List<String> formattedPlan = new ArrayList<>();
for (String senderIP : rawPlan) {
    if (senderIPs.contains(senderIP)) {
        formattedPlan.add(senderIP);  // ✅ envoyer l'@IP directement
    } else {
        formattedPlan.add("-");
    }
}

        try (Socket socket = new Socket(receiverIP, CONTROL_PORT);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {
            out.writeObject("receiver_exec");
            out.writeObject(formattedPlan);
        } catch (IOException e) {
            System.err.println("❌ Erreur envoi du plan exec à " + receiverIP + " : " + e.getMessage());
        }
    }

    }
    
 
    
    static class Request {
        String receiverIP;
        int blocIndex;
    
        Request(String receiverIP, int blocIndex) {
            this.receiverIP = receiverIP;
            this.blocIndex = blocIndex;
        }
    }
    
    public static void startRequestListener() {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(REQUEST_PORT)) {
                System.out.println("📥 [Sender] Écoute des demandes (port " + REQUEST_PORT + ")...");
                while (true) {
                    try {
                        Socket socket = serverSocket.accept();
                        ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
 
                        String receiverIP = (String) in.readObject();
                        int blocIndex = (int) in.readObject();
 
                        requestQueue.add(new Request(receiverIP, blocIndex));
                        System.out.println("📬 Requête reçue de " + receiverIP + " pour bloc " + blocIndex);
 
                    } catch (Exception e) {
                        System.err.println("❌ Erreur traitement demande : " + e.getMessage());
                    }
                }
            } catch (IOException e) {
                System.err.println("❌ Erreur serveur de requêtes : " + e.getMessage());
            }
        }).start();
 
               ExecutorService senderPool = Executors.newFixedThreadPool(S_MULTITHREAD);

        new Thread(() -> {
            while (true) {
                Request req = requestQueue.poll();
                if (req != null) {
                    senderPool.submit(() -> sendBlock(req));
                } else {
                    try {
                        Thread.sleep(10); // évite de saturer le CPU
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }).start();

    }
    private static void sendBlock(Request req) {
        String receiverSuffix = req.receiverIP.endsWith(".126") ? "2" : "1";
        String filename = BASE_DIR + "sender_to_r" + receiverSuffix + ".dat";
 
        try (RandomAccessFile file = new RandomAccessFile(filename, "r");
             Socket dataSocket = new Socket(req.receiverIP, DATA_PORT);
             OutputStream out = dataSocket.getOutputStream()) {
 
            byte[] buffer = new byte[BLOCK_SIZE];
            long offset = (req.blocIndex - 1L) * BLOCK_SIZE;
 
            file.seek(offset);
            int read = file.read(buffer);
            if (read > 0) {
                out.write(buffer, 0, read);
                out.flush();
                System.out.println("📤 Bloc " + req.blocIndex + " envoyé à " + req.receiverIP + " (" + read + " octets)");
            }
 
        } catch (IOException e) {
            System.err.println("❌ Erreur envoi bloc à " + req.receiverIP + " : " + e.getMessage());
        }
    }

    public static void startDataReceiverServer(int totalExpected) {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(DATA_PORT)) {
                System.out.println("📡 [Receiver] En écoute pour recevoir les blocs sur le port " + DATA_PORT + "...");
                int blocCounter = 1;

                while (true) {
                    try (Socket socket = serverSocket.accept();
                         InputStream in = socket.getInputStream()) {
 
                        byte[] buffer = new byte[BLOCK_SIZE];
                        int totalRead = 0;
                        while (totalRead < BLOCK_SIZE) {
                            int read = in.read(buffer, totalRead, BLOCK_SIZE - totalRead);
                            if (read == -1) break;
                            totalRead += read;
                        }
                        if (totalRead > 0) {
                            String filename = BASE_DIR + "bloc_" + blocCounter + ".data";
                            try (FileOutputStream fos = new FileOutputStream(filename)) {
                                fos.write(buffer, 0, totalRead);
                            }
                            System.out.println("📥 Bloc " + blocCounter + " reçu de " + socket.getInetAddress().getHostAddress() +
                                    " (" + totalRead + " octets) ");
                            ackQueue.offer("ACK");
                            blocCounter++;
                            if (blocCounter > totalExpected) {
                                sendFinishToMaster();
                                break;
                            }
                        }
                       
 
                    } catch (IOException e) {
                        System.err.println("❌ Erreur réception bloc : " + e.getMessage());
                    }
                }
 
            } catch (IOException e) {
                System.err.println("❌ Impossible de démarrer le serveur de réception de blocs : " + e.getMessage());
            }
        }).start();
    }

    private static void sendRequestsFromExecutionPlan(List<String> executionPlan) {
    String myIP = getMyIPFromWorkers();
    if (myIP.isEmpty()) return;

    sendStartToMaster();

    int blocIndex = 1;
    for (int i = 0; i < executionPlan.size(); i++) {
        String senderIP = executionPlan.get(i);

        if (senderIP.equals("-")) {
            System.out.println("⏳ Étape " + i + " : aucun bloc à recevoir (attente)");
            try { Thread.sleep(0); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            continue;
        }

        boolean success = false;
        while (!success) {
            try (Socket socket = new Socket(senderIP, REQUEST_PORT);
                 ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {

                out.writeObject(myIP);
                out.writeObject(blocIndex);
                System.out.println("📨 Demande envoyée à " + senderIP + " pour bloc " + blocIndex);

                String ack = ackQueue.poll(60, TimeUnit.SECONDS);
                if (ack != null) success = true;
                else {
                    System.err.println("⏱️ Timeout bloc " + blocIndex + ", retry...");
                    Thread.sleep(1000);
                }

            } catch (IOException | InterruptedException e) {
                System.err.println("❌ Erreur demande bloc " + blocIndex + " à " + senderIP + ": " + e.getMessage());
                try { Thread.sleep(1000); } catch (InterruptedException ex) { Thread.currentThread().interrupt(); }
            }
        }
        blocIndex++;
    }
}


private static void sendFinishToMaster() {
    try (Socket socket = new Socket("192.168.165.27", FIN_PORT);  // IP du master
         ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {

        out.writeObject("FIN");
        System.out.println("📤 Signal de fin envoyé au master");

    } catch (IOException e) {
        System.err.println("❌ Erreur envoi signal FIN : " + e.getMessage());
    }
}

private static void sendStartToMaster() {
    try (Socket socket = new Socket("192.168.165.27", 9910);
         ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {

        out.writeObject("DEBUT");
        System.out.println("🚀 Signal de début envoyé au master");

    } catch (IOException e) {
        System.err.println("❌ Erreur envoi signal DEBUT : " + e.getMessage());
    }
}
private static String getMyIPFromWorkers() {
    String ipFromEnv = System.getenv("MY_IP");
    if (ipFromEnv != null && !ipFromEnv.isEmpty()) {
        return ipFromEnv;
    }

    System.err.println("❌ Aucune IP définie. Veuillez définir la variable MY_IP.");
    return "";
}

public static void listenForFinishSignals() {
 
    try (ServerSocket serverSocket = new ServerSocket(FIN_PORT)) {
        int receivedFinish = 0;
        boolean startLogged = false;
        while (receivedFinish < R) {
            try (Socket socket = serverSocket.accept();
                 ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
       
                String msg = (String) in.readObject();
       
                if (msg.equals("DEBUT") && !startLogged) {
                    startTime = System.currentTimeMillis();
                    startLogged = true;
                    System.out.println("📡 [Master] En attente des signaux de fin sur le port " + FIN_PORT + "...");
                } else if (msg.equals("FIN")) {
                    receivedFinish++;
                    System.out.println("✅ Signal de fin reçu (" + receivedFinish + "/" + R + ")");
                }
       
            } catch (Exception e) {
                System.err.println("❌ Erreur réception message : " + e.getMessage());
            }
        }
        endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        System.out.println("⏱️ Transfert terminé en " + (duration / 1000.0) + " secondes");

    } catch (IOException e) {
        System.err.println("❌ Erreur serveur FIN : " + e.getMessage());
    }
}

}
