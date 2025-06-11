package network.engine.core;
 
import java.io.*;
 
import java.net.ServerSocket;
import java.net.Socket;
import network.engine.broadcast.TreeBroadcast;
import network.engine.broadcast.Cornet;
import network.engine.shuffle.ShufflePrioritaire;
import network.engine.shuffle.ShuffleEquitable;
import network.engine.shuffle.ShuffleMatriciel;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import network.engine.object.Standard;
import java.time.LocalDate;
import network.engine.object.KryoUtils;
public class Master {

    private static final int NB_WORKERS = 10;
    private static final int CHILDREN_PER_NODE = 7;
    private static final String MASTER_IP = "192.168.165.27";
    private static final String FILE_TO_SEND = "/home/ubuntu/mounted_vol/people_kryo.bin";
            
        
    public static void main(String[] args) throws IOException {

        /********************* TREE BROADCAST ***************************/
         TreeBroadcast treeBroadcast = new TreeBroadcast();
        String outputFile = "/home/ubuntu/mounted_vol/people_kryo.bin";
        //Standard.serializeStandard(outputFile);
        KryoUtils.serializeWithKryo(outputFile);
        treeBroadcast.loadWorkerIPs("/home/ubuntu/pqdag/workers");
        treeBroadcast.startServer(NB_WORKERS);
        treeBroadcast.buildTree(MASTER_IP, CHILDREN_PER_NODE);
        treeBroadcast.sendChildrenInfo(MASTER_IP);

        long start = treeBroadcast.startTimer();
        treeBroadcast.sendFile(MASTER_IP, FILE_TO_SEND);
        treeBroadcast.waitForCompletion(NB_WORKERS);
        treeBroadcast.displayDuration(start);
        



        /********************* CORNET ***************************/
       /*Cornet cornet = new Cornet();
        cornet.runAsMaster();

        /********************* SHUFFLE ***************************/
        //long startTime = System.currentTimeMillis();

         /*****************Shuffle Equitable ************************/
        //new Thread(() -> ShuffleEquitable.listenForFinishSignals()).start();
        //ShuffleEquitable.loadWorkerIPs();
        //ShuffleEquitable.sendRolesAndPlans();
         
         
         /*****************Shuffle Prioritaire ************************/
        //new Thread(() -> ShufflePrioritaire.listenForFinishSignals()).start();
        //ShufflePrioritaire.loadWorkerIPs();
        //ShufflePrioritaire.sendRolesAndPlans();


        /*****************Shuffle Optimized ************************/
       // new Thread(() -> ShuffleMatriciel.listenForFinishSignals()).start();
       //ShuffleMatriciel.loadWorkerIPs();
        //ShuffleMatriciel.sendRolesAndPlans();

        
    }
 
    
}
 
