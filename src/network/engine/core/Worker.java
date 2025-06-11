
 
package network.engine.core;

import network.engine.broadcast.TreeBroadcast;
import network.engine.broadcast.Cornet;
import network.engine.shuffle.ShufflePrioritaire;
import network.engine.shuffle.ShuffleEquitable;
import network.engine.shuffle.ShuffleMatriciel;
public class Worker {
    public static void main(String[] args) {
    /******************TreeBroadcast**********************/
        TreeBroadcast worker = new TreeBroadcast();
        worker.configurePort(args);           // 🔧 Lecture du port depuis les args
        new Thread(worker::startServer).start();  // 🌍 Démarre serveur en parallèle
        worker.connectToMaster();             // 🔗 Connexion au master pour récupérer les enfants
        
    /******************Cornet**********************/

        /*if (args.length != 1) {
            System.err.println("Usage: java Worker <ID>");
            return;
        }

        int id = Integer.parseInt(args[0]);
        Cornet cornet = new Cornet();
        cornet.runAsWorker(id);
        

    /**************** ShuffleEquitable *******************/
    //ShuffleEquitable.listenForControl();


    /**************** ShufflePrioritaire *******************/
    //ShufflePrioritaire.listenForControl();

    /**************** ShuffleOptimized *******************/
      //ShuffleOptimized.listenForControl();
    }
}




