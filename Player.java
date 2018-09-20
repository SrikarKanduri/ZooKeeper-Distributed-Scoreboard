import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

public class Player {
    // create static instance for zookeeper class.
    private static ZooKeeper zk;
    private static final CountDownLatch connectedSignal = new CountDownLatch(1);
    
    // Variable to store z-node hierarchy
    private static String path;
    
    // Method to connect to zookeeper ensemble.
    private static ZooKeeper connect(String host) throws IOException,InterruptedException {
        zk = new ZooKeeper(host,5000,new Watcher() {
            public void process(WatchedEvent we) {
                
                if (we.getState() == KeeperState.SyncConnected) {
                    connectedSignal.countDown();
                }
            }
        });
        
        connectedSignal.await();
        return zk;
    }
    
    // Method to disconnect from zookeeper server
    private static void close() throws InterruptedException {
        zk.close();
    }
    
    // Method to create a z-node in zookeeper ensemble
    private static void createNode(String path, byte[] data) throws
    KeeperException,InterruptedException {
        zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                  CreateMode.PERSISTENT);
    }
    
    // Method to create an ephemeral z-node in zookeeper ensemble
    private static void createEphemeralNode(String path, byte[] data) throws
    KeeperException,InterruptedException {
        zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                  CreateMode.EPHEMERAL);
    }
    
    // Method to check existence of a z-node
    private static boolean isNodeExists(String path) throws
    KeeperException,InterruptedException {
        return (zk.exists(path, true) != null);
    }
    
    // Method to generate random scores (and delays)
    private static void automateTests(String cnt, String dly, String scr) throws
    KeeperException,InterruptedException,NumberFormatException {
        int count = Integer.parseInt(cnt);
        long delay = Long.parseLong(dly);
        int score = Integer.parseInt(scr);
        
        double randomDelay, randomScore;
        int newDelay = 0, newScore;
        
        Random r = new Random();
        
        while(count-- > 0) {
            Thread.sleep(newDelay);
            
            do {
                randomDelay = r.nextGaussian() * 1000/3 + delay; // 3 SDs for more probability of value +/- 1000
                newDelay = (int) Math.round(randomDelay);
            } while(newDelay <= 0); // Handling negative randoms
            
            do {
                randomScore = r.nextGaussian() * 1000/3 + score;
                newScore = (int) Math.round(randomScore);
            } while(newScore <= 0); // Handling negative randoms
            
            System.out.println("Posting score: " + newScore + " and waiting(ms) " + newDelay);
            createNode(path + "/" + System.currentTimeMillis(), (newScore + "").getBytes());
        }
    }
    
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        
        boolean testing = false; // To check for automated input
        String hostPort = args[0];
        String name = args[1];
        String count = "";
        String waitTime = "";
        String score = "0";
        
        if(args.length > 2) {
            testing = true;
            count = args[2];
            waitTime = args[3];
            score = args[4];
        }
        
        try {
            zk = connect(hostPort);
            
            path = "/PlayerList";
            if(!isNodeExists(path)) { // PlayerList node doesn't exist
                createNode(path, "".getBytes());
            }
            
            path += "/" + name;
            if(!isNodeExists(path)) { // Player node doesn't exist
                createNode(path, "".getBytes());
            }
            
            // Ephemeral node to indicate if the player is online
            createEphemeralNode(path + "/online", "".getBytes());
            
            while(!testing) {
                System.out.println("Enter score: ");
                score = sc.next();
                
                Integer.parseInt(score); // Check for valid scores; If invalid, throws exception
                
                createNode(path + "/" + System.currentTimeMillis(), score.getBytes());
            }
            
            if(testing) automateTests(count, waitTime, score);
            
            close();
        } catch (Exception e) {
            System.out.println(e.getMessage()); //Catch error message
        }
    }
}
