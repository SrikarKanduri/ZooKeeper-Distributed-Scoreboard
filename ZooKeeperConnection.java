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
import org.apache.zookeeper.data.Stat;

public class ZooKeeperConnection {
    // create static instance for zookeeper class.
    private static ZooKeeper zk;
    private static final CountDownLatch connectedSignal = new CountDownLatch(1);
    
    private static String path;
    
    // Method to connect zookeeper ensemble.
    public static ZooKeeper connect(String host) throws IOException,InterruptedException {
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
    public static void close() throws InterruptedException {
        zk.close();
    }
    
    // Method to create znode in zookeeper ensemble
    public static void create(String path, byte[] data) throws
    KeeperException,InterruptedException {
        zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                  CreateMode.PERSISTENT);
    }
    
    // Method to create znode in zookeeper ensemble
    public static void createEphemeral(String path, byte[] data) throws
    KeeperException,InterruptedException {
        zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                  CreateMode.EPHEMERAL);
    }
    
    // Method to check existence of znode and its status, if znode is available.
    public static Stat znode_exists(String path) throws
    KeeperException,InterruptedException {
        return zk.exists(path, true);
    }
    
    // Method to update the data in a znode. Similar to getData but without watcher.
    public static void update(String path, byte[] data) throws
    KeeperException,InterruptedException {
        zk.setData(path, data, zk.exists(path,true).getVersion());
    }
    
    public static void automateTests(String cnt, String dly, String score) throws
    KeeperException,InterruptedException {
        int count = Integer.parseInt(cnt);
        long waitTime = Long.parseLong(dly);
        
        while(count-- > 0) {
            Thread.sleep(waitTime);
            create(path + "/" + new Date(), score.getBytes());
        }
    }
    
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        
        boolean testing = false;
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
        
        // znode path
        // Check if znode already exists
        byte[] data = score.getBytes();
        
        try {
            zk = connect(hostPort);
            
            path = "/PlayerList";
            Stat stat = znode_exists(path); // Stat checks the path of the znode
            if(stat == null) { // Node doesn't exist
                create(path, "".getBytes());
            }
            
            path += "/" + name;
            stat = znode_exists(path);
            if(stat == null) { // Node doesn't exist
                create(path, "".getBytes());
            }
            
            createEphemeral(path + "/online", "".getBytes());
            
            while(!testing) {
                System.out.println("Enter score: ");
                score = sc.next();
                if(score < 0) {
                    System.out.println("Invalid score. Exiting!");
                    break;
                }
                create(path + "/" + System.currentTimeMillis(), score.getBytes());
            }
            
            if(testing) automateTests(count, waitTime, score);
            
            close();
        } catch (Exception e) {
            System.out.println(e.getMessage()); //Catch error message
        }
    }
}
