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

class PlayerScore {
    boolean online;
    String name;
    long timeStamp;
    int score;
    
    PlayerScore(boolean online, String name, long timeStamp, int score) {
        this.online = online;
        this.name = name;
        this.timeStamp = timeStamp;
        this.score = score;
    }
}

public class DisplayScore {
    // create static instance for zookeeper class.
    private static ZooKeeper zk;
    private static final CountDownLatch connectedSignal = new CountDownLatch(1);
    
    // znode path
    private static String path = "/PlayerList";
    
    private static int maxSize;
    private static boolean isRootWatched;
    private static List<PlayerScore> topScores;
    private static List<PlayerScore> recentScores;
    private static Set<String> watcherSet;
//    private static Set<String> oldPlayers;
    
    DisplayScore(int maxSize) {
        topScores = new ArrayList<PlayerScore>();
        recentScores = new ArrayList<PlayerScore>();
        watcherSet = new HashSet<String>();
//        isRootWatched = false;
    }
    
    // Method to connect zookeeper ensemble.
    public static ZooKeeper connect(String host) throws IOException,InterruptedException {
        zk = new ZooKeeper(host, 5000, new Watcher() {
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
    
    // Method to check existence of znode and its status, if znode is available.
    public static boolean znode_exists(String path) throws
    KeeperException,InterruptedException {
        return (zk.exists(path, true) != null);
    }
    
    public static void print() {
        System.out.println("Most recent scores");
        System.out.println("------------------");
        for(int i = 0; i < recentScores.size(); i++) {
            System.out.println(recentScores.get(i).name + "\t" + recentScores.get(i).score + " " + (recentScores.get(i).online ? "**" : ""));
        }
        System.out.println();
        System.out.println("Highest scores");
        System.out.println("--------------");
        for(int i = 0; i < topScores.size(); i++) {
            System.out.println(topScores.get(i).name + "\t" + topScores.get(i).score + " " + (topScores.get(i).online ? "**" : ""));
        }
    }
    
//    public static String getNewPlayer() throws
//    InterruptedException,KeeperException{
//        List <String> playerNames = zk.getChildren(path, false);
//        for(int i = 0; i < playerNames.size(); i++) {
//            String playerName = playerNames.get(i);
//            if(!oldPlayers.contains(playerName)) return playerName;
//        }
//        return "";
//    }
    
    public static void chainedWatcher(String root, boolean isRoot) throws
    InterruptedException,KeeperException {
        zk.getChildren(root, new Watcher() {
            public void process(WatchedEvent we) {
                if (we.getType() == Event.EventType.None) {
                    switch(we.getState()) {
                        case Expired:
                            connectedSignal.countDown();
                            break;
                    }
                } else {
                    try {
                        getZnodeData();
                        connectedSignal.countDown();
//                        if(!isRoot)
                            watcherSet.add(we.getPath());
//                        else {
////                            isRootWatched = false;
////                            watcherSet.add(getNewPlayer());
//                        }
                    } catch(Exception ex) {
                        System.out.println(ex.getMessage());
                    }
                }
            }
        }, null);
    }
    
    public static void getZnodeData() throws Exception {
        List<PlayerScore> allScores = new ArrayList<PlayerScore>();
        
        List <String> playerNames = zk.getChildren(path, false);
        for(int i = 0; i < playerNames.size(); i++) {
            String playerName = playerNames.get(i);
            boolean playerOnline = znode_exists(path + "/" + playerName + "/online");
            
            List<String> timeStamps = zk.getChildren(path + "/" + playerName, false);
            for(int j = 0; j < timeStamps.size(); j++) {
                if(timeStamps.get(j).equals("online")) continue;
                
                String scoreAtTime = new String(zk.getData(path + "/" + playerName + "/" + timeStamps.get(j), false, null), "UTF-8");
                allScores.add(new PlayerScore(playerOnline, playerName, Long.parseLong(timeStamps.get(j)), Integer.parseInt(scoreAtTime)));
            }
        }
        
        int scoresSize = allScores.size();
        
        List<PlayerScore> allRecentScores = new ArrayList<>(allScores);
        Collections.sort(allRecentScores, new Comparator<PlayerScore>(){
            public int compare(PlayerScore p1, PlayerScore p2) {
                return (int) (p2.timeStamp - p1.timeStamp);
            }
        });

        if(scoresSize > maxSize)
            recentScores = allRecentScores.subList(0, maxSize);
        else
            recentScores = new ArrayList<>(allRecentScores);

        List<PlayerScore> allTopScores = new ArrayList<>(allScores);
        Collections.sort(allTopScores, new Comparator<PlayerScore>(){
            public int compare(PlayerScore p1, PlayerScore p2) {
                return p2.score - p1.score;
            }
        });

        if(scoresSize > maxSize)
            topScores = allTopScores.subList(0, maxSize);
        else
            topScores = new ArrayList<>(allTopScores);
        
        print();
    }
    
    public static void main(String[] args) {
        String hostPort = args[0];
        maxSize = Integer.parseInt(args[1]);
        
        DisplayScore dp = new DisplayScore(maxSize);
        
        try {
            zk = connect(hostPort);
            if(znode_exists(path)) {
                List <String> playerNames = zk.getChildren(path, false);
//                chainedWatcher(path, true);
//                isRootWatched = true;
                for(int i = 0; i < playerNames.size(); i++) {
                    String playerName = playerNames.get(i);
                    chainedWatcher(path + "/" + playerName, false);
//                    oldPlayers.add(playerName);
                }
                
                getZnodeData();
                connectedSignal.await();
            }
            
            while(true) {
//                if(!isRootWatched) {
////                    chainedWatcher(path, true);
//                    isRootWatched = true;
//                }
                
                if(watcherSet.size() > 0) {
                    for(String name: watcherSet)
                        chainedWatcher(name, false);
                    watcherSet.clear();
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage()); //Catch error message
        }
//        close();
    }
}
