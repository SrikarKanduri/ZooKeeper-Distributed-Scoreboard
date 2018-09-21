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

public class Dashboard {
    // create static instance for zookeeper class.
    private static ZooKeeper zk;
    private static final CountDownLatch connectedSignal = new CountDownLatch(1);
    
    // Variable to store z-node hierarchy
    private static String path = "/PlayerList";
    
    private static int maxSize;
    private static List<PlayerScore> topScores;
    private static List<PlayerScore> recentScores;
    private static Set<String> pendingWatchers;
    private static Set<String> oldPlayers;
    
    Dashboard() {
//         isRootWatched = false;
        topScores = new ArrayList<PlayerScore>();
        recentScores = new ArrayList<PlayerScore>();
        pendingWatchers = new HashSet<String>();
        oldPlayers = new HashSet<String>();
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
    
    // Method to check existence of a z-node
    public static boolean isNodeExists(String path) throws
    KeeperException,InterruptedException {
        return (zk.exists(path, true) != null);
    }
    
    // Returns new player that joined
    public static String getNewPlayer() throws
    InterruptedException,KeeperException{
        List <String> playerNames = zk.getChildren(path, false);
        for (String playerName : playerNames) {
            if (!oldPlayers.contains(playerName)) return playerName;
        }
        return "";
    }
    
    // Set watchers for expired/new nodes
    public synchronized static void setChainedWatcher() throws
    InterruptedException,KeeperException {
        if(pendingWatchers.size() > 0) {
            for(String name: pendingWatchers)
                setChildWatcher(name, false);
            pendingWatchers.clear();
        }
    }
    
    // Set watcher for a z-node
    public static void setChildWatcher(String root, boolean isRoot) throws
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
                        getSnapshot();
                        connectedSignal.countDown();
                        if(!isRoot)
                            pendingWatchers.add(we.getPath());
                        else {
                            setChildWatcher(path, true);
//                             isRootWatched = false;
                            String newPlayer = path + "/" + getNewPlayer();
                            pendingWatchers.add(newPlayer);
                            System.out.println("New Player Joined - " + newPlayer);
                        }
                        
                        setChainedWatcher();
                    } catch(Exception ex) {
                        System.out.println(ex.getMessage());
                    }
                }
            }
        }, null);
    }
    
    // Get current snapshot of data
    public static void getSnapshot() throws Exception {
        List<PlayerScore> allScores = new ArrayList<PlayerScore>();
        
        List <String> playerNames = zk.getChildren(path, false);
        for (String playerName : playerNames) { // Get all scores of all the players
            boolean playerOnline = isNodeExists(path + "/" + playerName + "/online");
            
            List<String> timeStamps = zk.getChildren(path + "/" + playerName, false);
            for (String timeStamp : timeStamps) {
                if (timeStamp.equals("online")) continue;
                
                String scoreAtTime = new String(zk.getData(path + "/" + playerName + "/" + timeStamp, false, null), "UTF-8");
                allScores.add(new PlayerScore(playerOnline, playerName, Long.parseLong(timeStamp), Integer.parseInt(scoreAtTime)));
            }
        }
        
        int scoresSize = allScores.size();
        
        // Get maxSize recent scores (sort all the players on time stamp)
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
        
        // Get maxSize top scores (sort all the players on score)
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
        
        printDashboard();
    }
    
    // Method to print dashboard
    public static void printDashboard() {
        System.out.println("Most recent scores");
        System.out.println("------------------");
        for (PlayerScore recentScore : recentScores) {
            System.out.println(recentScore.name + "\t" + recentScore.score + " " + (recentScore.online ? "**" : ""));
        }
        System.out.println();
        System.out.println("Highest scores");
        System.out.println("--------------");
        for (PlayerScore topScore : topScores) {
            System.out.println(topScore.name + "\t" + topScore.score + " " + (topScore.online ? "**" : ""));
        }
        System.out.println();
    }
    
    public static void main(String[] args) {
        String hostPort = args[0];
        maxSize = Integer.parseInt(args[1]);
        
        Dashboard dp = new Dashboard();
        
        try {
            zk = connect(hostPort);
            
            if(isNodeExists(path)) { // PlayerList node exists
                List <String> playerNames = zk.getChildren(path, false);
                setChildWatcher(path, true);
                
                // Set child-watchers for all the player nodes
                for (String playerName : playerNames) {
                    setChildWatcher(path + "/" + playerName, false);
                    oldPlayers.add(playerName);
                }
                
                // Print dashboard once
                getSnapshot();
                
                connectedSignal.await();
            } else { // PlayerList node doesn't exist
                System.out.println("No scores present. Exiting!");
                return;
            }
            
            Scanner sc = new Scanner(System.in);
            sc.next();
            close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
