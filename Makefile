all: player.class watcher.class

player.class: Player.java
	javac -cp zookeeper-3.4.12.jar Player.java

watcher.class: Dashboard.java
	javac -cp zookeeper-3.4.12.jar Dashboard.java
