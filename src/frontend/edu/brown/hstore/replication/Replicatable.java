package edu.brown.hstore.replication;

import java.net.InetAddress;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public interface Replicatable extends Watcher {
    
    
    public Object execute(Object originator,Object task);
    public Object receiveResponse(Object responder,Object result);
    public boolean isPrimary();
    public boolean setAsPrimary();
    public int getId();
    public InetAddress getInetAddress();
    public void process(WatchedEvent event);
    public int getSetId();

}
