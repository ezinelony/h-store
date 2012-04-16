package edu.brown.hstore.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.data.ACL;

public class GZooKeeper<E> extends ZooKeeper {

    HashMap<String,E> members;
    public GZooKeeper(String connectString, int sessionTimeout, Watcher watcher) throws IOException {
        super(connectString, sessionTimeout, watcher);
        members = new HashMap<String,E>();
    }

   
    public GZooKeeper(String connectString, int sessionTimeout, Watcher watcher, long sessionId, byte[] sessionPasswd) throws IOException {
        super(connectString, sessionTimeout, watcher, sessionId, sessionPasswd);
        members = new HashMap<String,E>();
    }
    
    public String create(final String path, E data, List<ACL> acl,
            CreateMode createMode) throws KeeperException, InterruptedException
    {
        String str= super.create(path, new byte[1], acl, createMode);
        members.put(path, data);
        return str;
    }
    
    public void create(final String path, E data, List<ACL> acl,
            CreateMode createMode,  StringCallback cb, Object ctx)
    {
        super.create(path, new byte[1], acl, createMode, cb, ctx);
        members.put(path, data);
    }
    
    public void delete(final String path, int version)
    throws InterruptedException, KeeperException
    {
        super.delete(path, version);
        members.remove(path);
    }
    
    public List<E> getGChildren(String path, boolean watch)
    throws KeeperException, InterruptedException {
        List<String> ch= super.getChildren(path, watch);
        List<E> l = new ArrayList<E>();
        for(String s: ch)
        {
            if(members.containsKey(s))
                l.add(members.get(s));
            else
                l.remove(s);
        }
        return l;
    }
    
    public void delete(final String path, int version, VoidCallback cb,
            Object ctx)
    {
        super.delete(path, version, cb, ctx);
        members.remove(path);
    }

}
