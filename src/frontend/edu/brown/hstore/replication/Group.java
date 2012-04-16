package edu.brown.hstore.replication;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class Group<E extends InetAddress> implements Watcher{
    
    private String name;
    private ZooKeeper zookeeper;
    private ZooKeeper leader;
    private static int port=2181; 
    private HashMap<String,E> members;
    private String root;
    private String lRoot;
    private static HashMap<String,Group> groups;
    
    @SuppressWarnings("unchecked")
    public Group(String name)
    {
        if (groups ==null)
            groups= new HashMap<String,Group>();
        members=new HashMap<String,E>();
        if(!name.startsWith("/"))
            name="/"+name;
        this.name= name;
        root=name;
        try {
            this.zookeeper= new ZooKeeper("localhost",port,this);
            this.leader= new ZooKeeper("localhost",port,this);
            Stat ss = zookeeper.exists(root, false);
            Stat ls = zookeeper.exists(root+"-leader", false);
            if(ss ==null) //Node exists 
              // populate instance variables 
              root=zookeeper.create(root, new byte[1], Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT); 
            else
            {
                this.members=(HashMap<String,E>)groups.get(root).members;
            }
            if(ls==null)
                lRoot=zookeeper.create(root+"-leader", new byte[1], Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            //otherwise populate instance variables
            groups.put(root, this);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (KeeperException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public String join(E e)
    {
        try {
            byte[] b=new byte[1];
            int ch=zookeeper.getChildren(root, true).size()+1;
            String child=zookeeper.create(root+ "/" + ch, b, Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
            //members.put(child, e);
            return child;
            
        } catch (KeeperException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        } catch (InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        return null;
    }
    
    @Override
    public void process(WatchedEvent event) {
        EventType eventType=EventType.fromInt(event.getType().getIntValue());
        if(eventType==EventType.NodeDeleted)
        {
            
        }
        else if(eventType==EventType.NodeCreated)
        {
            
        }
        else if(eventType==EventType.NodeChildrenChanged)
        {
            
        }
    }

}
