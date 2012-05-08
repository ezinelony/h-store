package edu.brown.hstore.replication;

import java.io.PrintStream;
import java.net.InetAddress;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

class Test implements Replicatable
{

    public void process(WatchedEvent event) {
        EventType eventType=EventType.fromInt(event.getType().getIntValue());
        if(eventType==EventType.NodeDeleted)
        {
            out.println(" My Id is :"+this.getId()+" And I have been notified of node deletion ");
        }
        else if(eventType==EventType.NodeCreated)
        {
            out.println(" My Id is :"+this.getId()+" And I have been notified of node creation ");

        }
        else if(eventType==EventType.NodeChildrenChanged)
        {
            out.println(" My Id is :"+this.getId()+" And I have been notified of number of children changing");
        }
       out.println(" My Id is :"+this.getId()+"   ----  "+event+" type --"+eventType);
    }

    
    String s;
    int site_id;
    int id;
    InetAddress addr;
    PrintStream out;
    public Test(int id, int  s,InetAddress ia)
    {
        this.site_id=s;
        this.id=id;
        addr=ia;
        out=System.out;
    }
     
    public void joinGroup(ZooKeeper z)
    {
try {
            z.getChildren("/"+site_id, true);
            byte[] b=new byte[1];
            b[0]=((byte)id);
            String child=z.create("/"+site_id+ "/" + id, b, Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
           // System.out.println(child+"  JUST JOINED------ ALREADY PRESENT CHILDREN---"+zookeeper.getData(child, true, null)[0]);
            
            //return child;
            
        } catch (KeeperException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        } catch (InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

    }
    public String toString()
    {
        return s;
    }
    
    public void process(String eventType) {
        // TODO Auto-generated method stub
        System.out.println(eventType);
    }

    @Override
    public Object execute(Object originator, Object task) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object receiveResponse(Object responder, Object result) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean getIsPrimary() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void setIsPrimary(boolean p) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public int getId() {
        // TODO Auto-generated method stub
        return id;
    }

    @Override
    public InetAddress getInetAddress() {
        // TODO Auto-generated method stub
        return addr;
    }

    @Override
    public int getSetId() {
        // TODO Auto-generated method stub
        return site_id;
    }
    
}