package edu.brown.hstore.replication;

import java.util.List;
import java.io.IOException; 
import java.util.LinkedList;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;


/**
 * @author Nelson Onyibe
 * @version 1.0.1
 * 
 */

public class Group<E extends Replicatable>{// implements HeartBeat {
    
    private static final long serialVersionUID = 5196346009800374283L;
    private ZooKeeper gZookeeper;
    //private HeartBeater<Group<E>> gHeartBeat;
    /**
     * the port that zookeeper runs on
     */
    private final static int PORT=2181;  
    /**
     * this group gRoot node
     */
    private String gRoot;
    /**
     * This group's leader gRoot node
     */
    private String gLRoot;
    /**
     * checks to see if the leader is alive
     * @return boolean
     */
    
    public boolean leaderIsAlive()
    {
        try {
           
            return this.gZookeeper.getChildren(this.gLRoot, true).size() >0;
        } catch (KeeperException e) { 
            e.printStackTrace();
        } catch (InterruptedException e) { 
            e.printStackTrace();
        }
        return false;
    }
    /**
     * First come first serve election style: When the leader is down, the first node to notice and whos' request to become the new leader gets here first becomes the leader
     * @param e
     * @return boolean {if this node succeeds in been set as leader returns true otherwise false}
     */
    public boolean setLeader(E e)
    {
        try
        {
            if(leaderIsAlive())
                return false;//There is a leader that is still alive
            byte[] b= new byte[1];
            b[0]=(byte)e.getId();
            gZookeeper.create(gLRoot+ "/"+e.getId(), b, Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
            this.getChildren(gLRoot); 
            e.setIsPrimary(true);
            return true;
        }
        catch(Exception ex)
        { ex.printStackTrace();}
        return false;     
    }
    /**
     * 
     * @return the id of the leader
     * @throws Exception
     */
    public int getLeader() throws Exception
    {
       boolean isAlive=this.leaderIsAlive(); 
       if(isAlive)
       {   String path=this.gZookeeper.getChildren(this.gLRoot, true).get(0);
           return (int)this.gZookeeper.getData(gLRoot+"/"+path, true, null)[0];
       }
       throw new Exception("No elected leader");
     }
    /**
     * 
     * constructor
     */
    public Group(E node)
    {
        gRoot="/"+node.getSetId();
        gLRoot=gRoot+"_leader";
        try {
            this.gZookeeper= new ZooKeeper("localhost",PORT,node); 
            Stat ss = gZookeeper.exists(gRoot, false);
            Stat ls = gZookeeper.exists(gLRoot, false);
            if(ss ==null) //Node !exists  
              gRoot=gZookeeper.create(gRoot, new byte[1], Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT); 
            if(ls==null) //Node !exists  
                gLRoot=gZookeeper.create(gLRoot, new byte[1], Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            gZookeeper.getChildren(gRoot, true);
            gZookeeper.getChildren(gLRoot, true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
    }
    private List<Integer> getChildren(String root)
    {
        LinkedList<Integer> ll = new LinkedList<Integer>();
        try
        {
            List<String> ch = this.gZookeeper.getChildren(root, true); 
            for(String s: ch)
            {
                byte[] by= this.gZookeeper.getData(root+"/"+s, true, null);
                ll.add((int)by[0]);
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
         return ll;
    }
    /**
     * 
     * @return the ids of members of this group
     */
    public List<Integer> getChildren()
    {
        return this.getChildren(gRoot);
    }
    /**
     * 
     */
    public String toString()
    {
        Integer leader=Integer.MIN_VALUE; 
        try {
            leader=getLeader();
        } catch (Exception e) { 
            e.printStackTrace();
        }
        String m="";
        List<Integer> ch=this.getChildren();
        for(Integer ll: ch)
            m +=" " +ll+"  , ";
        return "{ \n leader:"+leader+ "  \nzookeeper: \n"+this.gZookeeper+"; PORT: "+PORT+" \ngroupName: "+this.gRoot+ " \nmembers: "+m+" \n#of gMembers:  "+ch.size()+ "}";
    }
    /**
     * 
     * @param e
     * @return
     */
    public String join(E e)
    {
        /*
        if(gHeartBeat==null)
            gHeartBeat = new HeartBeater<Group<E>>(this);
        */
        try {
            
            byte[] b=new byte[1];
            b[0]=((byte)e.getId());
            String child=gZookeeper.create(gRoot+ "/" + e.getId(), b, Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
            return child;
            
        } catch (KeeperException e1) { 
            e1.printStackTrace();
        } catch (InterruptedException e1) { 
            e1.printStackTrace();
        }
        return null;
    }
   // @Override
    public void sendBeat() {
        try {
            
            this.gZookeeper.getChildren(gRoot, true);
            this.gZookeeper.getChildren(gLRoot, true);
            System.out.println(" Beat...  "+this.getInterval());
        } catch (KeeperException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        
    }
    //@Override
    public int getInterval() {
        return Math.abs(this.gZookeeper.getSessionTimeout()-1);
    }
    
}
