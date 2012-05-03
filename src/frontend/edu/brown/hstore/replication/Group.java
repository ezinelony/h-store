package edu.brown.hstore.replication;



import java.util.List;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;



import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;




public class Group<E extends Replicatable>{
    
    private String name;
    private ZooKeeper zookeeper;
    private ZooKeeper leader;
    private static int port=6000;  
    private String root;
    private String lRoot; //The root for leader
    
    /*
     * There can only be one leader
     */
    
    public boolean leaderIsAlive()
    {
        try {
            return this.zookeeper.getChildren(this.lRoot, true).size() >0;
        } catch (KeeperException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return false;
    }
    public boolean setLeader(E e)
    {
        try
        {
        List<String> children= this.zookeeper.getChildren(this.lRoot, true);
        if(children.size() !=0)
            return false;
        byte[] b= new byte[1];
        b[0]=(byte)e.getId();
        String c=zookeeper.create(lRoot+ "/"+e.getId(), b, Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
        return true;
        }
        catch(Exception ex)
        {
            ex.printStackTrace();
        }
        return false;
         
    }
    
    public int getLeader() throws Exception
    {
       if(this.leaderIsAlive())
       {   String path=this.zookeeper.getChildren(this.lRoot, true).get(0);
           return (int)this.zookeeper.getData(path, true, null)[0];
       }
       throw new Exception("No elected leader");
     }
    
    @SuppressWarnings("unchecked")
    public Group(E node)
    {
        name=""+node.getSetId(); 
        if(!name.startsWith("/"))
            name="/"+name;
        root=name;
        lRoot=root+"-leader";
        try {
            this.zookeeper= new ZooKeeper("localhost",port,node);
            this.leader= new ZooKeeper("localhost",port,node);
            Stat ss = zookeeper.exists(root, false);
            Stat ls = zookeeper.exists(root+"-leader", false);
            if(ss ==null) //Node !exists  
              root=zookeeper.create(root, new byte[1], Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT); 
            if(ls==null) //Node !exists  
                lRoot=zookeeper.create(lRoot, new byte[1], Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            zookeeper.getChildren(root, true);
            zookeeper.getChildren(lRoot, true);
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
    
    public LinkedList<Integer> getChildren()
    {
        LinkedList<Integer> ll = new LinkedList<Integer>();
        try
        {
            List<String> ch = this.zookeeper.getChildren(root, true);
            for(String s: ch)
            {
                byte[] by= this.zookeeper.getData(s, true, null);
                ll.add((int)by[0]);
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
         return ll;
    }
    public String join(E e)
    {
        try {
            
            byte[] b=new byte[1];
            b[0]=((byte)e.getId());
            String child=zookeeper.create(root+ "/" + e.getId(), b, Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
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
    
    
    
    
 
    public static void main(String[] args)
    {
        Group<Test> g1;
        Group<Test> g2;
        Group<Test> g3;
        InetAddress one = null;
        InetAddress two = null;
        InetAddress three = null;
        boolean reachable=false;
        boolean local=false;
        boolean r2=false;
        int setId1=10;
        try {
            one =InetAddress.getByName("web.mit.edu");
            two =InetAddress.getByName("cs.brown.edu");
            three =InetAddress.getByName("cnn.com");
            Test test1= new Test(1,setId1,one);
            g1= new Group<Test>(test1);
            Test test2= new Test(2,setId1,two);
            g2= new Group<Test>(test2);
            Test test3= new Test(3,setId1,three);
            g3= new Group<Test>(test3);
            //test1.joinGroup(g1.getZ());
            //test2.joinGroup(g2.getZ());
            //test3.joinGroup(g3.getZ());
            g1.join(test1);
            g2.join(test2);
            g3.join(test3);
            g2.setLeader(test2);
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        boolean reacheable;
        try {
            
            
            reacheable = one.isReachable(0);
              local=one.isMCGlobal();
            r2 = one.isReachable(0);
            System.out.println(r2);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        String name=one.getHostAddress();
        System.out.println(reachable);
       
        System.out.println(name);
        System.out.println(local);
        /*
        Group g4= new Group("/----");
        Group.Test nelson=g4.new Test("Hello Nelson");
        Group<Group.Test> g= new Group<Group.Test>("/group1");
        
        Group.Test str= g4.new Test("#1");
        Group.Test str2= g4.new Test("#2");
        g.join(str);
        g.join(str2);
        String nK=g.join(nelson);
        System.out.println(g.delete(nK));
        g.getChildren();
        String nK2=g.join(g4.new Test("Nelly"));
        String nK3=g.join(g4.new Test("Nelly3"));
        System.out.println(nK3);
        */
        
        
        
    }
}
