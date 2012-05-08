/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

/* WARNING: THIS FILE IS AUTO-GENERATED
            DO NOT MODIFY THIS SOURCE
            ALL CHANGES MUST BE MADE IN THE CATALOG GENERATOR */

package org.voltdb.catalog;

import java.net.InetAddress;
import java.util.LinkedList;
import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.jfree.util.Log;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.replication.Group;
import edu.brown.hstore.replication.Replicatable;

/**
 * A physical execution context for the system
 */
public class Site extends CatalogType implements Replicatable {

    /**
     * Replication:
     * Every site has a replica set Id which groups sites together to form a replica set
     */
    int m_replicaSiteId=Integer.MIN_VALUE;
    int m_leaderId=Integer.MIN_VALUE;
    int m_id;
    CatalogMap<Partition> m_partitions;
    boolean m_isUp;
    int m_messenger_port;
    int m_proc_port;
    boolean m_isPrimary;
    private List<Integer> m_replicaSet;
    /**
     * Group
     */
    private Group<Site> m_group;

    public void setIsPrimary(boolean p)
    {
        m_fields.put("isPrimary", p);
        m_isPrimary=p;
    }
    
    /**Every set has a primary/leader
     * (non-Javadoc)
     * @see edu.brown.hstore.replication.Replicatable#isPrimary()
     */
    public boolean getIsPrimary()
    {
        return m_isPrimary;
    }
    /*
     * Set up replica set group
     */
    public void setUpGroup()
    {
        System.out.println("Is PRIMARY:::::::"+this.getIsPrimary());
        if(m_replicaSiteId==Integer.MIN_VALUE)
            return;
        try
        {
            m_group= new Group<Site>(this); 
            m_group.join(this);      
            //this.getReplicaSiteId()==this.getId(): This is a hack for now before I figure out how to probably set the primary bit
            if(this.getIsPrimary()|| this.getReplicaSiteId()==this.getId())
            {
                this.setIsPrimary(true);
                boolean b=m_group.setLeader(this); 
                System.out.println("  Leader is Set :::::"+b+"  ID :::::"+this.getId());
            }
        }
        catch(Exception e)
        {
            System.out.println("setUpGroup Error------"+e);
            e.printStackTrace();
        }        

    }
    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        this.addField("id", m_id);
        this.addField("host", null);
        m_partitions = new CatalogMap<Partition>(catalog, this, path + "/" + "partitions", Partition.class);
        m_childCollections.put("partitions", m_partitions);
        this.addField("isUp", m_isUp);
        this.addField("messenger_port", m_messenger_port);
        this.addField("proc_port", m_proc_port);
        this.addField("replicaSiteId", m_replicaSiteId);
        this.addField("isPrimary", m_isPrimary);
        m_replicaSet= new LinkedList<Integer>();
        this.addField("replicaSet", null);         
        this.addField("group", null);
        this.addField("leaderId", m_leaderId);
        
        
    }

    public void update() {
        m_id = (Integer) m_fields.get("id");
        m_isUp = (Boolean) m_fields.get("isUp");
        m_messenger_port = (Integer) m_fields.get("messenger_port");
        m_proc_port = (Integer) m_fields.get("proc_port");
        m_replicaSiteId=(Integer) m_fields.get("replicaSiteId");//Every site with this id belongs to the same replica set
        m_isPrimary = (Boolean) m_fields.get("isPrimary");
        System.out.println("isPrimary:::::++++++"+ m_isPrimary);
        m_replicaSet=this.getReplicaSet();  
       
        if(m_group==null)
            this.setUpGroup();
        System.out.println("AFTER SETUP::::GROUP:::::++++++"+ m_group);
        if(m_replicaSiteId!=Integer.MIN_VALUE)
        {
            try {
                m_leaderId=m_group.getLeader();
               System.out.println("m_leaderId :::"+m_leaderId);
            } catch (Exception e) { 
                System.out.println("m_leaderId :::"+m_leaderId);
                e.printStackTrace();
            }
            
        }
    }
    
    public Object getField(String field) {
        if(field.equalsIgnoreCase("replicaSet"))
            return this.getReplicaSet();
        if(field.equals("group"))
            return  m_group;
        if(field.equals("leaderId"))
            try {
                System.out.println("---Group---NOT INSIDE setup"+m_group);
                if(m_group==null)
                {
                    System.out.println("---Group---b4 setup"+m_group);
                    this.setUpGroup();
                    System.out.println("---Group---After setup"+m_group);
                }
                m_leaderId=m_group.getLeader();
               return m_leaderId;     
            } catch (Exception e) { 
                Log.error(e);
                e.printStackTrace();
            }
        return super.getField(field);
    }

    /** GETTER: replicaSetLeaderId */
    public int getReplicaSetLeaderId() { 
        try {
            return m_group==null? Integer.MIN_VALUE:m_group.getLeader();
        } catch (Exception e) {
            Log.error(e);
            e.printStackTrace();
        }
        return Integer.MIN_VALUE;
    }
    /** GETTER: replicaSiteId */
    public int getReplicaSiteId() { 
        return m_replicaSiteId;
    }
    
    /** GETTER replicaSet */
    public List<Integer> getReplicaSet() { 
        return m_group==null? null:m_group.getChildren();
    }
    /** GETTER: Site Id */
    public int getId() {
        return m_id;
    }
    
    /** GETTER: Which host does the site belong to? */
    public Host getHost() {
        Object o = getField("host");
        if (o instanceof UnresolvedInfo) {
            UnresolvedInfo ui = (UnresolvedInfo) o;
            Host retval = (Host) m_catalog.getItemForRef(ui.path);
            assert(retval != null);
            m_fields.put("host", retval);
            return retval;
        }
        return (Host) o;
    }

    /** GETTER: Which logical data partition does this host process? */
    public CatalogMap<Partition> getPartitions() {
        return m_partitions;
    }

    /** GETTER: Is the site up? */
    public boolean getIsup() {
        return m_isUp;
    }

    /** GETTER: Port used by HStoreCoordinator */
    public int getMessenger_port() {
        return m_messenger_port;
    }

    /** GETTER: Port used by VoltProcedureListener */
    public int getProc_port() {
        return m_proc_port;
    }

    /** SETTER: Site Id */
    public void setReplicaSiteId(int value) {
        
        m_replicaSiteId = value;
        m_fields.put("replicaSiteId", value);
        
       }
    
    /** SETTER: Site Id */
    public void setReplicaSet(LinkedList<Integer> value) {
        this.m_replicaSet = value;
        m_fields.put("replicaSet", value);
       }
    
    
    /** SETTER: Site Id */
    public void setId(int value) {
        m_id = value; m_fields.put("id", value);
    }

    /** SETTER: Which host does the site belong to? */
    public void setHost(Host value) {
        m_fields.put("host", value);
    }

    /** SETTER: Is the site up? */
    public void setIsup(boolean value) {
        m_isUp = value; m_fields.put("isUp", value);
    }

    /** SETTER: Port used by HStoreCoordinator */
    public void setMessenger_port(int value) {
        m_messenger_port = value; m_fields.put("messenger_port", value);
    }

    /** SETTER: Port used by VoltProcedureListener */
    public void setProc_port(int value) {
        m_proc_port = value; m_fields.put("proc_port", value);
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
    public InetAddress getInetAddress() {
        // TODO Auto-generated method stub
        
        return null;
    }

    @Override
    /* -1:deleted/left
     * 1: joined
     */
    public void process(WatchedEvent event) {
        EventType eventType=EventType.fromInt(event.getType().getIntValue());
       
        if(eventType==EventType.NodeDeleted)
        {
            try{
                int leader= m_group.getLeader();
            }
            catch(Exception e)
            {
                m_group.setLeader(this);
            }
            
        }
        else if(eventType==EventType.NodeCreated)
        {
            System.out.println(" My Id is :"+this.getId()+" And I have been notified of node creation ");

        }
        else if(eventType==EventType.NodeChildrenChanged)
        {
            System.out.println(" My Id is :"+this.getId()+" And I have been notified of number of children changing");
        }
        System.out.println(" My Id is :"+this.getId()+"   ----  "+event+" type --"+eventType);
    
        
        
        
    }

    @Override
    public int getSetId() { 
        return this.getReplicaSiteId();
    }

}
