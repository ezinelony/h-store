package edu.brown.hstore.replication;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;

public class Election<E extends InetSocketAddress> {
    
    final int tickTime = 2000;
    final int initLimit = 3;
    final int syncLimit = 3;
    private HashMap<Long,QuorumServer> peers;
    private HashMap<Long,E> participants;
    private QuorumPeer election;
    
    public Election(HashMap<Long,E> list) throws IOException
    {
        
        participants=list;
        peers= new HashMap<Long,QuorumServer> ();
        Set<Long> keys=list.keySet();
        for(Long k:keys)
        {
            peers.put(Long.valueOf(k),new QuorumServer(k, list.get(k)));
        }
        election = new QuorumPeer(peers, File.createTempFile("00", "00"), File.createTempFile("11","11"), list.get(0).getPort(), 0, 2, tickTime, initLimit, syncLimit);
        election.start();
    }

}
