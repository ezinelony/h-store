package edu.brown.designer.placement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Database;

import edu.brown.statistics.Histogram;
import edu.brown.utils.FileUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;


public class TransformTransactionTraces {

    private static final Logger LOG = Logger.getLogger(TransformTransactionTraces.class);
    
    static class TxnPartition {
        int basePartition;
        List<Set<Integer>> partitions = new ArrayList<Set<Integer>>();
        
        public TxnPartition(int basePartition) {
            this.basePartition = basePartition;
        }
        
        public List<Set<Integer>> getPartitions() {
            return partitions;
        }
        
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("base partition: " + String.valueOf(basePartition) + " ");
            for (Set<Integer> batch : partitions) {
                for (Integer partition_id : batch) {
                    sb.append(String.valueOf(partition_id) + " ");
                }
            }
            return sb.toString();
        }
    }
    
    public static void transform(List<TransactionTrace> txn_traces, PartitionEstimator est, Database catalogDb) {
        Histogram<String> hist = new Histogram<String>();
        List<TxnPartition> al_txn_partitions = new ArrayList<TxnPartition>();
//        Map<String, Integer> txn_partition_counts = new HashMap<String, Integer>();
        TxnPartition txn_partitions;

        for (TransactionTrace trace : txn_traces) {
            int base_partition = 0;
            try {
                base_partition = est.getBasePartition(trace);
            } catch (Exception e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            txn_partitions = new TxnPartition(base_partition);
            int batch = 0;
            for (Integer batch_id : trace.getBatchIds()) {
                // list of query traces
                Set<Integer> query_partitions = new HashSet<Integer>();
                for (QueryTrace qt :trace.getBatches().get(batch_id)) {
                    try {
                        query_partitions.addAll(est.getAllPartitions(qt, base_partition));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                txn_partitions.getPartitions().add(query_partitions);
                batch++;
            }
            if (!hist.contains(txn_partitions.toString())) {
                hist.put(txn_partitions.toString(), 1);
            } else {
                long count = hist.get(txn_partitions.toString());
                count++;
                hist.put(txn_partitions.toString(), count);
            }
            al_txn_partitions.add(txn_partitions);
        }
        LOG.info("Partition Histogram");
        LOG.info(hist.toString());
        writeFile(al_txn_partitions);
//        for (TxnPartition tp : al_txn_partitions) {
//            LOG.info(tp.toString());                    
//        }
    }
    
    /**
     * File format like the following:
     * # of batches # of transactions
     * base partition
     * [partitions batch 0 touches]
     * [partitions batch 1 touches]
     * ...
     * ...
     * [partitions batch n touches]
     * EOF
     * @param xact_partitions
     */
    public static void writeFile(List<TxnPartition> xact_partitions) {
        assert (xact_partitions.size() > 0);
        TxnPartition xact_partition = xact_partitions.get(0);
        StringBuilder sb = new StringBuilder();
        sb.append(xact_partition.getPartitions().size() + " " + xact_partitions.size() + "\n");
        for (TxnPartition txn_partition: xact_partitions) {
            sb.append(txn_partition.basePartition + "\n");
            for (Set<Integer> partition_set : txn_partition.getPartitions()) {
                for (Integer p_id : partition_set) {
                    sb.append(p_id + " ");
                }
                sb.append("\n");
            }
        }
        try {
            FileUtil.writeStringToFile("/home/sw47/Desktop/transaction.trace", sb.toString()); // FIXME
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }
}