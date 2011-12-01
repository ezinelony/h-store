package edu.mit.hstore.callbacks;

import java.util.Collection;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstore;
import edu.brown.hstore.Hstore.Status;
import edu.brown.hstore.Hstore.TransactionInitResponse;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.dtxn.MapReduceTransaction;

/**
 * This is callback is used on the remote side of a TransactionMapRequest
 * so that the network-outbound callback is not invoked until all of the partitions
 * at this HStoreSite is finished with the Map phase. 
 * @author pavlo
 */
public class TransactionMapWrapperCallback extends BlockingCallback<Hstore.TransactionMapResponse, Integer> {
	private static final Logger LOG = Logger.getLogger(TransactionMapWrapperCallback.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
	
	private Hstore.TransactionMapResponse.Builder builder = null;
	
	public TransactionMapWrapperCallback(HStoreSite hstore_site) {
		super(hstore_site, false);
	}
	
	public void init(MapReduceTransaction ts, RpcCallback<Hstore.TransactionMapResponse> orig_callback) {
		this.builder = Hstore.TransactionMapResponse.newBuilder()
        					 .setTransactionId(txn_id)
        					 .setStatus(Hstore.Status.OK);
		super.init(txn_id, hstore_site.getLocalPartitionIds().size(), orig_callback);
	}
	
	@Override
	protected void abortCallback(Status status) {
		if (debug.get())
            LOG.debug(String.format("Txn #%d - Aborting %s with status %s",
                                    this.txn_id, this.getClass().getSimpleName(), status));
        this.builder.setStatus(status);
        Collection<Integer> localPartitions = hstore_site.getLocalPartitionIds();
        for (Integer p : this.hstore_site.getLocalPartitionIds()) {
            if (localPartitions.contains(p) && this.builder.getPartitionsList().contains(p) == false) {
                this.builder.addPartitions(p.intValue());
            }
        } // FOR
        this.unblockCallback();
	}

	@Override
	protected void finishImpl() {
		this.builder = null;
	}
	
    @Override
    public boolean isInitialized() {
        return (this.builder != null && super.isInitialized());
    }

	@Override
	protected int runImpl(Integer partition) {
		if (this.isAborted() == false)
            this.builder.addPartitions(partition.intValue());
        return 1;
	}

	@Override
	protected void unblockCallback() {
		if (debug.get()) {
            LOG.debug(String.format("Txn #%d - Sending %s to %s with status %s",
                                    this.txn_id,
                                    TransactionInitResponse.class.getSimpleName(),
                                    this.getOrigCallback().getClass().getSimpleName(),
                                    this.builder.getStatus()));
        }
        assert(this.getOrigCounter() == builder.getPartitionsCount()) :
            String.format("The %s for txn #%d has results from %d partitions but it was suppose to have %d.",
                          builder.getClass().getSimpleName(), txn_id, builder.getPartitionsCount(), this.getOrigCounter());
        assert(this.getOrigCallback() != null) :
            String.format("The original callback for txn #%d is null!", txn_id);
        this.getOrigCallback().run(this.builder.build());		
	}

}