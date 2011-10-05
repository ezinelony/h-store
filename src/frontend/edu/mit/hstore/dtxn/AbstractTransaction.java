/***************************************************************************
 *   Copyright (C) 2011 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   Permission is hereby granted, free of charge, to any person obtaining *
 *   a copy of this software and associated documentation files (the       *
 *   "Software"), to deal in the Software without restriction, including   *
 *   without limitation the rights to use, copy, modify, merge, publish,   *
 *   distribute, sublicense, and/or sell copies of the Software, and to    *
 *   permit persons to whom the Software is furnished to do so, subject to *
 *   the following conditions:                                             *
 *                                                                         *
 *   The above copyright notice and this permission notice shall be        *
 *   included in all copies or substantial portions of the Software.       *
 *                                                                         *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
 *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
 *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
 *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
 *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
 *   OTHER DEALINGS IN THE SOFTWARE.                                       *
 ***************************************************************************/
package edu.mit.hstore.dtxn;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.ExecutionSite;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Procedure;

import edu.brown.utils.Poolable;

/**
 * @author pavlo
 */
public abstract class AbstractTransaction implements Poolable {
    private static final Logger LOG = Logger.getLogger(AbstractTransaction.class);
    private static final boolean d = LOG.isDebugEnabled();
    private static final boolean t = LOG.isTraceEnabled();

    /**
     * Internal state for the transaction
     */
    protected enum RoundState {
        NULL,
        INITIALIZED,
        STARTED,
        FINISHED;
    }
    
    public static String formatTxnName(Procedure catalog_proc, long txn_id) {
        if (catalog_proc != null) {
            return (catalog_proc.getName() + " #" + txn_id);
        }
        return ("#" + txn_id);
    }

    // ----------------------------------------------------------------------------
    // GLOBAL DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    /**
     * 
     */
    public final Map<Integer, List<VoltTable>> ee_dependencies = new HashMap<Integer, List<VoltTable>>();
    
    /**
     * A simple flag that lets us know that the HStoreSite is done with this guy
     */
    private boolean hstoresite_finished = false;

    // ----------------------------------------------------------------------------
    // PREDICTIONS FLAGS
    // ----------------------------------------------------------------------------
    
    /** Whether this txn can abort */
    private boolean predict_abortable = true;
    
    /** Whether we predict that this txn will be read-only */
    private boolean predict_readOnly = false;
    
    // ----------------------------------------------------------------------------
    // EXECUTION FLAGS
    // ----------------------------------------------------------------------------
    
    /** Whether this transaction has been read-only so far */
    protected boolean exec_readOnly[];

    /** Whether this Transaction has submitted work to the EE that may need to be rolled back */
    protected boolean exec_eeWork[];
    
    /** This is set to true if the transaction did some work without an undo buffer **/
    private boolean exec_noUndoBuffer[];
    
    /**
     * Whether this transaction's control code is executing at this partition
     */
    protected boolean exec_local;
    
    // ----------------------------------------------------------------------------
    // INVOCATION DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    protected long txn_id = -1;
    protected long client_handle;
    protected int base_partition;
    protected final Set<Integer> touched_partitions = new HashSet<Integer>();
    protected Long last_undo_token;
    protected RoundState round_state;
    protected int round_ctr = 0;
    
    protected Long ee_finished_timestamp;
    protected boolean rejected;
    protected RuntimeException pending_error;

    /**
     * PartitionDependencyKey
     */
//    protected class PartitionDependencyKey extends Pair<Integer, Integer> {
//        public PartitionDependencyKey(Integer partition_id, Integer dependency_id) {
//            super(partition_id, dependency_id);
//        }
//    }
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    /**
     * Constructor
     * @param executor
     */
    public AbstractTransaction() {
        // FIXME: Allocate execute arrays
    }

    /**
     * Initialize this TransactionState for a new Transaction invocation
     * @param txn_id
     * @param client_handle
     * @param predict_readOnly TODO
     * @param predict_abortable TODO
     * @param exec_local
     * @param dtxn_txn_id
     */
    protected final AbstractTransaction init(long txn_id, long client_handle, int base_partition,
                                             boolean predict_readOnly, boolean predict_abortable, boolean exec_local) {
        this.txn_id = txn_id;
        this.client_handle = client_handle;
        this.base_partition = base_partition;
        this.round_state = RoundState.NULL;
        this.rejected = false;

        this.predict_readOnly = predict_readOnly;
        this.predict_abortable = predict_abortable;
        this.exec_local = exec_local;
        
        return (this);
    }

    @Override
    public boolean isInitialized() {
        return (this.txn_id != -1);
    }
    
    /**
     * Should be called once the TransactionState is finished and is
     * being returned to the pool
     */
    @Override
    public void finish() {
        this.txn_id = -1;
        this.hstoresite_finished = false;
        this.pending_error = null;
        this.ee_finished_timestamp = null;
        this.last_undo_token = null;
        
        this.predict_readOnly = false;
        this.predict_abortable = true;
        
        for (int i = 0; i < this.exec_readOnly.length; i++) {
            this.exec_readOnly[i] = true;
            this.exec_eeWork[i] = false;
            this.exec_noUndoBuffer[i] = false;
        }
        
        
        this.touched_partitions.clear();
    }

    // ----------------------------------------------------------------------------
    // IMPLEMENTING CLASS METHODS
    // ----------------------------------------------------------------------------

    /**
     * Initialize this TransactionState
     * @param txnId
     * @param clientHandle
     * @param source_partition
     * @param predict_singlePartitioned TODO
     * @param predict_readOnly TODO
     * @param predict_abortable TODO
     */
    public abstract <T extends AbstractTransaction> T init(long txnId, long clientHandle, int source_partition, boolean predict_readOnly, boolean predict_abortable);
    
    // ----------------------------------------------------------------------------
    // ROUND METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Must be called once before one can add new FragmentTaskMessages for this txn 
     * @param undoToken
     */
    public void initRound(long undoToken) {
        assert(this.round_state == RoundState.NULL || this.round_state == RoundState.FINISHED) : 
            "Invalid round state " + this.round_state + " for txn #" + this.txn_id;
        
        if (this.last_undo_token == null || undoToken != ExecutionSite.DISABLE_UNDO_LOGGING_TOKEN) {
            this.last_undo_token = undoToken;
        }
        if (undoToken == ExecutionSite.DISABLE_UNDO_LOGGING_TOKEN) {
            this.exec_noUndoBuffer[this.base_partition] = true;
        }
        this.round_state = RoundState.INITIALIZED;
//        this.pending_error = null;
        
        if (d) LOG.debug(String.format("Initializing new round information for %slocal txn #%d [undoToken=%d]",
                                       (this.exec_local ? "" : "non-"), this.txn_id, undoToken));
    }
    
    /**
     * Called once all of the FragmentTaskMessages have been submitted for this txn
     * @return
     */
    public void startRound() {
        assert(this.round_state == RoundState.INITIALIZED) :
            "Invalid round state " + this.round_state + " for txn #" + this.txn_id;
        
        this.round_state = RoundState.STARTED;
        
        if (d) LOG.debug("Starting round for local txn #" + this.txn_id);
    }
    
    /**
     * When a round is over, this must be called so that we can clean up the various
     * dependency tracking information that we have
     */
    public void finishRound() {
        assert(this.round_state == RoundState.STARTED) :
            "Invalid round state " + this.round_state + " for txn #" + this.txn_id;
        
        this.round_state = RoundState.FINISHED;
        this.round_ctr++;
    }
    
    // ----------------------------------------------------------------------------
    // PREDICTIONS
    // ----------------------------------------------------------------------------
    
    /**
     * Returns true if this Transaction was originally predicted as being able to abort
     */
    public boolean isPredictAbortable() {
        return (this.predict_abortable);
    }
    /**
     * Returns true if this transaction was originally predicted as read only
     */
    public boolean isPredictReadOnly() {
        return (this.predict_readOnly);
    }
    
    // ----------------------------------------------------------------------------
    // EXECUTION FLAG METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Mark this transaction as have performed some modification on this partition
     */
    public void markExecNotReadOnly(int partition) {
        this.exec_readOnly[partition] = false;
    }
    /**
     * Returns true if this transaction has not executed any modifying work at this partition
     */
    public boolean isExecReadOnly(int partition) {
        return (this.exec_readOnly[partition]);
    }
    /**
     * Returns true if this transaction executed without undo buffers at some point
     */
    public boolean isExecNoUndoBuffer(int partition) {
        return (this.exec_noUndoBuffer[partition]);
    }
    public void markExecNoUndoBuffer(int partition) {
        this.exec_noUndoBuffer[partition] = true;
    }
    /**
     * Returns true if this transaction's control code running at this partition 
     */
    public boolean isExecLocal() {
        return this.exec_local;
    }
    
    // ----------------------------------------------------------------------------
    // GENERAL METHODS
    // ----------------------------------------------------------------------------

    public boolean isRejected() {
        return (this.rejected);
    }
    
    public void markAsRejected() {
        this.rejected = true;
    }
    
    /**
     * Returns true if this transaction has done something at this partition
     */
    public boolean hasStarted() {
        return (this.last_undo_token != null);
    }
    
    /**
     * Get the current batch/round counter
     */
    public int getCurrentRound() {
        return (this.round_ctr);
    }
    
    /**
     * Returns true if this transaction has a pending error
     */
    public boolean hasPendingError() {
        return (this.pending_error != null);
    }
    /**
     * Return the pending error for this transaction
     * Does not clear it.
     * @return
     */
    public RuntimeException getPendingError() {
        return (this.pending_error);
    }
    public String getPendingErrorMessage() {
        return (this.pending_error != null ? this.pending_error.getMessage() : null);
    }
    
    /**
     * 
     * @param error
     */
    public synchronized void setPendingError(RuntimeException error) {
        assert(error != null) : "Trying to set a null error for txn #" + this.txn_id;
        if (this.pending_error == null) {
            if (d) LOG.debug("Got error for txn #" + this.txn_id + ". Aborting...");
            this.pending_error = error;
        }
    }
    
    // ----------------------------------------------------------------------------
    // Keep track of whether this txn executed stuff at this partition's EE
    // ----------------------------------------------------------------------------
    
    /**
     * Should be called whenever the txn submits work to the EE 
     */
    public void setSubmittedEE(int partition) {
        this.exec_eeWork[partition] = true;
    }
    
    public void unsetSubmittedEE(int partition) {
        this.exec_eeWork[partition] = false;
    }
    /**
     * Returns true if this txn has submitted work to the EE that needs to be rolled back
     * @return
     */
    public boolean hasSubmittedEE(int partition) {
        return (this.exec_eeWork[partition]);
    }
    
    // ----------------------------------------------------------------------------
    // Whether the ExecutionSite is finished with the transaction
    // ----------------------------------------------------------------------------
    
    
    /**
     * Mark this txn as finished (and thus ready for clean-up)
     */
    public void setEE_Finished() {
        if (this.ee_finished_timestamp == null) {
            this.ee_finished_timestamp = System.currentTimeMillis();
        }
    }
    /**
     * Is this TransactionState marked as finished
     * @return
     */
    public boolean isEE_Finished() {
        return (this.ee_finished_timestamp != null);
    }
    /**
     * 
     * @return
     */
    public long getEE_FinishedTimestamp() {
        assert(this.ee_finished_timestamp != null);
        return (this.ee_finished_timestamp);
    }
    
    // ----------------------------------------------------------------------------
    // Whether the HStoreSite is finished with the transaction
    // This assumes that all of the ExecutionSites are finished with it too
    // ----------------------------------------------------------------------------
    
    
    /**
     * Returns true if this transaction is finished at this HStoreSite
     * @return
     */
    public boolean isHStoreSite_Finished() {
        if (t) LOG.trace(String.format("%s - Returning HStoreSite done [val=%s, hash=%d]", this, this.hstoresite_finished, this.hashCode()));
        return (this.hstoresite_finished);
    }
    public void setHStoreSite_Finished(boolean val) {
        this.hstoresite_finished = val;
        if (t) LOG.trace(String.format("%s - Setting HStoreSite done [val=%s, hash=%d]", this, this.hstoresite_finished, this.hashCode()));
    }
    
    
    
    /**
     * Get this state's transaction id
     * @return
     */
    public long getTransactionId() {
        return this.txn_id;
    }
    /**
     * Get the current Round that this TransactionState is in
     * Used only for testing  
     * @return
     */
    protected RoundState getCurrentRoundState() {
        return (this.round_state);
    }
    /**
     * 
     * @return
     */
    public Long getLastUndoToken() {
        return this.last_undo_token;
    }
    
    /**
     * @return the client_handle
     */
    public long getClientHandle() {
        return this.client_handle;
    }

    /**
     * Get the base PartitionId where this txn's Java code is executing on
     * @return
     */
    public int getBasePartition() {
        return base_partition;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AbstractTransaction) {
            AbstractTransaction other = (AbstractTransaction)obj;
            if ((other.txn_id == -1) && (this.txn_id == -1)) return (this.hashCode() != other.hashCode());
            return (this.txn_id == other.txn_id && this.base_partition == other.base_partition);
        }
        return (false);
    }

    @Override
    public String toString() {
        if (this.isInitialized()) {
            return "Txn #" + this.txn_id;
        } else {
            return ("<Uninitialized>");
        }
    }
    
    public abstract String debug();
    
    protected Map<String, Object> getDebugMap() {
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        m.put("Transaction #", this.txn_id);
        m.put("Current Round State", this.round_state);
        m.put("Read-Only", this.exec_readOnly);
        m.put("Executing Locally", this.exec_local);
        m.put("Last UndoToken", this.last_undo_token);
        m.put("# of Rounds", this.round_ctr);
        m.put("Pending Error", (this.pending_error != null ? this.pending_error.toString() : null));
        return (m);
    }
}