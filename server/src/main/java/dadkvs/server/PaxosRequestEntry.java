package dadkvs.server;

public class PaxosRequestEntry {
    private TransactionRecord transactionRecord;
    private RequestState requestState;

    public PaxosRequestEntry(TransactionRecord transactionRecord, RequestState requestState) {
        this.transactionRecord = transactionRecord;
        this.wasCommited = false;
        this.wasAborted = false;
    }

    public PaxosRequestEntry() {
        this.transactionRecord = null;
        this.wasCommited = false;
        this.wasAborted = false;
    }

    public boolean wasAborted() {
        return wasAborted;
    }

    public void setAborted() {
        this.wasAborted = true;
        this.wasCommited = false;
    }

    public boolean hasCompleted() {
        return this.wasAborted || this.wasCommited;
    }

    public boolean isPending() {
        return !this.wasAborted && !this.wasCommited;
    }

    public TransactionRecord getTransactionRecord() {
        return transactionRecord;
    }

    public void setTransactionRecord(TransactionRecord transactionRecord){
        this.transactionRecord = transactionRecord;
    }

    public boolean wasCommited() {
        return wasCommited;
    }

    public boolean transactionIsAvailable(){return transactionRecord != null;}

    public void setCommited() {
        wasCommited = true;
        wasAborted = false;
    }
}
