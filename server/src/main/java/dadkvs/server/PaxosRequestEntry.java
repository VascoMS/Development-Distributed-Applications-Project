package dadkvs.server;

public class PaxosRequestEntry {
    private TransactionRecord transactionRecord;
    private RequestState requestState;

    public PaxosRequestEntry(TransactionRecord transactionRecord, RequestState requestState) {
        this.transactionRecord = transactionRecord;
        this.requestState = requestState;
    }

    public PaxosRequestEntry() {
        this.transactionRecord = null;
        // When a request entry is created without a transactionRecord it means it was approved by consensus but
        // the request hasn't yet arrived from the client, therefore it starts in the PENDING_EXECUTION state
        this.requestState = RequestState.PENDING_EXECUTION;
    }

    public RequestState getRequestState() {
        return requestState;
    }

    public void setRequestState(RequestState requestState) {
        this.requestState = requestState;
    }

    public boolean wasAborted() {
        return requestState == RequestState.ABORTED;
    }

    public void setAborted() {
        this.requestState = RequestState.ABORTED;
    }

    public boolean hasCompleted() {
        return this.requestState == RequestState.ABORTED || this.requestState == RequestState.COMMITTED;
    }

    public boolean isPending() {
        return this.requestState == RequestState.PENDING_EXECUTION;
    }

    public TransactionRecord getTransactionRecord() {
        return transactionRecord;
    }

    public void setTransactionRecord(TransactionRecord transactionRecord) {
        this.transactionRecord = transactionRecord;
    }

    public boolean wasCommited() {
        return this.requestState == RequestState.COMMITTED;
    }

    public boolean transactionIsAvailable() {
        return transactionRecord != null;
    }

    public void setCommited() {
        this.requestState = RequestState.COMMITTED;
    }
}
