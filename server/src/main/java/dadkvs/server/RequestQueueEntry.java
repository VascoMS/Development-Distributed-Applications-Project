package dadkvs.server;

public class RequestQueueEntry {
    private final int reqid;
    private final TransactionRecord transactionRecord;

    public RequestQueueEntry(int reqid, TransactionRecord transactionRecord) {
        this.reqid = reqid;
        this.transactionRecord = transactionRecord;
    }

    public int getReqid() {
        return reqid;
    }

    public TransactionRecord getTransactionRecord() {
        return transactionRecord;
    }

    @Override
    public String toString() {
        return "RequestQueueEntry{" +
                "reqid=" + reqid +
                '}';
    }
}
