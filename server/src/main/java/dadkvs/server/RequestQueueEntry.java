package dadkvs.server;

public class RequestQueueEntry {
    private final int reqid;
    private int priorityNumber;
    private final TransactionRecord transactionRecord;

    public RequestQueueEntry(int reqid, TransactionRecord transactionRecord, int priorityNumber) {
        this.reqid = reqid;
        this.transactionRecord = transactionRecord;
        this.priorityNumber = priorityNumber;
    }

    public int getPriorityNumber(){
        return priorityNumber;
    }

    public void setPriorityNumber(int priorityNumber){
        this.priorityNumber = priorityNumber;
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
