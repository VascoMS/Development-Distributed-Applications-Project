package dadkvs.server;

import java.util.Comparator;

public class RequestPriorityComparator implements Comparator<RequestQueueEntry> {
    @Override
    public int compare(RequestQueueEntry req1, RequestQueueEntry req2){
        return Integer.compare(req1.getPriorityNumber(), req2.getPriorityNumber());
    }
}
