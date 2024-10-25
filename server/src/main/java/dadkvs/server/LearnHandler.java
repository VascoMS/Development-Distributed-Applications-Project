package dadkvs.server;

import dadkvs.DadkvsPaxos;

import java.util.HashMap;
import java.util.List;

public class LearnHandler {
    private final DadkvsServerState serverState;
    private final HashMap<Integer, LearnRequestEntry> learnCountMap;
    private final int learnMajority;

    public LearnHandler(DadkvsServerState serverState) {
        this.serverState = serverState;
        this.learnCountMap = new HashMap<>();
        this.learnMajority = serverState.total_num_servers / 2 + 1;
    }

    public synchronized void handleLearnRequest(DadkvsPaxos.LearnRequest learnRequest) {
        LearnRequestEntry learnRequestEntry = learnCountMap.get(learnRequest.getLearnindex());
        int reqId = learnRequest.getLearnvalue();
        int index = learnRequest.getLearnindex();
        if (learnRequestEntry == null || learnRequest.getLearntimestamp() > learnRequestEntry.getTimestamp()) {
            learnCountMap.put(index, new LearnRequestEntry(learnRequest.getLearntimestamp()));
        } else if (learnRequest.getLearntimestamp() == learnRequestEntry.getTimestamp() &&
                learnRequestEntry.increaseCount() == learnMajority) {
            System.out.println("LEARNER COUNT: " + learnRequestEntry.getCount() + " TIMESTAMP: " + learnRequest.getLearntimestamp());
            System.out.println("MOVING REQ TO LOG: req-" + reqId + " index- " + index);
            serverState.moveTransactionsToLog(List.of(reqId), index);
            try {
                serverState.execution_lock.lock();
                serverState.execution_condition.signal();
            } finally {
                serverState.execution_lock.unlock();
            }
        }
    }

}
