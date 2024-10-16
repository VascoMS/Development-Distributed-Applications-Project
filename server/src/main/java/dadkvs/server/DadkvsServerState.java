package dadkvs.server;

import com.google.rpc.context.AttributeContext;
import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class DadkvsServerState {
    private static final int DEFAULT_CONFIG = 0;
    private static final int BLANK_ENTRY = -1;
    private static final int NUM_MULTIPAXOS_ROUNDS = 5;
    public final Lock execution_lock;
    public final Map<Integer, Condition> transaction_execution_conditions;
    public final Lock leader_lock;
    private final ConcurrentLinkedQueue<RequestQueueEntry> request_queue;
    private final ConcurrentHashMap<Integer, CompletableFuture<Boolean>> request_future_map;
    private final ConcurrentHashMap<Integer, PaxosRequestEntry> transaction_consensus_map;
    ConcurrentHashMap<Integer, PaxosRoundState> paxos_round_state_map;
    public final List<Integer> transaction_execution_log;
    private int current_config;
    public final int[][] configuration_matrix;
    //private final Lock queue_lock;
    private final Condition empty_queue_condition;
    private final Condition i_am_leader_condition;
    public final Condition reconfig_condition;
    private final ExecutorService paxos_leader_executor;
    boolean i_am_leader;
    int debug_mode;
    int base_port;
    int my_id;
    int store_size;
    int total_num_servers;
    // TODO: Implement new current index logic
    int current_index;

    String default_host;
    KeyValueStore store;
    //MainLoop main_loop;
    Thread leader_worker;
    DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[] async_paxos_stubs;
    String[] paxos_targets;


    public DadkvsServerState(int kv_size, int port, int myself) {
        base_port = port;
        total_num_servers = 5;
        my_id = myself;
        i_am_leader = false;
        default_host = "localhost";
        debug_mode = 0;
        current_index = -1;
        paxos_round_state_map = new ConcurrentHashMap<>();
        request_queue = new ConcurrentLinkedQueue<>();
        request_future_map = new ConcurrentHashMap<>();
        transaction_consensus_map = new ConcurrentHashMap<>();
        transaction_execution_log = new ArrayList<>();
        leader_lock = new ReentrantLock();
        i_am_leader_condition = leader_lock.newCondition();
        empty_queue_condition = leader_lock.newCondition();
        reconfig_condition = leader_lock.newCondition();
        execution_lock = new ReentrantLock();
        transaction_execution_conditions = new HashMap<>();
        // TODO: Maybe store config for each round
        current_config = DEFAULT_CONFIG;
        configuration_matrix = new int[][]{{0, 1, 2}, {1, 2, 3}, {2, 3, 4}};
        paxos_leader_executor = Executors.newFixedThreadPool(NUM_MULTIPAXOS_ROUNDS);

        store_size = kv_size;
        store = new KeyValueStore(kv_size);
        leader_worker = new Thread(this::runPaxos);
        //main_loop_worker.start();
        paxos_targets = new String[total_num_servers];

        for (int i = 0; i < total_num_servers; i++) {
            int target_port = base_port + i;
            paxos_targets[i] = default_host + ":" + target_port;
        }

        initPaxosStubs();
        leader_worker.start();
    }

    public void runPaxos() {
        while (true) {
            leader_lock.lock();
            try {
                if (i_am_leader) {
                    if (!request_queue.isEmpty()) {
                        RequestQueueEntry nextRequestToPropose = request_queue.peek();

                        moveTransactionToMap(nextRequestToPropose.getReqid(), RequestState.AWAITING_PROPOSAL);
                        paxos_leader_executor.submit(() -> runAsLeader(nextRequestToPropose));

                        if(nextRequestToPropose.getTransactionRecord().isReconfigTransaction()){
                            reconfig_condition.await();
                        }
                    } else {
                        // Waiting for queue to have transactions to be proposed
                        empty_queue_condition.await();
                    }
                } else {
                    i_am_leader_condition.await();
                }
            } catch (InterruptedException e) {
                System.out.println("Thread Interrupted");
            } finally {
                leader_lock.unlock();
            }
        }
    }



    public void signalNewLeader(boolean isLeader) {
        leader_lock.lock();
        try {
            i_am_leader = isLeader;
            if (isLeader)
                i_am_leader_condition.signal();  // Wake up threads waiting to become leader
        } finally {
            leader_lock.unlock();
        }
    }

    public CompletableFuture<Boolean> waitForTransactionExecution(Integer reqid) {
        CompletableFuture<Boolean> transaction_result_future = new CompletableFuture<>();
        request_future_map.put(reqid, transaction_result_future);
        return transaction_result_future;
    }

    private int getMajority(){
        return configuration_matrix[current_config].length / 2 + 1;
    }

    private void runAsLeader(RequestQueueEntry requestQueueEntry) {
        boolean reached_consensus = false;
        //TODO: Put request back in the queue if it wasn't accepted by the consensus and wakeup reconfig condition
        updateIndex();
        loadConfiguration();
        System.out.println("Config loaded: " + current_config);
        while (!reached_consensus && i_am_leader && isIndexEmpty(current_index)) {
            List<DadkvsPaxos.PhaseOneReply> phase_one_responses = new ArrayList<>();
            GenericResponseCollector<DadkvsPaxos.PhaseOneReply> phase_one_collector =
                    new GenericResponseCollector<>(phase_one_responses, total_num_servers);
            List<DadkvsPaxos.PhaseTwoReply> phase_two_responses = new ArrayList<>();
            GenericResponseCollector<DadkvsPaxos.PhaseTwoReply> phase_two_collector =
                    new GenericResponseCollector<>(phase_two_responses, total_num_servers);

            System.out.println("Queue size in leader: " + request_queue.size());
            phase_one_responses.clear();
            runPhase1(phase_one_collector, buildPhaseOneRequest(leader_ts));
            phase_one_collector.waitForTarget(getMajority());
            System.out.println("Number of responses: " + phase_one_collector.getReceived() + " Pending: " + phase_one_collector.getPending());
            System.out.println("RESPONSES: " + phase_one_responses.stream().map(DadkvsPaxos.PhaseOneReply::toString).collect(Collectors.toList()));
            boolean redo = handlePhaseOneResponses(phase_one_responses);
            if (redo)
                continue;
            if (phase_one_responses.size() >= getMajority()) {
                int chosen_value = pickValue(phase_one_responses, requestQueueEntry.getReqid());
                runPhase2(phase_two_collector, buildPhaseTwoRequest(chosen_value, leader_ts));
                phase_two_collector.waitForTarget(getMajority());
                System.out.println("Number of responses: " + phase_two_collector.getReceived() + " Pending: " + phase_one_collector.getPending());
                System.out.println("RESPONSES: " + phase_two_responses.stream().map(DadkvsPaxos.PhaseTwoReply::toString).collect(Collectors.toList()));
                redo = handlePhaseTwoResponses(phase_two_responses) ;
                if (redo)
                    continue;
                if (phase_two_responses.size() >= getMajority()) {
                    reached_consensus = true;
                    moveTransactionToLog(chosen_value, index);
                    if(chosen_value != requestQueueEntry.getReqid())
                        returnRequestToQueue(requestQueueEntry);
                    removePaxosRoundState(index);
                }
            }
        }
    }

    private void returnRequestToQueue(RequestQueueEntry requestQueueEntry){
        //TODO: Confirm whether adding to the end of the queue is bad
        request_queue.offer(requestQueueEntry);
        if(requestQueueEntry.getTransactionRecord().isReconfigTransaction())
            try {
                leader_lock.lock();
                reconfig_condition.signal();
            } finally {
                leader_lock.unlock();
            }
    }

    public void sendLearnRequests(int index, int value, int timestamp) {
        DadkvsPaxos.LearnRequest.Builder learnRequest = DadkvsPaxos.LearnRequest.newBuilder();
        GenericResponseCollector<DadkvsPaxos.PhaseOneReply> learnResponseCollector =
                new GenericResponseCollector<>(new ArrayList<>(), total_num_servers);
        learnRequest.setLearnindex(index)
                .setLearnvalue(value)
                .setLearntimestamp(timestamp);
        for (DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub stub : async_paxos_stubs) {
            CollectorStreamObserver<DadkvsPaxos.LearnReply> learnObserver =
                    new CollectorStreamObserver<>(learnResponseCollector);
            stub.learn(learnRequest.build(), learnObserver);
        }

    }

    private boolean handlePhaseOneResponses(List<DadkvsPaxos.PhaseOneReply> phaseOneResponses) {
        int largestTimestamp = leader_ts;

        for (DadkvsPaxos.PhaseOneReply response : phaseOneResponses) {
            // Update largest timestamp if response is not accepted
            if (!response.getPhase1Accepted()) {
                largestTimestamp = Math.max(largestTimestamp, response.getPhase1Timestamp());
            }
        }

        boolean hasGreaterLeader = largestTimestamp > leader_ts;
        if (hasGreaterLeader) {
            updateLeaderTimestamp(largestTimestamp);
        }

        return hasGreaterLeader;
    }

    private boolean handlePhaseTwoResponses(List<DadkvsPaxos.PhaseTwoReply> phaseTwoResponses) {
        int largestTimestamp = leader_ts;
        for (DadkvsPaxos.PhaseTwoReply response : phaseTwoResponses) {
            if (!response.getPhase2Accepted()) {
                largestTimestamp = Math.max(largestTimestamp, response.getPhase2Timestamp());
            }
        }
        boolean hasGreaterLeader = largestTimestamp > leader_ts;
        if (hasGreaterLeader) {
            updateLeaderTimestamp(largestTimestamp);
        }
        return hasGreaterLeader;
    }

    private void fillTransactionLog(int endIndex) {
        for (int i = transaction_execution_log.size(); i < endIndex; i++) {
            addTransactionToLog(null, i);
        }
    }

    private synchronized void addTransactionToLog(Integer reqid, int index) {
        if (transaction_execution_log.size() - 1 < index) {
            transaction_execution_log.add(reqid);
        } else if (transaction_execution_log.get(index) == null) {
            transaction_execution_log.add(index, reqid);
        }
    }

    public synchronized void moveTransactionToLog(int reqId, int index) {
        System.out.println("Queue size when moving to log: " + request_queue.size());
        moveTransactionToMap(reqId, RequestState.PENDING_EXECUTION);
        fillTransactionLog(index);
        addTransactionToLog(reqId, index);
    }

    public synchronized void moveTransactionToMap(int reqId, RequestState requestState){
        RequestQueueEntry req = findAndRemoveFromQueue(reqId);
        if (req != null) {
            transaction_consensus_map.put(reqId, new PaxosRequestEntry(req.getTransactionRecord(), requestState));
        } else if(!transaction_consensus_map.containsKey(reqId)) {
            transaction_consensus_map.put(reqId, new PaxosRequestEntry());
        } else if (!transaction_consensus_map.get(reqId).getRequestState().equals(requestState)){
            transaction_consensus_map.get(reqId).setRequestState(requestState);
        }
    }

    public RequestQueueEntry findAndRemoveFromQueue(int reqId){
        for(RequestQueueEntry entry : request_queue){
            if(entry.getReqid() == reqId)
                request_queue.remove(entry);
            return entry;
        }
        return null;
    }


    public DadkvsPaxos.PhaseOneRequest buildPhaseOneRequest(int ts) {
        DadkvsPaxos.PhaseOneRequest.Builder phase_one_request = DadkvsPaxos.PhaseOneRequest.newBuilder();

        phase_one_request
                .setPhase1Config(0)
                .setPhase1Index(current_index)
                .setPhase1Timestamp(ts);

        return phase_one_request.build();
    }

    public DadkvsPaxos.PhaseTwoRequest buildPhaseTwoRequest(int value, int ts) {
        DadkvsPaxos.PhaseTwoRequest.Builder phase_two_request = DadkvsPaxos.PhaseTwoRequest.newBuilder();

        phase_two_request.setPhase2Config(0)
                .setPhase2Timestamp(ts)
                .setPhase2Index(current_index)
                .setPhase2Value(value);

        return phase_two_request.build();
    }

    private void updateLeaderTimestamp(int response_ts) {
        leader_ts += (int) Math.ceil((double) (response_ts - leader_ts) / total_num_servers) * total_num_servers;
    }

    public void addTransactionRecordToQueue(Integer reqid, TransactionRecord transactionRecord) {
        RequestQueueEntry request_queue_entry = new RequestQueueEntry(reqid, transactionRecord);
        boolean was_empty = request_queue.isEmpty();
        if (transaction_consensus_map.containsKey(reqid) && !transaction_consensus_map.get(reqid).transactionIsAvailable()) {
            try {
                execution_lock.lock();
                System.out.println("Signalling thread waiting for request: " + reqid);
                transaction_consensus_map.get(reqid).setTransactionRecord(transactionRecord);
                transaction_execution_conditions.get(reqid).signal();
            } finally {
                execution_lock.unlock();
            }
        } else {
            request_queue.add(request_queue_entry);
            leader_lock.lock();
            if (was_empty)
                empty_queue_condition.signal();
            leader_lock.unlock();
        }

    }

    /*private synchronized RequestQueueEntry getReqFromQueue() {
        RequestQueueEntry req = request_queue.peek();
        return request_queue.poll() != null ? request_queue.po().getReqid(): -1;
    }*/

    private void runPhase1(GenericResponseCollector<DadkvsPaxos.PhaseOneReply> phase_one_collector, DadkvsPaxos.PhaseOneRequest request) {
        for (int serverIndex : configuration_matrix[current_config]) {
            System.out.println("Leader sending phase 1 request: ");
            DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub stub = async_paxos_stubs[serverIndex];
            CollectorStreamObserver.printMessageFields(request);
            CollectorStreamObserver<DadkvsPaxos.PhaseOneReply> phase_one_observer = new CollectorStreamObserver<>(phase_one_collector);
            stub.phaseone(request, phase_one_observer);
        }
    }

    private void runPhase2(GenericResponseCollector<DadkvsPaxos.PhaseTwoReply> phase_two_collector, DadkvsPaxos.PhaseTwoRequest request) {
        for (int serverIndex : configuration_matrix[current_config]) {
            System.out.println("Leader sending phase 2 request:\n" + request);
            DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub stub = async_paxos_stubs[serverIndex];
            CollectorStreamObserver.printMessageFields(request);
            CollectorStreamObserver<DadkvsPaxos.PhaseTwoReply> phase_two_observer = new CollectorStreamObserver<>(phase_two_collector);
            stub.phasetwo(request, phase_two_observer);
        }
    }

    private int pickValue(List<DadkvsPaxos.PhaseOneReply> responses, int reqId) {
        // Retrieving the response with the largest accepted timestamp
        int largestTimestamp = 0;
        int acceptedValue = 0;
        for(DadkvsPaxos.PhaseOneReply response : responses){
            if(response.getPhase1Timestamp() > largestTimestamp){
                largestTimestamp = response.getPhase1Timestamp();
                acceptedValue = response.getPhase1Value();
            }
        }
        return largestTimestamp > 0 ? acceptedValue : reqId;
    }

    public boolean transactionAvailable(int reqId) {
        return transaction_consensus_map.containsKey(reqId) && transaction_consensus_map.get(reqId).transactionIsAvailable();
    }

    public boolean previousTransactionComplete(int index) {
        System.out.println("Checking previous transaction for index: " + index);
        if(index == 0)
            return true;
        int reqId = getValueFromLog(index - 1);
        return reqId != -1 && transaction_consensus_map.get(reqId).hasCompleted();
    }

    public PaxosRequestEntry getPaxosRequestEntry(int reqId) {
        return transaction_consensus_map.get(reqId);
    }

    private void initPaxosStubs() {
        ManagedChannel[] channels = new ManagedChannel[total_num_servers];

        for (int i = 0; i < total_num_servers; i++) {
            channels[i] = ManagedChannelBuilder.forTarget(paxos_targets[i]).usePlaintext().build();
        }

        async_paxos_stubs = new DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[total_num_servers];

        for (int i = 0; i < total_num_servers; i++) {
            async_paxos_stubs[i] = DadkvsPaxosServiceGrpc.newStub(channels[i]);
        }
    }

    public void completeClientRequest(int reqId, boolean requestResult) {
        System.out.println("Request ID Completed: " + reqId);
        request_future_map.get(reqId).complete(requestResult);
        request_future_map.remove(reqId);
    }

    public int getValueFromLog(int index){
        return index < transaction_execution_log.size() &&
                transaction_execution_log.get(index) != null ? transaction_execution_log.get(index) : -1;
    }

    public boolean isIndexEmpty(int index) {
        return index > transaction_execution_log.size() - 1 || transaction_execution_log.get(index) == null;
    }

    public void removePaxosRoundState(int index){
        Lock paxosStateLock = paxos_round_state_map.get(index).getPaxosStateLock();
        try {
            paxosStateLock.lock();
            paxos_round_state_map.get(index).getPaxosStateLock().lock();
            paxos_round_state_map.remove(index);
        } finally {
            paxosStateLock.unlock();
        }
    }

    public void addAcceptedValue(int index, int value){
        paxos_round_state_map.get(index).setUncommitedAcceptedRequest(value);
    }

    public synchronized int updateIndex(){
        int i = current_index + 1;
        while(i < transaction_execution_log.size() && transaction_execution_log.get(i) != null) {
            i++;
        }
        current_index = i;
        return current_index;
    }

    public int getUncommitedConsensusAccept(int index){
        PaxosRoundState paxosRoundState = paxos_round_state_map.get(index);
        return paxosRoundState != null ? paxosRoundState.getUncommitedAcceptedRequest() : -1;
    }

    private boolean isReconfig(int index){
        int reqId = getValueFromLog(index);
        return reqId != -1 &&
                transaction_consensus_map
                .get(reqId)
                .getTransactionRecord()
                .isReconfigTransaction();
    }

    public boolean isPartOfConfig(int id, int[] config) {
        return Arrays.stream(config).anyMatch(member -> member == id);
    }

    public synchronized void loadConfiguration(){
        // If we find a request that is completed before we find an incomplete reconfiguration, we can set the current configuration to what is in key 0
        // else if we find an incomplete reconfiguration, we set the current configuration to the respective configuration
        for (int i = current_index; i > 0; i--) {
            // Get the transaction based on the current index
            int reqId = transaction_execution_log.get(i - 1);

            // If we find a completed transaction before an incomplete reconfiguration
            if (previousTransactionComplete(i)) {
                // Set the new configuration from the KeyValueStore
                current_config = store.read(KeyValueStore.CONFIG_KEY).getValue();
                break;
            }
            // Check if this transaction is an incomplete reconfiguration
            else if (isReconfig(i - 1)) {
                // Set the current configuration from the transaction consensus map
                current_config = transaction_consensus_map.get(reqId)
                        .getTransactionRecord().getPrepareValue();
                break;
            }
        }
        if(!isPartOfConfig(my_id, configuration_matrix[current_config])){
            i_am_leader = false;
        }
    }

    public TimestampState getTimestampState(int round){
        return paxos_round_state_map.get(round) != null ? paxos_round_state_map.get(round).getTimestampState() : null;
    }

    public int getTimestamp(int reqIndex, TimestampEnum timestampType){
        TimestampState timestampState = paxos_round_state_map.get(reqIndex).getTimestampState();
        if (timestampState == null){
            return -1;
        }
        return switch (timestampType) {
            case PREPARE -> timestampState.getLargestPrepareTs();
            case ACCEPT -> timestampState.getLargestAcceptTs();
            case LEADER -> timestampState.getLeaderTs();
            default -> -1;
        };
    }

    private int getLeaderTs(int round){
        return paxos_round_state_map.get(round).getTimestampState().getLeaderTs();
    }

}
