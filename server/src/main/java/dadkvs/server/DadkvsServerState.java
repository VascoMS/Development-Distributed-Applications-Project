package dadkvs.server;

import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DadkvsServerState {
    // TODO: Fix requests being repeated
    private static final int DEFAULT_CONFIG = 0;
    private static final int BLANK_ENTRY = -1;
    private static final int NUM_MULTIPAXOS_ROUNDS = 5;
    public final Lock execution_lock;
    public final Condition execution_condition;
    public final Lock leader_lock;
    public final List<Integer> transaction_execution_log;
    public final int[][] configuration_matrix;
    public final Condition reconfig_condition;
    private final ConcurrentLinkedQueue<RequestQueueEntry> request_queue;
    private final ConcurrentHashMap<Integer, CompletableFuture<Boolean>> request_future_map;
    private final ConcurrentHashMap<Integer, PaxosRequestEntry> transaction_consensus_map;
    //private final Lock queue_lock;
    private final Condition empty_queue_condition;
    private final Condition i_am_leader_condition;
    ConcurrentHashMap<Integer, PaxosRoundState> paxos_round_state_map;
    boolean i_am_leader;
    int debug_mode;
    int base_port;
    int my_id;
    int store_size;
    int total_num_servers;
    int current_index;
    String default_host;
    KeyValueStore store;
    //MainLoop main_loop;
    Thread leader_worker;
    Thread execution_worker;
    DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[] async_paxos_stubs;
    String[] paxos_targets;
    private int current_config;


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
        execution_condition = execution_lock.newCondition();
        current_config = DEFAULT_CONFIG;
        configuration_matrix = new int[][]{{0, 1, 2}, {1, 2, 3}, {2, 3, 4}};

        store_size = kv_size;
        store = new KeyValueStore(kv_size);
        leader_worker = new Thread(this::runPaxos);
        execution_worker = new Thread(this::executor);
        //main_loop_worker.start();
        paxos_targets = new String[total_num_servers];

        for (int i = 0; i < total_num_servers; i++) {
            int target_port = base_port + i;
            paxos_targets[i] = default_host + ":" + target_port;
        }

        initPaxosStubs();
        leader_worker.start();
        execution_worker.start();

    }

    public void runPaxos() {
        while (true) {
            leader_lock.lock();
            try {
                if (i_am_leader) {
                    if (!request_queue.isEmpty()) {
                        runAsLeader();
                    } else {
                        // Waiting for queue to have transactions to be proposed
                        empty_queue_condition.await();
                    }
                } else {
                    i_am_leader_condition.await();
                }
            } catch (InterruptedException e) {
                System.out.println("Thread Interrupted");
            } catch (Exception e) {
                System.err.println("Exception thrown: " + e.getMessage());
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

    public CompletableFuture<Boolean> waitForTransactionExecution(Integer reqId, TransactionRecord txRecord) {
        CompletableFuture<Boolean> transaction_result_future = new CompletableFuture<>();
        request_future_map.put(reqId, transaction_result_future);
        addTransactionRecordToQueue(reqId, txRecord);
        return transaction_result_future;
    }

    private int getMajority() {
        return configuration_matrix[current_config].length / 2 + 1;
    }

    private synchronized List<RequestQueueEntry> buildRequestBatch() {
        List<RequestQueueEntry> newRequestBatch = new ArrayList<>();
        System.out.println("Queue when building batch: " + request_queue.toString());
        // Building a request batch based on the queue size, the max batch size and the number of
        // contiguous empty slots available in the log from the current index
        for (int i = 0; !request_queue.isEmpty() && newRequestBatch.size() < NUM_MULTIPAXOS_ROUNDS && isIndexEmpty(current_index + i); i++) {
            //Taking the request out the queue, adding it into the requestBatch and incrementing the fakeIndex
            RequestQueueEntry currentRequest = request_queue.peek();
            // If learner fetched the last request from the queue after we check if queue is empty, we can break the loop
            // and return the batch
            if(currentRequest == null) {
                break;
            }
            newRequestBatch.add(currentRequest);
            moveTransactionToMap(currentRequest.getReqid(), RequestState.AWAITING_PROPOSAL);

            //if its a reconfig, we added it into the batch, but stopped adding stuff afterwards
            if (currentRequest.getTransactionRecord().isReconfigTransaction()) {
                break;
            }
        }

        return newRequestBatch;
    }


    private void runAsLeader() {
        boolean reached_consensus = false;
        int index = updateIndex();
        loadConfiguration(index);
        List<RequestQueueEntry> requestBatch = buildRequestBatch();
        int batchSize = requestBatch.size();
        initPaxosRoundStatesAsLeader(index, batchSize);
        System.out.println("Config loaded: " + current_config);
        System.out.println("Log before starting new consensus batch: " + transaction_execution_log);
        while (!reached_consensus && i_am_leader && isIndexEmpty(index)) {
            List<DadkvsPaxos.MultiPaxosPhaseOneResponse> phase_one_responses = new ArrayList<>();
            GenericResponseCollector<DadkvsPaxos.MultiPaxosPhaseOneResponse> phase_one_collector =
                    new GenericResponseCollector<>(phase_one_responses, total_num_servers);
            List<DadkvsPaxos.MultiPaxosPhaseTwoResponse> phase_two_responses = new ArrayList<>();
            GenericResponseCollector<DadkvsPaxos.MultiPaxosPhaseTwoResponse> phase_two_collector =
                    new GenericResponseCollector<>(phase_two_responses, total_num_servers);

            System.out.println("Queue size in leader: " + request_queue.size());
            runPhase1(phase_one_collector, buildPhaseOneRequest(index, batchSize));
            phase_one_collector.waitForTarget(getMajority());
            System.out.println("Number of responses: " + phase_one_collector.getReceived() + " Pending: " + phase_one_collector.getPending());

            boolean redo = handlePhaseOneResponses(phase_one_responses, index, batchSize);
            if (redo)
                continue;
            if (phase_one_responses.size() >= getMajority()) {
                List<Integer> chosen_values = pickAllValues(phase_one_responses, requestBatch);
                runPhase2(phase_two_collector, buildPhaseTwoRequest(chosen_values, index));
                phase_two_collector.waitForTarget(getMajority());
                System.out.println("Number of responses: " + phase_two_collector.getReceived() + " Pending: " + phase_one_collector.getPending());
                redo = handlePhaseTwoResponses(phase_two_responses, index, chosen_values.size());
                if (redo)
                    continue;
                if (phase_two_responses.size() >= getMajority()) {
                    reached_consensus = true;
                    moveTransactionsToLog(chosen_values, index);
                    returnRequestsToQueue(requestBatch, chosen_values);
                    removePaxosRoundState(index);
                }
            }
        }
    }

    private void initPaxosRoundStatesAsLeader(int baseRound, int numRequests) {
        for (int i = 0; i < numRequests; i++) {
            int currentRound = baseRound + i;
            PaxosRoundState newRoundState = new PaxosRoundState();
            newRoundState.getTimestampState().setLeaderTs(my_id);
            if (!paxos_round_state_map.containsKey(currentRound))
                paxos_round_state_map.put(currentRound, newRoundState);
        }
    }

    private void returnRequestsToQueue(List<RequestQueueEntry> requestBatch, List<Integer> chosenValues) {
        //TODO: Confirm whether adding to the end of the queue is bad
        for (RequestQueueEntry requestQueueEntry : requestBatch) {
            if (chosenValues.stream().noneMatch(reqId -> reqId == requestQueueEntry.getReqid()))
                request_queue.offer(requestQueueEntry);
            if (requestQueueEntry.getTransactionRecord().isReconfigTransaction()) {
                try {
                    leader_lock.lock();
                    reconfig_condition.signal();
                } finally {
                    leader_lock.unlock();
                }
            }
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

    private boolean handlePhaseOneResponses(List<DadkvsPaxos.MultiPaxosPhaseOneResponse> phaseOneResponses, int index, int batchSize) {
        boolean rejected = false;
        for (int i = 0; i < batchSize; i++) {
            int currentIndex = index + i;
            int leader_ts = getLeaderTs(currentIndex);
            int largestTimestamp = leader_ts;
            for (DadkvsPaxos.MultiPaxosPhaseOneResponse multiPaxosResponse : phaseOneResponses) {
                // Update largest timestamp if response is not accepted
                DadkvsPaxos.PhaseOneReply responseForCurrentIndex = multiPaxosResponse.getResponse(i);
                if (!responseForCurrentIndex.getPhase1Accepted()) {
                    largestTimestamp = Math.max(largestTimestamp, responseForCurrentIndex.getPhase1Timestamp());
                }
                boolean hasGreaterLeader = largestTimestamp > leader_ts;
                if (hasGreaterLeader) {
                    updateLeaderTimestamp(largestTimestamp, index);
                }
                rejected = rejected || hasGreaterLeader;
            }
        }
        return rejected;
    }

    private boolean handlePhaseTwoResponses(List<DadkvsPaxos.MultiPaxosPhaseTwoResponse> phaseTwoResponses, int index, int batchSize) {
        boolean rejected = false;
        for (int i = 0; i < batchSize; i++) {
            int currentIndex = index + i;
            int leader_ts = getLeaderTs(currentIndex);
            int largestTimestamp = leader_ts;
            for (DadkvsPaxos.MultiPaxosPhaseTwoResponse multiPaxosResponse : phaseTwoResponses) {
                DadkvsPaxos.PhaseTwoReply responseForCurrentIndex = multiPaxosResponse.getResponse(i);
                if (!responseForCurrentIndex.getPhase2Accepted()) {
                    largestTimestamp = Math.max(largestTimestamp, responseForCurrentIndex.getPhase2Timestamp());
                }
                boolean hasGreaterLeader = largestTimestamp > leader_ts;
                if (hasGreaterLeader) {
                    updateLeaderTimestamp(largestTimestamp, index);
                }
                rejected = rejected || hasGreaterLeader;
            }
        }
        return rejected;
    }

    // Puts nulls in the log, so we can add transactions in their correct index (ArrayList)
    private void fillTransactionLog(int endIndex) {
        for (int i = transaction_execution_log.size(); i < endIndex; i++) {
            addTransactionToLog(null, i);
        }
    }

    private synchronized void addTransactionToLog(Integer reqid, int index) {
        if (transaction_execution_log.size() - 1 < index) {
            transaction_execution_log.add(reqid);
        } else if (transaction_execution_log.get(index) == null) {
            transaction_execution_log.set(index, reqid);
        }
    }

    public synchronized void moveTransactionsToLog(List<Integer> reqIds, int index) {
        System.out.println("Queue size when moving to log: " + request_queue.size());
        // Filling transaction log with nulls to accommodate the start of the received batch in the log.
        fillTransactionLog(index);
        for (int i = 0; i < reqIds.size(); i++) {
            moveTransactionToMap(reqIds.get(i), RequestState.PENDING_EXECUTION);
            addTransactionToLog(reqIds.get(i), index + i);
        }
    }

    /*public synchronized void moveTransactionToLog(int reqId, int index) {
        System.out.println("Queue size when moving to log: " + request_queue.size());
        moveTransactionToMap(reqId, RequestState.PENDING_EXECUTION);
        fillTransactionLog(index);
        addTransactionToLog(reqId, index);
    }*/

    public synchronized void moveTransactionToMap(int reqId, RequestState requestState) {
        RequestQueueEntry req = findAndRemoveFromQueue(reqId);
        if (req != null) {
            transaction_consensus_map.put(reqId, new PaxosRequestEntry(req.getTransactionRecord(), requestState));
        } else if (!transaction_consensus_map.containsKey(reqId)) {
            transaction_consensus_map.put(reqId, new PaxosRequestEntry());
        } else if (!transaction_consensus_map.get(reqId).getRequestState().equals(requestState)) {
            transaction_consensus_map.get(reqId).setRequestState(requestState);
        }
    }

    public RequestQueueEntry findAndRemoveFromQueue(int reqId) {
        for (RequestQueueEntry entry : request_queue) {
            if (entry.getReqid() == reqId)
                request_queue.remove(entry);
            return entry;
        }
        return null;
    }


    public DadkvsPaxos.MultiPaxosPhaseOneRequest buildPhaseOneRequest(int baseIndex, int batchSize) {
        DadkvsPaxos.MultiPaxosPhaseOneRequest.Builder multiPaxosRequest =
                DadkvsPaxos.MultiPaxosPhaseOneRequest.newBuilder();
        for (int i = 0; i < batchSize; i++) {
            DadkvsPaxos.PhaseOneRequest.Builder phase_one_request = DadkvsPaxos.PhaseOneRequest.newBuilder();
            int currentIndex = baseIndex + i;
            phase_one_request
                    .setPhase1Config(current_config)
                    .setPhase1Index(currentIndex)
                    .setPhase1Timestamp(getLeaderTs(baseIndex));

            multiPaxosRequest.addRequest(phase_one_request.build());
        }

        return multiPaxosRequest.build();
    }

    public DadkvsPaxos.MultiPaxosPhaseTwoRequest buildPhaseTwoRequest(List<Integer> values, int baseIndex) {
        DadkvsPaxos.MultiPaxosPhaseTwoRequest.Builder multiPaxosRequest =
                DadkvsPaxos.MultiPaxosPhaseTwoRequest.newBuilder();
        for (int i = 0; i < values.size(); i++) {
            int currentIndex = baseIndex + i;
            DadkvsPaxos.PhaseTwoRequest.Builder phase_two_request = DadkvsPaxos.PhaseTwoRequest.newBuilder();
            phase_two_request
                    .setPhase2Config(current_config)
                    .setPhase2Timestamp(getLeaderTs(currentIndex))
                    .setPhase2Index(currentIndex)
                    .setPhase2Value(values.get(i));

            multiPaxosRequest.addRequest(phase_two_request);
        }
        return multiPaxosRequest.build();
    }

    private void updateLeaderTimestamp(int response_ts, int index) {
        int currentLeaderTS = getLeaderTs(index);
        currentLeaderTS += (int) Math.ceil((double) (response_ts - currentLeaderTS) / total_num_servers) * total_num_servers;
        paxos_round_state_map.get(index).getTimestampState().setLeaderTs(currentLeaderTS);
    }

    public void addTransactionRecordToQueue(Integer reqid, TransactionRecord transactionRecord) {
        RequestQueueEntry request_queue_entry = new RequestQueueEntry(reqid, transactionRecord);
        boolean was_empty = request_queue.isEmpty();
        if (transaction_consensus_map.containsKey(reqid) && !transaction_consensus_map.get(reqid).transactionIsAvailable()) {
            try {
                execution_lock.lock();
                System.out.println("Signalling thread waiting for request: " + reqid);
                transaction_consensus_map.get(reqid).setTransactionRecord(transactionRecord);
                execution_condition.signal();
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

    private void runPhase1(GenericResponseCollector<DadkvsPaxos.MultiPaxosPhaseOneResponse> phase_one_collector, DadkvsPaxos.MultiPaxosPhaseOneRequest request) {
        for (int serverIndex : configuration_matrix[current_config]) {
            System.out.println("Leader sending phase 1 request: ");
            DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub stub = async_paxos_stubs[serverIndex];
            CollectorStreamObserver.printMessageFields(request);
            CollectorStreamObserver<DadkvsPaxos.MultiPaxosPhaseOneResponse> phase_one_observer =
                    new CollectorStreamObserver<>(phase_one_collector);
            stub.phaseone(request, phase_one_observer);
        }
    }

    private void runPhase2(GenericResponseCollector<DadkvsPaxos.MultiPaxosPhaseTwoResponse> phase_two_collector, DadkvsPaxos.MultiPaxosPhaseTwoRequest request) {
        for (int serverIndex : configuration_matrix[current_config]) {
            System.out.println("Leader sending phase 2 request:\n" + request);
            DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub stub = async_paxos_stubs[serverIndex];
            CollectorStreamObserver.printMessageFields(request);
            CollectorStreamObserver<DadkvsPaxos.MultiPaxosPhaseTwoResponse> phase_two_observer =
                    new CollectorStreamObserver<>(phase_two_collector);
            stub.phasetwo(request, phase_two_observer);
        }
    }

    private List<Integer> pickAllValues(List<DadkvsPaxos.MultiPaxosPhaseOneResponse> responses, List<RequestQueueEntry> requestBatch) {
        List<Integer> pickedValues = new ArrayList<>();
        for (int i = 0; i < requestBatch.size(); i++) {
            List<DadkvsPaxos.PhaseOneReply> currentIndexResponses = new ArrayList<>();
            for (DadkvsPaxos.MultiPaxosPhaseOneResponse response : responses) {
                currentIndexResponses.add(response.getResponse(i));
            }
            int pickedValue = pickValue(currentIndexResponses, requestBatch.get(i).getReqid());
            pickedValues.add(pickedValue);
            // If a reconfig is returned in a read we have to prune the batch we are proposing after the reconfig
            if(isReconfigByReqId(pickedValue))
                break;
        }
        return pickedValues;
    }

    private int pickValue(List<DadkvsPaxos.PhaseOneReply> responses, int reqId) {
        // Retrieving the response with the largest accepted timestamp
        int largestTimestamp = 0;
        int acceptedValue = 0;
        for (DadkvsPaxos.PhaseOneReply response : responses) {
            if (response.getPhase1Timestamp() > largestTimestamp) {
                largestTimestamp = response.getPhase1Timestamp();
                acceptedValue = response.getPhase1Value();
            }
        }
        return largestTimestamp > 0 ? acceptedValue : reqId;
    }

    private boolean isReconfigByReqId(int reqId){
        return reqId % 10 == 0;
    }

    public boolean transactionAvailable(int reqId) {
        return transaction_consensus_map.containsKey(reqId) && transaction_consensus_map.get(reqId).transactionIsAvailable();
    }

    public boolean previousTransactionComplete(int index) {
        System.out.println("Checking previous transaction for index: " + index);
        if (index == 0)
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

    public int getValueFromLog(int index) {
        return index < transaction_execution_log.size() &&
                transaction_execution_log.get(index) != null ? transaction_execution_log.get(index) : -1;
    }

    public boolean isIndexEmpty(int index) {
        return index > transaction_execution_log.size() - 1 || transaction_execution_log.get(index) == null;
    }

    public void removePaxosRoundState(int index) {
        if (paxos_round_state_map.get(index) == null)
            return;
        Lock paxosStateLock = paxos_round_state_map.get(index).getPaxosStateLock();
        try {
            paxosStateLock.lock();
            paxos_round_state_map.get(index).getPaxosStateLock().lock();
            paxos_round_state_map.remove(index);
        } finally {
            paxosStateLock.unlock();
        }
    }

    public void addAcceptedValue(int index, int value) {
        paxos_round_state_map.get(index).setUncommitedAcceptedRequest(value);
    }

    public synchronized int updateIndex() {
        current_index = findNextFreeIndex(current_index + 1);
        return current_index;
    }

    private int findNextFreeIndex(int i) {
        while (i < transaction_execution_log.size() && transaction_execution_log.get(i) != null) {
            i++;
        }
        return i;
    }

    public int getUncommitedConsensusAccept(int index) {
        PaxosRoundState paxosRoundState = paxos_round_state_map.get(index);
        return paxosRoundState != null ? paxosRoundState.getUncommitedAcceptedRequest() : -1;
    }

    private boolean isReconfig(int index) {
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

    public synchronized void loadConfiguration(int index) {
        // If we find a request that is completed before we find an incomplete reconfiguration, we can set the current configuration to what is in key 0
        // else if we find an incomplete reconfiguration, we set the current configuration to the respective configuration
        for (int i = index; i > 0; i--) {
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
        if (!isPartOfConfig(my_id, configuration_matrix[current_config])) {
            i_am_leader = false;
        }
    }

    public TimestampState getTimestampState(int round) {
        return paxos_round_state_map.get(round) != null ? paxos_round_state_map.get(round).getTimestampState() : null;
    }

    public int getTimestamp(int reqIndex, TimestampEnum timestampType) {
        TimestampState timestampState = paxos_round_state_map.get(reqIndex).getTimestampState();
        if (timestampState == null) {
            return -1;
        }
        return switch (timestampType) {
            case PREPARE -> timestampState.getLargestPrepareTs();
            case ACCEPT -> timestampState.getLargestAcceptTs();
            case LEADER -> timestampState.getLeaderTs();
            default -> -1;
        };
    }

    private int getLeaderTs(int round) {
        return paxos_round_state_map.get(round).getTimestampState().getLeaderTs();
    }

    private void executor() {
        int currentIndexWithRequestToExecute = findNextIndexWithRequestToExecute(0);
        while (true) {
            try {
                execution_lock.lock();
                if (isIndexEmpty(currentIndexWithRequestToExecute) ||
                        !getRequestEntryFromIndex(currentIndexWithRequestToExecute).transactionIsAvailable()) {
                    execution_condition.await();
                } else {
                    int reqId = getValueFromLog(currentIndexWithRequestToExecute);
                    System.out.println("Executing Request: " + reqId);
                    PaxosRequestEntry paxosRequestEntry = transaction_consensus_map.get(reqId);
                    TransactionRecord transaction = paxosRequestEntry.getTransactionRecord();
                    transaction.setTimestamp(currentIndexWithRequestToExecute);
                    boolean commitSuccessful = store.commit(transaction);
                    if (commitSuccessful) {
                        paxosRequestEntry.setCommited();
                    } else {
                        paxosRequestEntry.setAborted();
                    }
                    completeClientRequest(reqId, commitSuccessful);

                    if (transaction.isReconfigTransaction()) {
                        try {
                            leader_lock.lock();
                            reconfig_condition.signal();
                        } finally {
                            leader_lock.unlock();
                        }
                    }
                    currentIndexWithRequestToExecute = findNextIndexWithRequestToExecute(currentIndexWithRequestToExecute);
                }
            } catch (InterruptedException e) {
                System.out.println("Thread interrupted");
            } finally {
                execution_lock.unlock();
            }
        }
    }

    public PaxosRequestEntry getRequestEntryFromIndex(int index) {
        int reqId = getValueFromLog(index);
        return reqId != -1 ? transaction_consensus_map.get(reqId) : null;
    }

    private int findNextIndexWithRequestToExecute(int currentIndex) {
        int requestInCurrentIndex = getValueFromLog(currentIndex);
        while (requestInCurrentIndex != -1 && transaction_consensus_map.get(requestInCurrentIndex).hasCompleted()) {
            requestInCurrentIndex = getValueFromLog(++currentIndex);
        }
        return currentIndex;
    }

}
