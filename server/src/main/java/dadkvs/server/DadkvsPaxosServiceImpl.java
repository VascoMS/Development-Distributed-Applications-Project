package dadkvs.server;


import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.locks.Lock;

public class DadkvsPaxosServiceImpl extends DadkvsPaxosServiceGrpc.DadkvsPaxosServiceImplBase {

    DadkvsServerState server_state;

    LearnHandler learnHandler;

    DebugHandler debugHandler;

    public DadkvsPaxosServiceImpl(DadkvsServerState state, DebugHandler debugHandler) {
        this.server_state = state;
        this.learnHandler = new LearnHandler(state);
        this.debugHandler = debugHandler;
    }

    @Override
    public void phaseone(DadkvsPaxos.MultiPaxosPhaseOneRequest request, StreamObserver<DadkvsPaxos.MultiPaxosPhaseOneResponse> responseObserver) {
        // for debug purposes
        System.out.println("Receive phase1 request: " + request);
        debugHandler.runDebug(server_state.debug_mode, false);
        DadkvsPaxos.MultiPaxosPhaseOneResponse.Builder multiPaxosPhaseOneResponse =
                DadkvsPaxos.MultiPaxosPhaseOneResponse.newBuilder();
        for(DadkvsPaxos.PhaseOneRequest singleRoundRequest : request.getRequestList()){
            DadkvsPaxos.PhaseOneReply response = processPhaseOneRequest(singleRoundRequest);
            multiPaxosPhaseOneResponse.addResponse(response);
        }
        responseObserver.onNext(multiPaxosPhaseOneResponse.build());
        responseObserver.onCompleted();
    }

    private DadkvsPaxos.PhaseOneReply processPhaseOneRequest(DadkvsPaxos.PhaseOneRequest request) {
        DadkvsPaxos.PhaseOneReply phase_one_response;
        int reqIndex = request.getPhase1Index();
        int reqTS = request.getPhase1Timestamp();
        PaxosRoundState paxosRoundState = getOrInitPaxosRoundState(reqIndex);
        Lock paxosRoundLock = paxosRoundState.getPaxosStateLock();
        try {
            paxosRoundLock.lock();
            if (isOlderTS(reqTS, reqIndex)) {
                phase_one_response = build_phase_one_response(false, server_state.getTimestamp(reqIndex, TimestampEnum.PREPARE),
                        -1, reqIndex);
            } else {
                int request_to_send;
                updatePrepareTimestampState(reqIndex, reqTS);
                if (!server_state.isIndexEmpty(request.getPhase1Index()))
                    request_to_send = server_state.getValueFromLog(request.getPhase1Index());
                else
                    request_to_send = server_state.getUncommitedConsensusAccept(request.getPhase1Index());

                int ts = request_to_send != -1 ? server_state.getTimestamp(reqIndex, TimestampEnum.ACCEPT) : -1;
                phase_one_response = build_phase_one_response(true, ts, request_to_send, reqIndex);
            }
        } finally {
            paxosRoundLock.unlock();
        }

        return phase_one_response;
    }

    private synchronized PaxosRoundState getOrInitPaxosRoundState(int round) {
        if (!server_state.paxos_round_state_map.containsKey(round)) {
            server_state.paxos_round_state_map.put(round, new PaxosRoundState());
        }
        return server_state.paxos_round_state_map.get(round);
    }

    private void updatePrepareTimestampState(int index, int newPrepareTS) {
        TimestampState tsState = server_state.getTimestampState(index);
        if (tsState == null) {
            tsState = new TimestampState(server_state.my_id);
            PaxosRoundState newPaxosRoundState = new PaxosRoundState();
            newPaxosRoundState.setTimestampState(tsState);

            server_state.paxos_round_state_map.put(index, newPaxosRoundState);
        }
        tsState.setLargestPrepareTs(newPrepareTS);
    }

    private void updateAcceptTimestampState(int index, int newAcceptTS) {
        TimestampState tsState = server_state.getTimestampState(index);
        if (tsState == null) {
            tsState = new TimestampState(server_state.my_id);
            PaxosRoundState newPaxosRoundState = new PaxosRoundState();
            newPaxosRoundState.setTimestampState(tsState);

            server_state.paxos_round_state_map.put(index, newPaxosRoundState);
        }
        tsState.setLargestAcceptTs(newAcceptTS);
    }

    private boolean isOlderTS(int ts, int index) {
        //turned Integer into int
        int largestPrepareTS = server_state.getTimestamp(index, TimestampEnum.PREPARE);
        int largestAcceptTS = server_state.getTimestamp(index, TimestampEnum.ACCEPT);

        //Cuz its an int, it can only be -1
        return (largestPrepareTS != -1 && ts < largestPrepareTS) || (largestAcceptTS != -1 && ts < largestAcceptTS);

    }


    private DadkvsPaxos.PhaseOneReply build_phase_one_response(boolean accepted, int ts, int value, int index) {
        DadkvsPaxos.PhaseOneReply.Builder phase_one_response_builder = DadkvsPaxos.PhaseOneReply.newBuilder();
        return phase_one_response_builder
                .setPhase1Accepted(accepted)
                .setPhase1Timestamp(ts)
                .setPhase1Value(value)
                .setPhase1Index(index)
                .build();
    }

    @Override
    public void phasetwo(DadkvsPaxos.MultiPaxosPhaseTwoRequest request, StreamObserver<DadkvsPaxos.MultiPaxosPhaseTwoResponse> responseObserver) {
        // for debug purposes
        System.out.println("Receive phase two request: " + request);
        debugHandler.runDebug(server_state.debug_mode, false);
        DadkvsPaxos.MultiPaxosPhaseTwoResponse.Builder multiPaxosPhaseTwoResponse = DadkvsPaxos.MultiPaxosPhaseTwoResponse.newBuilder();
        boolean fullBatchAccepted = true;
        for(DadkvsPaxos.PhaseTwoRequest singleRoundRequest : request.getRequestList()) {
            DadkvsPaxos.PhaseTwoReply response = processPhaseTwoRequest(singleRoundRequest);
            fullBatchAccepted = fullBatchAccepted && response.getPhase2Accepted();
            multiPaxosPhaseTwoResponse.addResponse(response);
        }
        Context forkedContext = Context.current().fork();
        if(fullBatchAccepted){
            request.getRequestList().forEach(phaseTwoRequest -> {
                int index = phaseTwoRequest.getPhase2Index();
                int ts = phaseTwoRequest.getPhase2Timestamp();
                int value = phaseTwoRequest.getPhase2Value();
                updateAcceptTimestampState(index, ts);
                server_state.addAcceptedValue(index, value);
                forkedContext.run(() -> {
                    server_state.sendLearnRequests(phaseTwoRequest);
                });
            });
        }
        responseObserver.onNext(multiPaxosPhaseTwoResponse.build());
        responseObserver.onCompleted();
    }

    private DadkvsPaxos.PhaseTwoReply processPhaseTwoRequest(DadkvsPaxos.PhaseTwoRequest request){
        DadkvsPaxos.PhaseTwoReply.Builder phase_two_response = DadkvsPaxos.PhaseTwoReply.newBuilder();
        int reqIndex = request.getPhase2Index();
        int reqTS = request.getPhase2Timestamp();
        PaxosRoundState paxosRoundState = getOrInitPaxosRoundState(reqIndex);
        Lock paxosRoundLock = paxosRoundState.getPaxosStateLock();
        try {
            paxosRoundLock.lock();
            if (isOlderTS(reqTS, reqIndex)) {
                phase_two_response
                        .setPhase2Accepted(false)
                        .setPhase2Index(request.getPhase2Index())
                        .setPhase2Timestamp(server_state.getTimestamp(reqIndex, TimestampEnum.PREPARE));
            } else {
                System.out.println("Accepting reqId: " + request.getPhase2Value() + " at index: " + reqIndex);
                phase_two_response
                        .setPhase2Accepted(true)
                        .setPhase2Index(reqIndex);
            }
        } finally {
            paxosRoundLock.unlock();
        }
        return phase_two_response.build();
    }

    @Override
    public void learn(DadkvsPaxos.LearnRequest request, StreamObserver<DadkvsPaxos.LearnReply> responseObserver) {
        // for debug purposes
        System.out.println("Receive learn request: " + request);
        debugHandler.runDebug(server_state.debug_mode, false);
        learnHandler.handleLearnRequest(request);
        responseObserver.onNext(DadkvsPaxos.LearnReply.getDefaultInstance());
        responseObserver.onCompleted();
    }

}
