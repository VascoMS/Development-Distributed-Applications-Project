package dadkvs.server;


import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
    public void phaseone(DadkvsPaxos.PhaseOneRequest request, StreamObserver<DadkvsPaxos.PhaseOneReply> responseObserver) {
        // for debug purposes
        System.out.println("Receive phase1 request: " + request);
        debugHandler.runDebug(server_state.debug_mode, false);
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
        responseObserver.onNext(phase_one_response);
        responseObserver.onCompleted();
    }

    private synchronized PaxosRoundState getOrInitPaxosRoundState(int round){
        if(!server_state.paxos_round_state_map.containsKey(round)){
            server_state.paxos_round_state_map.put(round, new PaxosRoundState());
        }
        return server_state.paxos_round_state_map.get(round);
    }



    private void updatePrepareTimestampState(int index, int newPrepareTS) {
        TimestampState tsState = server_state.timestamp_state_map.get(index);
        if(tsState == null){
            tsState = new TimestampState(server_state.my_id);
            server_state.timestamp_state_map.put(index, tsState);
        }
        tsState.setLargestPrepareTs(newPrepareTS);
    }

    private void updateAcceptTimestampState(int index, int newAcceptTS) {
        TimestampState tsState = server_state.timestamp_state_map.get(index);
        if(tsState == null){
            tsState = new TimestampState(server_state.my_id);
            server_state.timestamp_state_map.put(index, tsState);
        }
        tsState.setLargestPrepareTs(newAcceptTS);
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
    public void phasetwo(DadkvsPaxos.PhaseTwoRequest request, StreamObserver<DadkvsPaxos.PhaseTwoReply> responseObserver) {
        // for debug purposes
        System.out.println("Receive phase two request: " + request);
        debugHandler.runDebug(server_state.debug_mode, false);
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
                phase_two_response.setPhase2Accepted(true);
                updateAcceptTimestampState(reqIndex, reqTS);
                server_state.addAcceptedValue(reqIndex, request.getPhase2Value());
                Context forkedContext = Context.current().fork();
                forkedContext.run(() -> {
                    server_state.sendLearnRequests(reqIndex, request.getPhase2Value(), reqTS);
                });
            }
        } finally {
            paxosRoundLock.unlock();
        }
        responseObserver.onNext(phase_two_response.build());
        responseObserver.onCompleted();
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
