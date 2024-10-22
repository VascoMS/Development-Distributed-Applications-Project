package dadkvs.server;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PaxosRoundState {
    public static final int NO_UNCOMMITED_ACCEPTED_REQUEST = -1;
    private TimestampState timestampState;
    private Lock paxosStateLock;
    private int uncommitedAcceptedRequest;

    public PaxosRoundState() {
        timestampState = new TimestampState();
        paxosStateLock = new ReentrantLock();
        uncommitedAcceptedRequest = NO_UNCOMMITED_ACCEPTED_REQUEST;
    }
    public TimestampState getTimestampState() {
        return timestampState;
    }

    public void setTimestampState(TimestampState timestampState) {
        this.timestampState = timestampState;
    }

    public void setPaxosStateLock(Lock paxosStateLock) {
        this.paxosStateLock = paxosStateLock;
    }

    public void setUncommitedAcceptedRequest(int uncommitedAcceptedRequest) {
        this.uncommitedAcceptedRequest = uncommitedAcceptedRequest;
    }

    public Lock getPaxosStateLock() {
        return paxosStateLock;
    }

    public int getUncommitedAcceptedRequest() {
        return uncommitedAcceptedRequest;
    }

}


