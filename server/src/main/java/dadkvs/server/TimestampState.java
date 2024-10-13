package dadkvs.server;

public class TimestampState {
    private int leaderTs;          // Leader timestamp
    private int largestPrepareTs;  // Largest prepare timestamp seen
    private int largestAcceptTs;

    public TimestampState(int leaderTs) {
        this.leaderTs = leaderTs;
        this.largestPrepareTs = 0;
        this.largestAcceptTs = 0;
    }

    public int getLeaderTs() {
        return leaderTs;
    }

    public void setLeaderTs(int leaderTs) {
        this.leaderTs = leaderTs;
    }

    public int getLargestPrepareTs() {
        return largestPrepareTs;
    }

    public void setLargestPrepareTs(int largestPrepareTs) {
        this.largestPrepareTs = largestPrepareTs;
    }

    public int getLargestAcceptTs() {
        return largestAcceptTs;
    }

    public void setLargestAcceptTs(int largestAcceptTs) {
        this.largestAcceptTs = largestAcceptTs;
    }
}
