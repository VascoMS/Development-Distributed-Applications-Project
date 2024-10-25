package dadkvs.server;

public enum RequestState {
    AWAITING_PROPOSAL, PENDING_EXECUTION, ABORTED, COMMITTED;

    public boolean isGreaterThan(RequestState other) {
        return this.ordinal() > other.ordinal();
    }
}
