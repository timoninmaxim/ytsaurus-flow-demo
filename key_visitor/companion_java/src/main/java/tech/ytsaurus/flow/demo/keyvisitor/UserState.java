package tech.ytsaurus.flow.demo.keyvisitor;

import javax.persistence.Entity;

/**
 * Per-key internal state of the visit tester: the last stored payload and a monotonically
 * increasing visit counter. Serialized as binary YSON by the SDK's {@code @Entity} codec.
 */
@Entity
public class UserState {
    private String payload;
    private long visitIndex;

    public UserState() {
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public long getVisitIndex() {
        return visitIndex;
    }

    public void setVisitIndex(long visitIndex) {
        this.visitIndex = visitIndex;
    }
}
