package tech.ytsaurus.flow.demo.transformhighthroughput;

import javax.persistence.Column;
import javax.persistence.Entity;

/**
 * Per-key internal state of the reducer, mirroring the C++ variant's state value — a YSON map
 * {@code {count; last_data}}. Serialized as binary YSON by the SDK's {@code @Entity} codec, so
 * in the pipeline's built-in {@code states} table it lands as an opaque payload rather than the
 * C++ variant's structured map (exactly as the Go and Python variants' states do).
 */
@Entity
public class ReducerState {
    @Column(name = "count")
    private long count;

    @Column(name = "last_data")
    private String lastData;

    public ReducerState() {
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public String getLastData() {
        return lastData;
    }

    public void setLastData(String lastData) {
        this.lastData = lastData;
    }
}
