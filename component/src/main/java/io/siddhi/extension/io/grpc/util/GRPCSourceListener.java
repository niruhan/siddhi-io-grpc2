package io.siddhi.extension.io.grpc.util;

import io.siddhi.core.stream.input.source.SourceEventListener;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class GRPCSourceListener {
    protected ExecutorService executorService;
    protected SourceEventListener sourceEventListener;

    public GRPCSourceListener(int workerThread, SourceEventListener sourceEventListener) {
        this.executorService = Executors.newFixedThreadPool(workerThread);
        this.sourceEventListener = sourceEventListener;
    }
}
