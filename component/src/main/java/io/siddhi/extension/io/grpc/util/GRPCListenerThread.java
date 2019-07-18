package io.siddhi.extension.io.grpc.util;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.extension.io.grpc.util.service.SequenceCallResponse;

public class GRPCListenerThread implements Runnable {
    private SourceEventListener sourceEventListener;
    private ResponseStaticHolder responseStaticHolder = ResponseStaticHolder.getInstance();

    public GRPCListenerThread(SourceEventListener sourceEventListener) {
        this.sourceEventListener = sourceEventListener;
    }

    @Override
    public void run() {
        ListenableFuture listenableFuture = responseStaticHolder.getListenableFuture("InvokeSequence:CallSequenceWithResponse:mySeq");
        if (listenableFuture != null) {
            Futures.addCallback(listenableFuture, new FutureCallback<SequenceCallResponse>() {
                @Override
                public void onSuccess(SequenceCallResponse result) {
                    String response = result.getResponseAsJSON();
                    sourceEventListener.onEvent(new Object[]{response}, new String[]{"1"});
                    System.out.println("Success! from source");
                }

                @Override
                public void onFailure(Throwable t) {
                    System.out.println("Failure");
                    throw new SiddhiAppRuntimeException(t.getMessage());
                }
            });
            responseStaticHolder.removeListenableFuture("InvokeSequence:CallSequenceWithResponse:mySeq");
        }
    }
}
