package io.siddhi.extension.io.grpc.sink;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.extension.io.grpc.TestService;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class TestCaseOfGrpcSink {
    private static final Logger log = Logger.getLogger(TestCaseOfGrpcSink.class);
    private Server server;

    @Test
    public void testHTTPRequestResponse1() throws Exception {
        log.info("Send a POST request with a json body message and receive the response");
        SiddhiManager siddhiManager = new SiddhiManager();

        startServer();
        String payload =  String.valueOf(server.getPort());
        String inStreamDefinition = ""
                + "@sink(type='grpc', port = \'" + payload +"\') "
                + "define stream FooStream (message String);";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition);
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");


        System.out.println(server.getPort());
        try {
            siddhiAppRuntime.start();

            fooStream.send(new Object[]{payload});

            Thread.sleep(5000);
            siddhiAppRuntime.shutdown();
        } finally {
            stopServer();
        }
    }

    private void startServer() throws IOException {
        if (server != null) {
            throw new IllegalStateException("Already started");
        }
        server = ServerBuilder.forPort(0).addService(new TestService()).build();
        server.start();
    }

    private void stopServer() throws InterruptedException {
        Server s = server;
        if (s == null) {
            throw new IllegalStateException("Already stopped");
        }
        server = null;
        s.shutdown();
        if (s.awaitTermination(1, TimeUnit.SECONDS)) {
            return;
        }
        s.shutdownNow();
        if (s.awaitTermination(1, TimeUnit.SECONDS)) {
            return;
        }
        throw new RuntimeException("Unable to shutdown server");
    }
}
