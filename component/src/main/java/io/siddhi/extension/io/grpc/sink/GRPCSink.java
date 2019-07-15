/*
 * Copyright (c)  2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.siddhi.extension.io.grpc.sink;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.*;
import io.grpc.stub.ClientCalls;
import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.output.sink.Sink;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.grpc.util.GRPCStubHolder;
import io.siddhi.extension.io.grpc.util.GrpcBlockingStub;
import io.siddhi.extension.io.grpc.util.GRPCService.Request;
import io.siddhi.extension.io.grpc.util.GRPCService.EmptyResponse;
import io.siddhi.extension.io.grpc.util.Message;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This is a sample class-level comment, explaining what the extension class does.
 */
@Extension(
        name = "grpc",
        namespace = "sink",
        description = " ",
        parameters = {
                /*@Parameter(name = " ",
                        description = " " ,
                        dynamic = false/true,
                        optional = true/false, defaultValue = " ",
                        type = {DataType.INT, DataType.BOOL, DataType.STRING, DataType.DOUBLE,etc }),
                        type = {DataType.INT, DataType.BOOL, DataType.STRING, DataType.DOUBLE, }),*/
        },
        examples = {
                @Example(
                        syntax = " ",
                        description = " "
                )
        }
)

// for more information refer https://siddhi-io.github.io/siddhi/documentation/siddhi-4.x/query-guide-4.x/#sink

public class GRPCSink extends Sink {
    private static final Logger logger = Logger.getLogger(GRPCSink.class.getName());
    private GrpcBlockingStub blockingStub;
//    private InvokeSequenceStub asyncStub;
    private SiddhiAppContext siddhiAppContext;
    private Random random = new Random();
    private Channel channel;
    private String serviceName;

    /**
     * Returns the list of classes which this sink can consume.
     * Based on the type of the sink, it may be limited to being able to publish specific type of classes.
     * For example, a sink of type file can only write objects of type String .
     * @return array of supported classes , if extension can support of any types of classes
     * then return empty array .
     */
    @Override
    public Class[] getSupportedInputEventClasses() {
            return new Class[0];
    }

    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return null;
    }

    /**
     * Returns a list of supported dynamic options (that means for each event value of the option can change) by
     * the transport
     *
     * @return the list of supported dynamic option keys
     */
    @Override
    public String[] getSupportedDynamicOptions() {
            return new String[0];
    }

    /**
     * The initialization method for {@link Sink}, will be called before other methods. It used to validate
     * all configurations and to get initial values.
     * @param streamDefinition  containing stream definition bind to the {@link Sink}
     * @param optionHolder            Option holder containing static and dynamic configuration related
     *                                to the {@link Sink}
     * @param configReader        to read the sink related system configuration.
     * @param siddhiAppContext        the context of the {@link io.siddhi.query.api.SiddhiApp} used to
     */
    @Override
    protected StateFactory init(StreamDefinition streamDefinition, OptionHolder optionHolder, ConfigReader configReader,
                                SiddhiAppContext siddhiAppContext) {
        this.siddhiAppContext = siddhiAppContext;
        this.serviceName = ""; //todo: get from options holder

        channel = ManagedChannelBuilder.forTarget("dns:///localhost:" + 8000)
                .usePlaintext(true)
                .build();

        blockingStub = new GrpcBlockingStub(channel);
        GRPCStubHolder.getInstance().setBlockingStub(blockingStub);

        return null;
    }

    @Override
    public void publish(Object payload, DynamicOptions dynamicOptions, State state) throws ConnectionUnavailableException {
        ClientCall<Request, EmptyResponse> call = channel.newCall(CREATE_METHOD, CallOptions.DEFAULT);
        Request request = new Request();
        ListenableFuture<EmptyResponse> res = ClientCalls.futureUnaryCall(call, request);
        res.addListener(() ->  { }, MoreExecutors.directExecutor());

        Futures.addCallback(res, new FutureCallback<EmptyResponse>() {
            @Override
            public void onSuccess(EmptyResponse result) {
                System.out.println("Success!");
            }

            @Override
            public void onFailure(Throwable t) {
                System.out.println("Failure");
            }
        });
    }

    MethodDescriptor<Request, EmptyResponse> CREATE_METHOD =
            MethodDescriptor.newBuilder(
                    marshallerFor(Request.class),
                    marshallerFor(EmptyResponse.class))
                    .setFullMethodName(
                            MethodDescriptor.generateFullMethodName(serviceName, "Create"))
                    .setType(MethodDescriptor.MethodType.UNARY)
                    .setSampledToLocalTracing(true)
                    .build();

    <T> MethodDescriptor.Marshaller<T> marshallerFor(Class<T> clz) {
        return new MethodDescriptor.Marshaller<T>() {
            @Override
            public InputStream stream(T value) {
                return new ByteArrayInputStream((byte[]) value);
            }

            @Override
            public T parse(InputStream stream) {
                return null; //gson.fromJson(new InputStreamReader(stream, StandardCharsets.UTF_8), clz);
                //todo: find way to get byte[] from inputstream
            }
        };
    }

    private String generateJSONStringFromObjectPayload(Object payload) {
        return null; //todo: implement function
    }

    /**
     * This method will be called before the processing method.
     * Intention to establish connection to publish event.
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     */
    @Override
    public void connect() throws ConnectionUnavailableException {

    }

    /**
     * Called after all publishing is done, or when {@link ConnectionUnavailableException} is thrown
     * Implementation of this method should contain the steps needed to disconnect from the sink.
     */
    @Override
    public void disconnect() {

    }

    /**
     * The method can be called when removing an event receiver.
     * The cleanups that have to be done after removing the receiver could be done here.
     */
    @Override
    public void destroy() {

    }

    @Override
    public void shutdown() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new SiddhiAppRuntimeException(siddhiAppContext.getName() + ": " + e.getMessage());
        }
        super.shutdown();
    }
}
