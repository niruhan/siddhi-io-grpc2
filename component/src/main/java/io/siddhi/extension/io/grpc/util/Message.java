/*
 *  Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.siddhi.extension.io.grpc.util;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.WireFormat;
import io.netty.handler.codec.http.HttpHeaders;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.query.api.definition.Attribute.Type;
import org.ballerinalang.jvm.BallerinaValues;
import org.ballerinalang.jvm.types.BArrayType;
import org.ballerinalang.jvm.types.BField;
import org.ballerinalang.jvm.types.BRecordType;
import org.ballerinalang.jvm.types.BType;
import org.ballerinalang.jvm.types.BTypes;
import org.ballerinalang.jvm.values.ArrayValue;
import org.ballerinalang.jvm.values.MapValue;
import org.ballerinalang.net.grpc.exception.UnsupportedFieldTypeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.siddhi.extension.io.grpc.util.MessageUtils.toCamelCase;

//import static org.ballerinalang.net.grpc.builder.utils.BalGenerationUtils.toCamelCase;

/**
 * Generic Proto3 Message.
 *
 * @since 1.0.0
 */
public class Message {
    private String messageName;
    private int memoizedSize = -1;
    private HttpHeaders headers;
    private Object bMessage = null;
    private Descriptors.Descriptor descriptor = null;

    private boolean isError = false;
    private Throwable error;

    public Message(String messageName, Object bMessage) {
        this.messageName = messageName;
        this.bMessage = bMessage;
        this.descriptor = MessageRegistry.getInstance().getMessageDescriptor(messageName);
    }

    private Message(String messageName) {
        this.messageName = messageName;
    }

    public HttpHeaders getHeaders() {
        return headers;
    }

    public void setHeaders(HttpHeaders headers) {
        this.headers = headers;
    }

    public boolean isError() {
        return isError;
    }

    public Throwable getError() {
        return error;
    }

    public Object getbMessage() {
        return bMessage;
    }

    public Message(Throwable error) {
        this.error = error;
        this.isError = true;
    }

    public Message(
            String messageName,
            Type siddhiAttributeType,
            com.google.protobuf.CodedInputStream input,
            Map<Integer, Descriptors.FieldDescriptor> fieldDescriptors)
            throws IOException {
        this(messageName);
        Map<String, Object> bMapValue = null;
        Map<String, Type> bMapFields = new HashMap<>();
//        if (siddhiAttributeType instanceof BRecordType) {
//            bMapValue = BallerinaValues.createRecordValue(siddhiAttributeType.getPackage().getName(), siddhiAttributeType.getName());
//            for (BField messageField : ((BRecordType) siddhiAttributeType).getFields().values()) {
//                bMapFields.put(messageField.getFieldName(), messageField.getFieldType());
//            }
//            bMessage = bMapValue;
//        }
        boolean done = false;
        while (!done) {
            int tag = input.readTag();
            if (tag == 0) {
                done = true;
            } else if (fieldDescriptors.containsKey(tag)) {
                Descriptors.FieldDescriptor fieldDescriptor = fieldDescriptors.get(tag);
                String name = fieldDescriptor.getName();
                switch (fieldDescriptor.getType().toProto().getNumber()) {

                    case DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING_VALUE: {
                        if (bMapValue != null) {
                            if (fieldDescriptor.isRepeated()) {
                                List stringArray = new ArrayList(Type.STRING);
                                if (bMapValue.containsKey(name)) {
                                    stringArray = (ArrayValue) bMapValue.get(name);
                                }
                                stringArray.add(stringArray.size(), input.readStringRequireUtf8());
                                bMapValue.put(name, stringArray);
                            } else if (fieldDescriptor.getContainingOneof() != null) {
                                updateBMapValue(siddhiAttributeType, bMapValue, fieldDescriptor, input.readStringRequireUtf8());
                            } else {
                                bMapValue.put(name, input.readStringRequireUtf8());
                            }
                        } else {
                            bMessage = input.readStringRequireUtf8();
                        }
                        break;
                    }
                    default: {
                        throw new SiddhiAppCreationException("Error while decoding request message. Field " +
                                "type is not supported : " + fieldDescriptor.getType());
                    }
                }
            }
        }
    }

    private void updateBMapValue(Type siddhiAttributeType, Map<String, Object> bMapValue,
                                 Descriptors.FieldDescriptor fieldDescriptor, Object bValue) {
        Map<String, Object> bMsg = getOneOfBValue(siddhiAttributeType, fieldDescriptor, bValue);
        bMapValue.put(fieldDescriptor.getContainingOneof().getName(), bMsg);
    }

    private Map<String, Object> getOneOfBValue(Type siddhiAttributeName, Descriptors.FieldDescriptor fieldDescriptor,
                                                    Object bValue) {
        Descriptors.OneofDescriptor oneofDescriptor = fieldDescriptor.getContainingOneof();
        String msgType = oneofDescriptor.getContainingType().getName() + "_" + toCamelCase
                (fieldDescriptor.getName());
        Map<String, Object> bMsg = BallerinaValues.createRecordValue(siddhiAttributeName.getPackage().getName(), msgType);
        bMsg.put(fieldDescriptor.getName(), bValue);
        return bMsg;
    }

    public com.google.protobuf.Descriptors.Descriptor getDescriptor() {
        if (descriptor != null) {
            return descriptor;
        }
        return MessageRegistry.getInstance().getMessageDescriptor(messageName);
    }

    @SuppressWarnings("unchecked")
    void writeTo(com.google.protobuf.CodedOutputStream output)
            throws java.io.IOException {
        if (bMessage == null) {
            return;
        }
        Descriptors.Descriptor messageDescriptor = getDescriptor();
        Map<String, Object> bMapValue = null;
        if (bMessage instanceof Map) {
            bMapValue = (Map) bMessage;
        }
        for (Descriptors.FieldDescriptor fieldDescriptor : messageDescriptor.getFields()) {
            switch (fieldDescriptor.getType().toProto().getNumber()) {

                case DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING_VALUE: {
                    if (bMapValue != null && bMapValue.containsKey(fieldDescriptor.getName())) {
                        Object bValue = bMapValue.get(fieldDescriptor.getName());
                        if (bValue instanceof ArrayList) {
                            ArrayValue valueArray = (ArrayValue) bValue;
                            for (int i = 0; i < valueArray.size(); i++) {
                                output.writeString(fieldDescriptor.getNumber(), valueArray.getString(i));
                            }
                        } else {
                            output.writeString(fieldDescriptor.getNumber(), (String) bValue);
                        }
                    } else if (isOneofField(bMapValue, fieldDescriptor)) {
                        Object bValue = getOneofFieldMap(bMapValue, fieldDescriptor);
                        if (hasOneofFieldValue(fieldDescriptor.getName(), bValue)) {
                            output.writeString(fieldDescriptor.getNumber(), (String) ((MapValue) bValue)
                                    .get(fieldDescriptor.getName()));
                        }
                    } else if (bMessage instanceof String) {
                        output.writeString(fieldDescriptor.getNumber(), (String) bMessage);
                    }
                    break;
                }
                default: {
                    throw new SiddhiAppCreationException("Error while writing output stream. Field " +
                            "type is not supported : " + fieldDescriptor.getType());
                }
            }
        }
    }

    private Object getOneofFieldMap(MapValue<String, Object> bMapValue, Descriptors.FieldDescriptor fieldDescriptor) {
        Descriptors.OneofDescriptor oneofDescriptor = fieldDescriptor.getContainingOneof();
        return bMapValue.get(oneofDescriptor.getName());
    }

    private boolean isOneofField(MapValue<String, Object> bMapValue, Descriptors.FieldDescriptor fieldDescriptor) {
        return bMapValue != null && fieldDescriptor.getContainingOneof() != null;
    }

    @SuppressWarnings("unchecked")
    public int getSerializedSize() {
        int size = memoizedSize;
        if (size != -1) {
            return size;
        }
        size = 0;
        if (bMessage == null) {
            memoizedSize = size;
            return size;
        }
        Descriptors.Descriptor messageDescriptor = getDescriptor();
        if (messageDescriptor == null) {
            throw Status.Code.INTERNAL.toStatus()
                    .withDescription("Error while processing the message, Couldn't find message descriptor.")
                    .asRuntimeException();
        }
        MapValue<String, Object> bMapValue = null;
        if (bMessage instanceof MapValue) {
            bMapValue = (MapValue<String, Object>) bMessage;
        }

        for (Descriptors.FieldDescriptor fieldDescriptor : messageDescriptor.getFields()) {
            switch (fieldDescriptor.getType().toProto().getNumber()) {

                case DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING_VALUE: {
                    if (bMapValue != null && bMapValue.containsKey(fieldDescriptor.getName())) {
                        Object bValue = bMapValue.get(fieldDescriptor.getName());
                        if (bValue instanceof ArrayValue) {
                            ArrayValue valueArray = (ArrayValue) bValue;
                            for (int i = 0; i < valueArray.size(); i++) {
                                size += CodedOutputStream.computeStringSize(fieldDescriptor.getNumber(), valueArray
                                        .getString(i));
                            }
                        } else {
                            size += CodedOutputStream.computeStringSize(fieldDescriptor.getNumber(), (String) bValue);
                        }
                    } else if (isOneofField(bMapValue, fieldDescriptor)) {
                        Object bValue = getOneofFieldMap(bMapValue, fieldDescriptor);
                        if (hasOneofFieldValue(fieldDescriptor.getName(), bValue)) {
                            size += CodedOutputStream.computeStringSize(fieldDescriptor.getNumber(),
                                    (String) ((MapValue) bValue).get(fieldDescriptor.getName()));
                        }
                    } else if (bMessage instanceof String) {
                        size += CodedOutputStream.computeStringSize(fieldDescriptor.getNumber(), (String) bMessage);
                    }
                    break;
                }
                default: {
                    throw new UnsupportedFieldTypeException("Error while calculating the serialized type. Field " +
                            "type is not supported : " + fieldDescriptor.getType());
                }
            }
        }
        memoizedSize = size;
        return size;
    }

    private boolean hasOneofFieldValue(String fieldName, Object bValue) {
        return (bValue instanceof MapValue) && ((MapValue) bValue).containsKey(fieldName);
    }

    private int computeMessageSize(Descriptors.FieldDescriptor fieldDescriptor, Message message) {
        return CodedOutputStream.computeTagSize(fieldDescriptor
                .getNumber()) + CodedOutputStream.computeUInt32SizeNoTag
                (message.getSerializedSize()) + message.getSerializedSize();
    }

    public byte[] toByteArray() {
        try {
            final byte[] result = new byte[getSerializedSize()];
            final CodedOutputStream output = CodedOutputStream.newInstance(result);
            writeTo(output);
            output.checkNoSpaceLeft();
            return result;
        } catch (IOException e) {
            throw new RuntimeException("Serializing " + messageName + " to a byte array threw an IOException" +
                    " (should never happen).", e);
        }
    }


    private Message readMessage(final Descriptors.FieldDescriptor fieldDescriptor, final BType bType,
                                final CodedInputStream in) throws IOException {
        int length = in.readRawVarint32();
        final int oldLimit = in.pushLimit(length);
        Message result = new MessageParser(fieldDescriptor.getMessageType().getName(), bType).parseFrom(in);
        in.popLimit(oldLimit);
        return result;
    }
}