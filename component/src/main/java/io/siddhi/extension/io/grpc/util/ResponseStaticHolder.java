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
package io.siddhi.extension.io.grpc.util;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.HashMap;

public class ResponseStaticHolder {
    private static ResponseStaticHolder instance = new ResponseStaticHolder();
    HashMap<String, ListenableFuture> listenableFutureHolder = new HashMap<>();

    private ResponseStaticHolder() {
    }

    public static ResponseStaticHolder getInstance() {
        return instance;
    }

    public void putListenableFuture(String key, ListenableFuture listenableFuture) {
        listenableFutureHolder.put(key, listenableFuture);
    }

    public ListenableFuture getListenableFuture(String key) {
        return listenableFutureHolder.get(key);
    }

    public void removeListenableFuture(String key) {
        listenableFutureHolder.remove(key);
    }
}
