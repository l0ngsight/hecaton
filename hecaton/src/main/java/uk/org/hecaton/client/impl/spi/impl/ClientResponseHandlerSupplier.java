/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.org.hecaton.client.impl.spi.impl;

import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.spi.impl.listener.ClientListenerServiceImpl;
import uk.org.hecaton.internal.util.ConcurrencyDetection;
import uk.org.hecaton.internal.util.MutableInteger;
import uk.org.hecaton.internal.util.ThreadAffinity;
import uk.org.hecaton.internal.util.concurrent.MPSCQueue;
import uk.org.hecaton.internal.util.executor.HecatonManagedThread;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.spi.impl.operationservice.impl.InboundResponseHandlerSupplier;
import uk.org.hecaton.spi.properties.HecatonProperties;
import uk.org.hecaton.spi.properties.HecatonProperty;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static uk.org.hecaton.client.impl.protocol.codec.builtin.ErrorsCodec.EXCEPTION_MESSAGE_TYPE;
import static uk.org.hecaton.client.properties.ClientProperty.RESPONSE_THREAD_COUNT;
import static uk.org.hecaton.client.properties.ClientProperty.RESPONSE_THREAD_DYNAMIC;
import static uk.org.hecaton.instance.impl.OutOfMemoryErrorDispatcher.onOutOfMemory;
import static uk.org.hecaton.internal.util.HashUtil.hashToIndex;
import static uk.org.hecaton.internal.util.ThreadAffinity.newSystemThreadAffinity;
import static uk.org.hecaton.spi.impl.operationservice.impl.InboundResponseHandlerSupplier.getIdleStrategy;

/**
 * A {@link Supplier} for {@link Supplier} instance that processes responses for client
 * invocations.
 * <p>
 * Depending on the configuration the supplier provides:
 * <ol>
 * <li>a on thread ClientResponseHandler (so no offloading to a different thread)</li>
 * <li>a single threaded ClientResponseHandler that offloads the response processing
 * a ResponseThread/li>
 * <li>a multi threaded ClientResponseHandler that offloads the response processing
 * to a pool of ResponseThreads.</li>
 * </ol>
 * <p>
 * @see InboundResponseHandlerSupplier
 */
public class ClientResponseHandlerSupplier implements Supplier<Consumer<ClientMessage>> {

    // expert setting; we don't want to expose this to the public
    private static final HecatonProperty IDLE_STRATEGY
            = new HecatonProperty("hecaton.client.responsequeue.idlestrategy", "block");

    private static final ThreadLocal<MutableInteger> INT_HOLDER = ThreadLocal.withInitial(() -> new MutableInteger());

    private final ClientInvocationServiceImpl invocationService;
    private final ResponseThread[] responseThreads;
    private final HecatonClientInstanceImpl client;

    private final ILogger logger;
    private final Consumer<ClientMessage> responseHandler;
    private final boolean responseThreadsDynamic;
    private final ConcurrencyDetection concurrencyDetection;
    private final ThreadAffinity threadAffinity = newSystemThreadAffinity("hecaton.client.response.thread.affinity");

    public ClientResponseHandlerSupplier(ClientInvocationServiceImpl invocationService,
                                         ConcurrencyDetection concurrencyDetection) {
        this.invocationService = invocationService;
        this.concurrencyDetection = concurrencyDetection;
        this.client = invocationService.client;
        this.logger = invocationService.invocationLogger;

        HecatonProperties properties = client.getProperties();
        int responseThreadCount = properties.getInteger(RESPONSE_THREAD_COUNT);
        if (threadAffinity.isEnabled()) {
            responseThreadCount = threadAffinity.getThreadCount();
        }

        if (responseThreadCount < 0) {
            throw new IllegalArgumentException(RESPONSE_THREAD_COUNT.getName() + " can't be smaller than 0");
        }
        this.responseThreadsDynamic = properties.getBoolean(RESPONSE_THREAD_DYNAMIC);
        logger.info("Running with " + responseThreadCount + " response threads, dynamic=" + responseThreadsDynamic);
        this.responseThreads = new ResponseThread[responseThreadCount];
        for (int k = 0; k < responseThreads.length; k++) {
            responseThreads[k] = new ResponseThread(invocationService.client.getName() + ".responsethread-" + k + "-");
            responseThreads[k].setThreadAffinity(threadAffinity);
        }

        if (responseThreadCount == 0) {
            this.responseHandler = new SyncResponseHandler();
        } else if (responseThreadsDynamic) {
            this.responseHandler = new DynamicResponseHandler();
        } else {
            this.responseHandler = new AsyncResponseHandler();
        }
    }

    public void start() {
        if (responseThreadsDynamic) {
            // when dynamic mode is enabled, response threads should not be started.
            // this is useful for ephemeral clients where thread creation can be a
            // significant part of the total execution time.
            return;
        }
        for (ResponseThread responseThread : responseThreads) {
            responseThread.start();
        }
    }

    public void shutdown() {
        for (ResponseThread responseThread : responseThreads) {
            responseThread.interrupt();
        }
    }

    @Override
    public Consumer<ClientMessage> get() {
        return responseHandler;
    }

    private void process(ClientMessage response) {
        try {
            handleResponse(response);
        } catch (Exception e) {
            logger.severe("Failed to process response: " + response
                    + " on responseThread: " + Thread.currentThread().getName(), e);
        }
    }

    private void handleResponse(ClientMessage message) {
        if (ClientMessage.isFlagSet(message.getHeaderFlags(), ClientMessage.BACKUP_EVENT_FLAG)) {
            ClientListenerServiceImpl listenerService = (ClientListenerServiceImpl) client.getListenerService();
            listenerService.handleEventMessageOnCallingThread(message);
            return;
        }

        long correlationId = message.getCorrelationId();

        ClientInvocation invocation = invocationService.getInvocation(correlationId);
        if (invocation == null) {
            logger.warning("No call for callId: " + correlationId + ", response: " + message);
            return;
        }

        if (EXCEPTION_MESSAGE_TYPE == message.getMessageType()) {
            invocation.notifyException(correlationId , client.getClientExceptionFactory().createException(message));
        } else {
            invocation.notify(message);
        }
    }

    private ResponseThread nextResponseThread() {
        if (responseThreads.length == 1) {
            return responseThreads[0];
        } else {
            int index = hashToIndex(INT_HOLDER.get().getAndInc(), responseThreads.length);
            return responseThreads[index];
        }
    }

    private class ResponseThread extends HecatonManagedThread {
        private final BlockingQueue<ClientMessage> responseQueue;
        private final AtomicBoolean started = new AtomicBoolean();

        ResponseThread(String name) {
            super(name);
            setContextClassLoader(client.getClientConfig().getClassLoader());
            this.responseQueue = new MPSCQueue<>(this,
                    getIdleStrategy(client.getProperties(), IDLE_STRATEGY));
        }

        @Override
        public void executeRun() {
            try {
                doRun();
            } catch (OutOfMemoryError e) {
                onOutOfMemory(e);
            } catch (Throwable t) {
                invocationService.invocationLogger.severe(t);
            }
        }

        private void doRun() {
            while (!invocationService.isShutdown()) {
                ClientMessage response;
                try {
                    response = responseQueue.take();
                } catch (InterruptedException e) {
                    continue;
                }
                process(response);
            }
        }

        private void queue(ClientMessage message) {
            responseQueue.add(message);
        }

//        @SuppressFBWarnings(value = "IA_AMBIGUOUS_INVOCATION_OF_INHERITED_OR_OUTER_METHOD",
//                justification = "The thread.start method is the one we want to call")
        private void ensureStarted() {
            if (!started.get() && started.compareAndSet(false, true)) {
                super.start();
            }
        }
    }

    class SyncResponseHandler implements Consumer<ClientMessage> {
        @Override
        public void accept(ClientMessage message) {
            process(message);
        }
    }


    class AsyncResponseHandler implements Consumer<ClientMessage> {
        @Override
        public void accept(ClientMessage message) {
            nextResponseThread().queue(message);
        }
    }

    // dynamically switches between direct processing on io thread and processing on
// response thread based on if concurrency is detected
    class DynamicResponseHandler implements Consumer<ClientMessage> {
        @Override
        public void accept(ClientMessage message) {
            if (concurrencyDetection.isDetected()) {
                ResponseThread responseThread = nextResponseThread();
                responseThread.queue(message);
                responseThread.ensureStarted();
            } else {
                process(message);
            }
        }
    }
}
