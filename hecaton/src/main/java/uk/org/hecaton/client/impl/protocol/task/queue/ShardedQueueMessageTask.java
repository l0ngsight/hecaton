/*
 * Copyright (c) 2026, The Hecaton Authors.
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

package uk.org.hecaton.client.impl.protocol.task.queue;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.QueueAddAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueAddListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueClearCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueCompareAndRemoveAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueCompareAndRetainAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueContainsAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueContainsCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueDrainToCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueDrainToMaxSizeCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueIsEmptyCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueIteratorCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueOfferCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueuePeekCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueuePollCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueuePutCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueRemainingCapacityCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueRemoveCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueRemoveListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueSizeCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueTakeCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractMessageTask;
import uk.org.hecaton.client.impl.protocol.task.BlockingMessageTask;
import uk.org.hecaton.collection.ItemEvent;
import uk.org.hecaton.collection.ItemListener;
import uk.org.hecaton.collection.impl.common.DataAwareItemEvent;
import uk.org.hecaton.collection.impl.queue.QueueService;
import uk.org.hecaton.collection.impl.queue.ShardedQueue;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.QueuePermission;

import java.security.Permission;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * A client request on a sharded queue (load protection, F23): clients send their queue requests as
 * to any queue, to the partition of its name, and this task serves them from the queue's shards
 * through {@link ShardedQueue}, so official clients use sharded queues unchanged. Offers, polls,
 * peeks and sizes answer asynchronously; a waiting {@code take} holds no thread. The bulk operations
 * wait for the shards, so the task runs on the client blocking executor.
 */
public final class ShardedQueueMessageTask extends AbstractMessageTask<Object> implements BlockingMessageTask {

    private final int messageType;
    private final String name;
    private final Object request;
    private final ShardedQueue queue;

    private ShardedQueueMessageTask(ClientMessage clientMessage, Node node, Connection connection, String name,
                                    Object request, ShardedQueue queue) {
        super(clientMessage, node, connection);
        this.messageType = clientMessage.getMessageType();
        this.name = name;
        this.request = request;
        this.queue = queue;
    }

    /** The task for {@code clientMessage} if it is a request on a sharded queue, otherwise {@code null}. */
    public static ShardedQueueMessageTask forMessage(ClientMessage clientMessage, Node node, Connection connection) {
        Object request = decode(clientMessage);
        if (request == null) {
            return null;
        }
        String name = nameOf(request);
        QueueService service = node.getNodeEngine().getService(QueueService.SERVICE_NAME);
        ShardedQueue queue = service.getShardedQueue(name);
        return queue == null ? null : new ShardedQueueMessageTask(clientMessage, node, connection, name, request, queue);
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private static Object decode(ClientMessage message) {
        return switch (message.getMessageType()) {
            case QueueOfferCodec.REQUEST_MESSAGE_TYPE -> QueueOfferCodec.decodeRequest(message);
            case QueuePutCodec.REQUEST_MESSAGE_TYPE -> QueuePutCodec.decodeRequest(message);
            case QueuePollCodec.REQUEST_MESSAGE_TYPE -> QueuePollCodec.decodeRequest(message);
            case QueueTakeCodec.REQUEST_MESSAGE_TYPE -> QueueTakeCodec.decodeRequest(message);
            case QueuePeekCodec.REQUEST_MESSAGE_TYPE -> QueuePeekCodec.decodeRequest(message);
            case QueueSizeCodec.REQUEST_MESSAGE_TYPE -> QueueSizeCodec.decodeRequest(message);
            case QueueIsEmptyCodec.REQUEST_MESSAGE_TYPE -> QueueIsEmptyCodec.decodeRequest(message);
            case QueueRemainingCapacityCodec.REQUEST_MESSAGE_TYPE -> QueueRemainingCapacityCodec.decodeRequest(message);
            case QueueClearCodec.REQUEST_MESSAGE_TYPE -> QueueClearCodec.decodeRequest(message);
            case QueueRemoveCodec.REQUEST_MESSAGE_TYPE -> QueueRemoveCodec.decodeRequest(message);
            case QueueContainsCodec.REQUEST_MESSAGE_TYPE -> QueueContainsCodec.decodeRequest(message);
            case QueueContainsAllCodec.REQUEST_MESSAGE_TYPE -> QueueContainsAllCodec.decodeRequest(message);
            case QueueDrainToCodec.REQUEST_MESSAGE_TYPE -> QueueDrainToCodec.decodeRequest(message);
            case QueueDrainToMaxSizeCodec.REQUEST_MESSAGE_TYPE -> QueueDrainToMaxSizeCodec.decodeRequest(message);
            case QueueIteratorCodec.REQUEST_MESSAGE_TYPE -> QueueIteratorCodec.decodeRequest(message);
            case QueueAddAllCodec.REQUEST_MESSAGE_TYPE -> QueueAddAllCodec.decodeRequest(message);
            case QueueCompareAndRemoveAllCodec.REQUEST_MESSAGE_TYPE -> QueueCompareAndRemoveAllCodec.decodeRequest(message);
            case QueueCompareAndRetainAllCodec.REQUEST_MESSAGE_TYPE -> QueueCompareAndRetainAllCodec.decodeRequest(message);
            case QueueAddListenerCodec.REQUEST_MESSAGE_TYPE -> QueueAddListenerCodec.decodeRequest(message);
            case QueueRemoveListenerCodec.REQUEST_MESSAGE_TYPE -> QueueRemoveListenerCodec.decodeRequest(message);
            default -> null;
        };
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private static String nameOf(Object request) {
        if (request instanceof String name) {
            return name;
        } else if (request instanceof QueueOfferCodec.RequestParameters p) {
            return p.name;
        } else if (request instanceof QueuePutCodec.RequestParameters p) {
            return p.name;
        } else if (request instanceof QueuePollCodec.RequestParameters p) {
            return p.name;
        } else if (request instanceof QueueRemoveCodec.RequestParameters p) {
            return p.name;
        } else if (request instanceof QueueContainsCodec.RequestParameters p) {
            return p.name;
        } else if (request instanceof QueueContainsAllCodec.RequestParameters p) {
            return p.name;
        } else if (request instanceof QueueDrainToMaxSizeCodec.RequestParameters p) {
            return p.name;
        } else if (request instanceof QueueAddAllCodec.RequestParameters p) {
            return p.name;
        } else if (request instanceof QueueCompareAndRemoveAllCodec.RequestParameters p) {
            return p.name;
        } else if (request instanceof QueueCompareAndRetainAllCodec.RequestParameters p) {
            return p.name;
        } else if (request instanceof QueueAddListenerCodec.RequestParameters p) {
            return p.name;
        } else if (request instanceof QueueRemoveListenerCodec.RequestParameters p) {
            return p.name;
        }
        throw new IllegalArgumentException("Not a queue request: " + request);
    }

    @Override
    protected Object decodeClientMessage(ClientMessage clientMessage) {
        return request;
    }

    @Override
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    protected void processMessage() {
        QueueService service = getService(QueueService.SERVICE_NAME);
        switch (messageType) {
            case QueueOfferCodec.REQUEST_MESSAGE_TYPE -> {
                QueueOfferCodec.RequestParameters p = (QueueOfferCodec.RequestParameters) request;
                answer(queue.offer(p.value, p.timeoutMillis));
            }
            case QueuePutCodec.REQUEST_MESSAGE_TYPE ->
                    answer(queue.offer(((QueuePutCodec.RequestParameters) request).value, -1));
            case QueuePollCodec.REQUEST_MESSAGE_TYPE ->
                    answer(queue.poll(((QueuePollCodec.RequestParameters) request).timeoutMillis));
            case QueueTakeCodec.REQUEST_MESSAGE_TYPE -> answer(queue.poll(-1));
            case QueuePeekCodec.REQUEST_MESSAGE_TYPE -> answer(queue.peek());
            case QueueSizeCodec.REQUEST_MESSAGE_TYPE -> answer(queue.size());
            case QueueIsEmptyCodec.REQUEST_MESSAGE_TYPE -> answer(queue.size().thenApply(size -> size == 0));
            case QueueRemainingCapacityCodec.REQUEST_MESSAGE_TYPE -> sendResponse(queue.remainingCapacity());
            case QueueClearCodec.REQUEST_MESSAGE_TYPE -> {
                queue.clear();
                sendResponse(null);
            }
            case QueueRemoveCodec.REQUEST_MESSAGE_TYPE ->
                    sendResponse(queue.remove(((QueueRemoveCodec.RequestParameters) request).value));
            case QueueContainsCodec.REQUEST_MESSAGE_TYPE ->
                    sendResponse(queue.containsAll(List.of(((QueueContainsCodec.RequestParameters) request).value)));
            case QueueContainsAllCodec.REQUEST_MESSAGE_TYPE ->
                    sendResponse(queue.containsAll(((QueueContainsAllCodec.RequestParameters) request).dataList));
            case QueueDrainToCodec.REQUEST_MESSAGE_TYPE -> sendResponse(queue.drain(-1));
            case QueueDrainToMaxSizeCodec.REQUEST_MESSAGE_TYPE ->
                    sendResponse(queue.drain(((QueueDrainToMaxSizeCodec.RequestParameters) request).maxSize));
            case QueueIteratorCodec.REQUEST_MESSAGE_TYPE -> sendResponse(queue.items());
            case QueueAddAllCodec.REQUEST_MESSAGE_TYPE ->
                    sendResponse(queue.addAll(((QueueAddAllCodec.RequestParameters) request).dataList));
            case QueueCompareAndRemoveAllCodec.REQUEST_MESSAGE_TYPE ->
                    sendResponse(queue.compareAndRemove(((QueueCompareAndRemoveAllCodec.RequestParameters) request).dataList,
                            false));
            case QueueCompareAndRetainAllCodec.REQUEST_MESSAGE_TYPE ->
                    sendResponse(queue.compareAndRemove(((QueueCompareAndRetainAllCodec.RequestParameters) request).dataList,
                            true));
            case QueueAddListenerCodec.REQUEST_MESSAGE_TYPE -> sendResponse(addListener(service));
            case QueueRemoveListenerCodec.REQUEST_MESSAGE_TYPE -> {
                UUID id = ((QueueRemoveListenerCodec.RequestParameters) request).registrationId;
                endpoint.removeDestroyAction(id);
                sendResponse(queue.removeItemListener(service, id));
            }
            default -> throw new IllegalStateException("Not a queue request: " + messageType);
        }
    }

    private UUID addListener(QueueService service) {
        QueueAddListenerCodec.RequestParameters p = (QueueAddListenerCodec.RequestParameters) request;
        Data partitionKey = serializationService.toData(name);
        ItemListener<Object> listener = new ItemListener<>() {
            @Override
            public void itemAdded(ItemEvent<Object> event) {
                send(event);
            }

            @Override
            public void itemRemoved(ItemEvent<Object> event) {
                send(event);
            }

            private void send(ItemEvent<Object> event) {
                if (endpoint.isAlive() && event instanceof DataAwareItemEvent<?> dataAware) {
                    sendClientMessage(partitionKey, QueueAddListenerCodec.encodeItemEvent(dataAware.getItemData(),
                            event.getMember().getUuid(), event.getEventType().getType()));
                }
            }
        };
        UUID id = queue.addItemListener(service, listener, p.includeValue);
        endpoint.addDestroyAction(id, () -> queue.removeItemListener(service, id));
        return id;
    }

    private void answer(CompletableFuture<?> result) {
        result.whenComplete((response, error) -> {
            if (error != null) {
                handleProcessingFailure(error);
            } else {
                sendResponse(response);
            }
        });
    }

    @Override
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "unchecked"})
    protected ClientMessage encodeResponse(Object response) {
        return switch (messageType) {
            case QueueOfferCodec.REQUEST_MESSAGE_TYPE -> QueueOfferCodec.encodeResponse((Boolean) response);
            case QueuePutCodec.REQUEST_MESSAGE_TYPE -> QueuePutCodec.encodeResponse();
            case QueuePollCodec.REQUEST_MESSAGE_TYPE -> QueuePollCodec.encodeResponse((Data) response);
            case QueueTakeCodec.REQUEST_MESSAGE_TYPE -> QueueTakeCodec.encodeResponse((Data) response);
            case QueuePeekCodec.REQUEST_MESSAGE_TYPE -> QueuePeekCodec.encodeResponse((Data) response);
            case QueueSizeCodec.REQUEST_MESSAGE_TYPE -> QueueSizeCodec.encodeResponse((Integer) response);
            case QueueIsEmptyCodec.REQUEST_MESSAGE_TYPE -> QueueIsEmptyCodec.encodeResponse((Boolean) response);
            case QueueRemainingCapacityCodec.REQUEST_MESSAGE_TYPE ->
                    QueueRemainingCapacityCodec.encodeResponse((Integer) response);
            case QueueClearCodec.REQUEST_MESSAGE_TYPE -> QueueClearCodec.encodeResponse();
            case QueueRemoveCodec.REQUEST_MESSAGE_TYPE -> QueueRemoveCodec.encodeResponse((Boolean) response);
            case QueueContainsCodec.REQUEST_MESSAGE_TYPE -> QueueContainsCodec.encodeResponse((Boolean) response);
            case QueueContainsAllCodec.REQUEST_MESSAGE_TYPE -> QueueContainsAllCodec.encodeResponse((Boolean) response);
            case QueueDrainToCodec.REQUEST_MESSAGE_TYPE -> QueueDrainToCodec.encodeResponse((Collection<Data>) response);
            case QueueDrainToMaxSizeCodec.REQUEST_MESSAGE_TYPE ->
                    QueueDrainToMaxSizeCodec.encodeResponse((Collection<Data>) response);
            case QueueIteratorCodec.REQUEST_MESSAGE_TYPE -> QueueIteratorCodec.encodeResponse((Collection<Data>) response);
            case QueueAddAllCodec.REQUEST_MESSAGE_TYPE -> QueueAddAllCodec.encodeResponse((Boolean) response);
            case QueueCompareAndRemoveAllCodec.REQUEST_MESSAGE_TYPE ->
                    QueueCompareAndRemoveAllCodec.encodeResponse((Boolean) response);
            case QueueCompareAndRetainAllCodec.REQUEST_MESSAGE_TYPE ->
                    QueueCompareAndRetainAllCodec.encodeResponse((Boolean) response);
            case QueueAddListenerCodec.REQUEST_MESSAGE_TYPE -> QueueAddListenerCodec.encodeResponse((UUID) response);
            case QueueRemoveListenerCodec.REQUEST_MESSAGE_TYPE -> QueueRemoveListenerCodec.encodeResponse((Boolean) response);
            default -> throw new IllegalStateException("Not a queue request: " + messageType);
        };
    }

    @Override
    public String getServiceName() {
        return QueueService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public Permission getRequiredPermission() {
        String action = switch (messageType) {
            case QueueOfferCodec.REQUEST_MESSAGE_TYPE, QueuePutCodec.REQUEST_MESSAGE_TYPE, QueueAddAllCodec.REQUEST_MESSAGE_TYPE ->
                    ActionConstants.ACTION_ADD;
            case QueuePollCodec.REQUEST_MESSAGE_TYPE, QueueTakeCodec.REQUEST_MESSAGE_TYPE, QueueClearCodec.REQUEST_MESSAGE_TYPE,
                 QueueRemoveCodec.REQUEST_MESSAGE_TYPE, QueueDrainToCodec.REQUEST_MESSAGE_TYPE,
                 QueueDrainToMaxSizeCodec.REQUEST_MESSAGE_TYPE, QueueCompareAndRemoveAllCodec.REQUEST_MESSAGE_TYPE,
                 QueueCompareAndRetainAllCodec.REQUEST_MESSAGE_TYPE -> ActionConstants.ACTION_REMOVE;
            case QueueAddListenerCodec.REQUEST_MESSAGE_TYPE, QueueRemoveListenerCodec.REQUEST_MESSAGE_TYPE ->
                    ActionConstants.ACTION_LISTEN;
            default -> ActionConstants.ACTION_READ;
        };
        return new QueuePermission(name, action);
    }

    @Override
    public String getMethodName() {
        return "sharded-queue";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{request};
    }
}
