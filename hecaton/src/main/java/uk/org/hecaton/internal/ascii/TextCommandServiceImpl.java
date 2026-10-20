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

package uk.org.hecaton.internal.ascii;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.instance.impl.OutOfMemoryErrorDispatcher;
import uk.org.hecaton.internal.ascii.memcache.BulkGetCommandProcessor;
import uk.org.hecaton.internal.ascii.memcache.DeleteCommandProcessor;
import uk.org.hecaton.internal.ascii.memcache.EntryConverter;
import uk.org.hecaton.internal.ascii.memcache.ErrorCommandProcessor;
import uk.org.hecaton.internal.ascii.memcache.GetCommandProcessor;
import uk.org.hecaton.internal.ascii.memcache.IncrementCommandProcessor;
import uk.org.hecaton.internal.ascii.memcache.SetCommandProcessor;
import uk.org.hecaton.internal.ascii.memcache.SimpleCommandProcessor;
import uk.org.hecaton.internal.ascii.memcache.Stats;
import uk.org.hecaton.internal.ascii.memcache.StatsCommandProcessor;
import uk.org.hecaton.internal.ascii.memcache.TouchCommandProcessor;
import uk.org.hecaton.internal.ascii.memcache.VersionCommandProcessor;
import uk.org.hecaton.internal.ascii.rest.HttpDeleteCommandProcessor;
import uk.org.hecaton.internal.ascii.rest.HttpGetCommandProcessor;
import uk.org.hecaton.internal.ascii.rest.HttpHeadCommandProcessor;
import uk.org.hecaton.internal.ascii.rest.HttpPostCommandProcessor;
import uk.org.hecaton.internal.ascii.rest.RestCallCollector;
import uk.org.hecaton.internal.ascii.rest.RestValue;
import uk.org.hecaton.internal.nio.Protocols;
import uk.org.hecaton.internal.nio.ascii.TextEncoder;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.server.Server;
import uk.org.hecaton.internal.server.ServerConnectionManager;
import uk.org.hecaton.internal.util.Clock;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.map.IMap;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static uk.org.hecaton.instance.EndpointQualifier.MEMCACHE;
import static uk.org.hecaton.internal.ascii.TextCommandConstants.TextCommandType.ADD;
import static uk.org.hecaton.internal.ascii.TextCommandConstants.TextCommandType.APPEND;
import static uk.org.hecaton.internal.ascii.TextCommandConstants.TextCommandType.BULK_GET;
import static uk.org.hecaton.internal.ascii.TextCommandConstants.TextCommandType.DECREMENT;
import static uk.org.hecaton.internal.ascii.TextCommandConstants.TextCommandType.DELETE;
import static uk.org.hecaton.internal.ascii.TextCommandConstants.TextCommandType.ERROR_CLIENT;
import static uk.org.hecaton.internal.ascii.TextCommandConstants.TextCommandType.ERROR_SERVER;
import static uk.org.hecaton.internal.ascii.TextCommandConstants.TextCommandType.GET;
import static uk.org.hecaton.internal.ascii.TextCommandConstants.TextCommandType.GET_END;
import static uk.org.hecaton.internal.ascii.TextCommandConstants.TextCommandType.HTTP_DELETE;
import static uk.org.hecaton.internal.ascii.TextCommandConstants.TextCommandType.HTTP_GET;
import static uk.org.hecaton.internal.ascii.TextCommandConstants.TextCommandType.HTTP_HEAD;
import static uk.org.hecaton.internal.ascii.TextCommandConstants.TextCommandType.HTTP_POST;
import static uk.org.hecaton.internal.ascii.TextCommandConstants.TextCommandType.HTTP_PUT;
import static uk.org.hecaton.internal.ascii.TextCommandConstants.TextCommandType.INCREMENT;
import static uk.org.hecaton.internal.ascii.TextCommandConstants.TextCommandType.NO_OP;
import static uk.org.hecaton.internal.ascii.TextCommandConstants.TextCommandType.PREPEND;
import static uk.org.hecaton.internal.ascii.TextCommandConstants.TextCommandType.QUIT;
import static uk.org.hecaton.internal.ascii.TextCommandConstants.TextCommandType.REPLACE;
import static uk.org.hecaton.internal.ascii.TextCommandConstants.TextCommandType.SET;
import static uk.org.hecaton.internal.ascii.TextCommandConstants.TextCommandType.STATS;
import static uk.org.hecaton.internal.ascii.TextCommandConstants.TextCommandType.TOUCH;
import static uk.org.hecaton.internal.ascii.TextCommandConstants.TextCommandType.UNKNOWN;
import static uk.org.hecaton.internal.ascii.TextCommandConstants.TextCommandType.VERSION;
import static uk.org.hecaton.internal.util.EmptyStatement.ignore;
import static uk.org.hecaton.internal.util.ThreadUtil.createThreadName;
import static java.lang.Thread.currentThread;

@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classdataabstractioncoupling"})
public class TextCommandServiceImpl implements TextCommandService {

    private static final int TEXT_COMMAND_PROCESSOR_SIZE = 100;
    private static final int MILLIS_TO_SECONDS = 1000;
    private static final long WAIT_TIME = 1000;

    private final TextCommandProcessor[] textCommandProcessors = new TextCommandProcessor[TEXT_COMMAND_PROCESSOR_SIZE];
    private final AtomicLong sets = new AtomicLong();
    private final AtomicLong touches = new AtomicLong();
    private final AtomicLong getHits = new AtomicLong();
    private final AtomicLong getMisses = new AtomicLong();
    private final AtomicLong deleteMisses = new AtomicLong();
    private final AtomicLong deleteHits = new AtomicLong();
    private final AtomicLong incrementHits = new AtomicLong();
    private final AtomicLong incrementMisses = new AtomicLong();
    private final AtomicLong decrementHits = new AtomicLong();
    private final AtomicLong decrementMisses = new AtomicLong();
    private final long startTime = Clock.currentTimeMillis();
    private final RestCallCollector restCallCollector = new RestCallCollector();

    private final Node node;
    private final HecatonInstance hecaton;
    private final ILogger logger;

    private volatile ResponseThreadRunnable responseThreadRunnable;
    private volatile boolean running = true;

    private final Object mutex = new Object();

    @SuppressWarnings("checkstyle:executablestatementcount")
    public TextCommandServiceImpl(Node node) {
        this.node = node;
        this.hecaton = node.hecatonInstance;
        this.logger = node.getLogger(this.getClass().getName());
        EntryConverter entryConverter = new EntryConverter(this, node.getLogger(EntryConverter.class));
        register(GET, new GetCommandProcessor(this, entryConverter));
        register(BULK_GET, new BulkGetCommandProcessor(this, entryConverter));
        register(SET, new SetCommandProcessor(this));
        register(APPEND, new SetCommandProcessor(this));
        register(PREPEND, new SetCommandProcessor(this));
        register(ADD, new SetCommandProcessor(this));
        register(REPLACE, new SetCommandProcessor(this));
        register(GET_END, new NoOpCommandProcessor(this));
        register(DELETE, new DeleteCommandProcessor(this));
        register(QUIT, new SimpleCommandProcessor(this));
        register(STATS, new StatsCommandProcessor(this));
        register(UNKNOWN, new ErrorCommandProcessor(this));
        register(VERSION, new VersionCommandProcessor(this));
        register(TOUCH, new TouchCommandProcessor(this));
        register(INCREMENT, new IncrementCommandProcessor(this, entryConverter));
        register(DECREMENT, new IncrementCommandProcessor(this, entryConverter));
        register(ERROR_CLIENT, new ErrorCommandProcessor(this));
        register(ERROR_SERVER, new ErrorCommandProcessor(this));
        register(HTTP_GET, new HttpGetCommandProcessor(this));
        register(HTTP_POST, new HttpPostCommandProcessor(this));
        register(HTTP_PUT, new HttpPostCommandProcessor(this));
        register(HTTP_DELETE, new HttpDeleteCommandProcessor(this));
        register(HTTP_HEAD, new HttpHeadCommandProcessor(this));
        register(NO_OP, new NoOpCommandProcessor(this));
    }

    protected void register(TextCommandConstants.TextCommandType type, TextCommandProcessor processor) {
        textCommandProcessors[type.getValue()] = processor;
    }

    @Override
    public Node getNode() {
        return node;
    }

    @Override
    public byte[] toByteArray(Object value) {
        Data data = node.getSerializationService().toData(value);
        return data.toByteArray();
    }

    @Override
    public Stats getStats() {
        Stats stats = new Stats();
        stats.setUptime((int) ((Clock.currentTimeMillis() - startTime) / MILLIS_TO_SECONDS));
        stats.setCmdGet(getMisses.get() + getHits.get());
        stats.setCmdSet(sets.get());
        stats.setCmdTouch(touches.get());
        stats.setGetHits(getHits.get());
        stats.setGetMisses(getMisses.get());
        stats.setDeleteHits(deleteHits.get());
        stats.setDeleteMisses(deleteMisses.get());
        stats.setIncrHits(incrementHits.get());
        stats.setIncrMisses(incrementMisses.get());
        stats.setDecrHits(decrementHits.get());
        stats.setDecrMisses(decrementMisses.get());
        Server server = node.getServer();
        ServerConnectionManager cm = server.getConnectionManager(MEMCACHE);
        int memcachedCount = cm == null ? 0 : cm.connectionCount(c -> Protocols.MEMCACHE.equals(c.getConnectionType()));
        stats.setCurrConnections(memcachedCount);
        stats.setTotalConnections(server.connectionCount());
        return stats;
    }

    @Override
    public long incrementDeleteHitCount(int inc) {
        return deleteHits.addAndGet(inc);
    }

    @Override
    public long incrementDeleteMissCount() {
        return deleteMisses.incrementAndGet();
    }

    @Override
    public long incrementGetHitCount() {
        return getHits.incrementAndGet();
    }

    @Override
    public long incrementGetMissCount() {
        return getMisses.incrementAndGet();
    }

    @Override
    public long incrementSetCount() {
        return sets.incrementAndGet();
    }

    @Override
    public long incrementIncHitCount() {
        return incrementHits.incrementAndGet();
    }

    @Override
    public long incrementIncMissCount() {
        return incrementMisses.incrementAndGet();
    }

    @Override
    public long incrementDecrHitCount() {
        return decrementHits.incrementAndGet();
    }

    @Override
    public long incrementDecrMissCount() {
        return decrementMisses.incrementAndGet();
    }

    @Override
    public long incrementTouchCount() {
        return touches.incrementAndGet();
    }

    @Override
    public RestCallCollector getRestCallCollector() {
        return restCallCollector;
    }

    @Override
    public void processRequest(TextCommand command) {
        startResponseThreadIfNotRunning();
        node.nodeEngine.getExecutionService().execute("hz:text", new CommandExecutor(command));
    }

    private void startResponseThreadIfNotRunning() {
        if (responseThreadRunnable == null) {
            synchronized (mutex) {
                if (responseThreadRunnable == null) {
                    responseThreadRunnable = new ResponseThreadRunnable();
                    String threadNamePrefix = createThreadName(hecaton.getName(), "ascii.service.response");
                    Thread thread = new Thread(responseThreadRunnable, threadNamePrefix);
                    thread.start();
                }
            }
        }
    }

    @Override
    public Object get(String mapName, String key) {
        return hecaton.getMap(mapName).get(key);
    }

    @Override
    public Map<String, Object> getAll(String mapName, Set<String> keys) {
        IMap<String, Object> map = hecaton.getMap(mapName);
        return map.getAll(keys);
    }

    @Override
    public int getAdjustedTTLSeconds(int ttl) {
        if (ttl <= TextCommandConstants.getMonthSeconds()) {
            return ttl;
        } else {
            return ttl - (int) (TimeUnit.MILLISECONDS.toSeconds(Clock.currentTimeMillis()));
        }
    }

    @Override
    public byte[] getByteArray(String mapName, String key) {
        Object value = hecaton.getMap(mapName).get(key);
        byte[] result = null;
        if (value != null) {
            if (value instanceof RestValue restValue) {
                result = restValue.getValue();
            } else if (value instanceof byte[] bytes) {
                result = bytes;
            } else {
                result = toByteArray(value);
            }
        }
        return result;
    }

    @Override
    public Object put(String mapName, String key, Object value) {
        return hecaton.getMap(mapName).put(key, value);
    }

    @Override
    public Object put(String mapName, String key, Object value, int ttlSeconds) {
        return hecaton.getMap(mapName).put(key, value, ttlSeconds, TimeUnit.SECONDS);
    }

    @Override
    public Object putIfAbsent(String mapName, String key, Object value, int ttlSeconds) {
        return hecaton.getMap(mapName).putIfAbsent(key, value, ttlSeconds, TimeUnit.SECONDS);
    }

    @Override
    public Object replace(String mapName, String key, Object value) {
        return hecaton.getMap(mapName).replace(key, value);
    }

    @Override
    public void lock(String mapName, String key) throws InterruptedException {
        if (!hecaton.getMap(mapName).tryLock(key, 1, TimeUnit.MINUTES)) {
            throw new RuntimeException("Memcache client could not get the lock for map: "
                    + mapName + ", key: " + key + " in 1 minute");
        }
    }

    @Override
    public void unlock(String mapName, String key) {
        hecaton.getMap(mapName).unlock(key);
    }

    @Override
    public void deleteAll(String mapName) {
        final IMap<Object, Object> map = hecaton.getMap(mapName);
        map.clear();
    }

    @Override
    public Object delete(String mapName, String key) {
        return hecaton.getMap(mapName).remove(key);
    }

    @Override
    public boolean offer(String queueName, Object value) {
        return hecaton.getQueue(queueName).offer(value);
    }

    @Override
    public Object poll(String queueName, int seconds) {
        try {
            return hecaton.getQueue(queueName).poll(seconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            currentThread().interrupt();
            return null;
        }
    }

    @Override
    public Object poll(String queueName) {
        return hecaton.getQueue(queueName).poll();
    }

    @Override
    public int size(String queueName) {
        return hecaton.getQueue(queueName).size();
    }

    @Override
    public void sendResponse(TextCommand textCommand) {
        textCommand.beforeSendResponse(this);
        if (!textCommand.shouldReply() || textCommand.getRequestId() == -1) {
            throw new RuntimeException("Shouldn't reply " + textCommand);
        }
        responseThreadRunnable.sendResponse(textCommand);
    }

    @Override
    public void stop() {
        final ResponseThreadRunnable rtr = responseThreadRunnable;
        if (rtr != null) {
            logger.info("Stopping text command service...");
            rtr.stop();
        }
    }

    @Override
    public String getInstanceName() {
        return hecaton.getName();
    }

    class CommandExecutor implements Runnable {
        final TextCommand command;

        CommandExecutor(TextCommand command) {
            this.command = command;
        }

        @Override
        public void run() {
            try {
                TextCommandConstants.TextCommandType type = command.getType();
                TextCommandProcessor textCommandProcessor = textCommandProcessors[type.getValue()];
                textCommandProcessor.handle(command);
            } catch (Throwable e) {
                logger.warning(e);
            }
        }
    }

    private class ResponseThreadRunnable implements Runnable {
        private final BlockingQueue<TextCommand> blockingQueue = new ArrayBlockingQueue<>(200);
        private final Object stopObject = new Object();

        public void sendResponse(TextCommand textCommand) {
            blockingQueue.offer(textCommand);
        }

        @Override
        public void run() {
            while (running) {
                try {
                    TextCommand textCommand = blockingQueue.take();
                    if (TextCommandConstants.TextCommandType.STOP == textCommand.getType()) {
                        synchronized (stopObject) {
                            stopObject.notify();
                        }
                    } else {
                        TextEncoder textWriteHandler = textCommand.getEncoder();
                        textWriteHandler.enqueue(textCommand);
                    }
                } catch (InterruptedException e) {
                    currentThread().interrupt();
                    return;
                } catch (OutOfMemoryError e) {
                    OutOfMemoryErrorDispatcher.onOutOfMemory(e);
                    throw e;
                } catch (Throwable t) {
                    logger.severe("Error while processing Memcache or Rest command.", t);
                }
            }
        }

        void stop() {
            running = false;
            synchronized (stopObject) {
                try {
                    blockingQueue.offer(new AbstractTextCommand(TextCommandConstants.TextCommandType.STOP) {
                        @Override
                        public boolean readFrom(ByteBuffer src) {
                            return true;
                        }

                        @Override
                        public boolean writeTo(ByteBuffer dst) {
                            return true;
                        }
                    });
                    //noinspection WaitNotInLoop
                    stopObject.wait(WAIT_TIME);
                } catch (Exception ignored) {
                    ignore(ignored);
                }
            }
        }
    }
}
