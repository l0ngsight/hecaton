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

package uk.org.hecaton.jet.impl.util;

import uk.org.hecaton.client.impl.protocol.ClientExceptionFactory;
import uk.org.hecaton.client.impl.protocol.ClientExceptionFactory.ExceptionFactory;
import uk.org.hecaton.core.HecatonInstanceNotActiveException;
import uk.org.hecaton.core.MemberLeftException;
import uk.org.hecaton.core.OperationTimeoutException;
import uk.org.hecaton.jet.JetException;
import uk.org.hecaton.jet.JobAlreadyExistsException;
import uk.org.hecaton.jet.RestartableException;
import uk.org.hecaton.jet.core.JobNotFoundException;
import uk.org.hecaton.jet.core.TopologyChangedException;
import uk.org.hecaton.jet.datamodel.Tuple3;
import uk.org.hecaton.jet.impl.exception.CancellationByUserException;
import uk.org.hecaton.jet.impl.exception.EnteringPassiveClusterStateException;
import uk.org.hecaton.jet.impl.exception.JetDisabledException;
import uk.org.hecaton.jet.impl.exception.JobTerminateRequestedException;
import uk.org.hecaton.jet.impl.exception.TerminatedWithSnapshotException;
import uk.org.hecaton.jet.impl.execution.TaskletExecutionException;
import uk.org.hecaton.jet.impl.operation.InitExecutionOperation;
import uk.org.hecaton.jet.impl.operation.StartExecutionOperation;
import uk.org.hecaton.jet.pipeline.test.AssertionCompletedException;
import uk.org.hecaton.nio.serialization.HecatonSerializationException;
import uk.org.hecaton.spi.exception.TargetNotMemberException;
import uk.org.hecaton.sql.impl.ResultLimitReachedException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static uk.org.hecaton.client.impl.protocol.ClientProtocolErrorCodes.JET_EXCEPTIONS_RANGE_START;
import static uk.org.hecaton.jet.datamodel.Tuple3.tuple3;

public final class ExceptionUtil {
    private static final List<Tuple3<Integer, Class<? extends Throwable>, ExceptionFactory>> EXCEPTIONS = Arrays.asList(
            tuple3(JET_EXCEPTIONS_RANGE_START, JetException.class, JetException::new),
            tuple3(JET_EXCEPTIONS_RANGE_START + 1, TopologyChangedException.class, TopologyChangedException::new),
            tuple3(JET_EXCEPTIONS_RANGE_START + 2, JobNotFoundException.class, JobNotFoundException::new),
            tuple3(JET_EXCEPTIONS_RANGE_START + 3, JobAlreadyExistsException.class, JobAlreadyExistsException::new),
            tuple3(JET_EXCEPTIONS_RANGE_START + 4, AssertionCompletedException.class, AssertionCompletedException::new),
            tuple3(JET_EXCEPTIONS_RANGE_START + 5, JetDisabledException.class, JetDisabledException::new),
            tuple3(JET_EXCEPTIONS_RANGE_START + 6, CancellationByUserException.class, CancellationByUserException::new),
            tuple3(JET_EXCEPTIONS_RANGE_START + 7, TaskletExecutionException.class, TaskletExecutionException::new)
    );

    private ExceptionUtil() { }

    /**
     * Returns true if the exception is one of a kind upon which the job
     * restarts rather than fails.
     */
    @SuppressWarnings("checkstyle:booleanexpressioncomplexity")
    public static boolean isRestartableException(Throwable t) {
        return isTopologyExceptionCause(t)
                || (t instanceof RestartableException)
                || (t instanceof JetException && isOrHasCause(t, RestartableException.class))
                || (t instanceof CompletionException && isOrHasCause(t, RestartableException.class))
                ;
    }

    @SuppressWarnings("checkstyle:booleanexpressioncomplexity")
    public static boolean isTopologyException(Throwable t) {
        return t instanceof TopologyChangedException
            || t instanceof MemberLeftException
            || t instanceof TargetNotMemberException
            || t instanceof HecatonInstanceNotActiveException
            || t instanceof EnteringPassiveClusterStateException
            || t instanceof OperationTimeoutException
            && (t.getMessage().contains(InitExecutionOperation.class.getSimpleName())
            || t.getMessage().contains(StartExecutionOperation.class.getSimpleName()));
    }

    @SuppressWarnings("checkstyle:booleanexpressioncomplexity")
    private static boolean isTopologyExceptionCause(Throwable t) {
        return isOrHasCause(t, TopologyChangedException.class)
            || isOrHasCause(t, MemberLeftException.class)
            || isOrHasCause(t, TargetNotMemberException.class)
            || isOrHasCause(t, HecatonInstanceNotActiveException.class)
            || isOrHasCause(t, EnteringPassiveClusterStateException.class)
            || isCauseOperationTimeoutFromExecution(t);
    }

    private static boolean isCauseOperationTimeoutFromExecution(Throwable t) {
        if (!isOrHasCause(t, OperationTimeoutException.class)) {
            return false;
        }
        String message = t.getMessage();
        if (message == null) {
            return false;
        }
        return message.contains(InitExecutionOperation.class.getSimpleName())
            || message.contains(StartExecutionOperation.class.getSimpleName());
    }

    /**
     * Called during startup to make our exceptions known to Hecaton serialization
     */
    public static void registerJetExceptions(@Nonnull ClientExceptionFactory factory) {
        for (Tuple3<Integer, Class<? extends Throwable>, ExceptionFactory> exception : EXCEPTIONS) {
            factory.register(exception.f0(), exception.f1(), exception.f2());
        }
    }

    /**
     * If {@code t} is either of {@link CompletionException}, {@link ExecutionException}
     * or {@link InvocationTargetException}, returns its cause, peeling it recursively.
     * Otherwise returns {@code t}.
     *
     * @param t Throwable to peel
     * @see #peeledAndUnchecked(Throwable)
     */
    public static Throwable peel(@Nullable Throwable t) {
        while ((t instanceof CompletionException
                || t instanceof ExecutionException
                || t instanceof InvocationTargetException)
                && t.getCause() != null
                && t.getCause() != t
        ) {
            t = t.getCause();
        }
        return t;
    }

    /**
     * Same as {@link #peel(Throwable)}, but additionally if the resulting
     * throwable is not an instance of {@link RuntimeException}, wraps it as
     * {@link JetException}.
     *
     * @param t Throwable to peel
     * @see #peel(Throwable)
     */
    @Nonnull
    private static RuntimeException peeledAndUnchecked(@Nonnull Throwable t) {
        t = peel(t);

        if (t instanceof RuntimeException exception) {
            return exception;
        }

        return new JetException(t);
    }

    @Nonnull
    public static RuntimeException rethrow(@Nonnull final Throwable t) {
        uk.org.hecaton.internal.util.ExceptionUtil.rethrowIfError(t);
        throw peeledAndUnchecked(t);
    }

    /**
     * If the given exception has "java.lang.ClassCastException: cannot assign
     * instance of java.lang.invoke.SerializedLambda" in the causes, wrap it in
     * another JetException explaining the possible reason.
     * <p>
     * This is a hack to improve readability of this common exception.
     *
     * @param e the exception to handle
     * @return the given exception wrapped, if it is a case of CCE for SerializedLambda
     *     or the given exception otherwise
     */
    // TODO: This behaviour changed in JDK 23, we should remove it eventually (see https://bugs.openjdk.org/browse/JDK-8331224)
    public static RuntimeException handleSerializedLambdaCce(HecatonSerializationException e) {
        Throwable cause = e.getCause();
        while (cause != null) {
            if (cause instanceof ClassCastException
                    && cause.getMessage().startsWith("cannot assign instance of java.lang.invoke.SerializedLambda")) {
                throw new JetException("Class containing the lambda probably missing from class path, did you add it " +
                        "using JobConfig.addClass()?: " + e, e);
            }
            cause = cause.getCause();
        }

        throw e;
    }

    /**
     * Checks, if {@code t} itself or any exception in its cause chain is an
     * instance of {@code classToFind}.
     */
    public static boolean isOrHasCause(Throwable t, Class<?> classToFind) {
        while (t != null && t.getCause() != t && !classToFind.isAssignableFrom(t.getClass())) {
            t = t.getCause();
        }
        return t != null && classToFind.isAssignableFrom(t.getClass());
    }

    public static boolean isTechnicalCancellationException(Throwable t) {
        Throwable peeledFailure = peel(t);
        return peeledFailure instanceof JobTerminateRequestedException
                || peeledFailure instanceof ResultLimitReachedException
                || peeledFailure instanceof TerminatedWithSnapshotException
                || peeledFailure instanceof CancellationByUserException
                || checkCause(peeledFailure);
    }

    private static boolean checkCause(Throwable peeledFailure) {
        return peeledFailure.getCause() != null
                && peeledFailure != peeledFailure.getCause()
                && isTechnicalCancellationException(peeledFailure.getCause());
    }
}
