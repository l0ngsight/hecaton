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

package uk.org.hecaton.jet.impl;

import uk.org.hecaton.cluster.Cluster;
import uk.org.hecaton.collection.IList;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.util.Preconditions;
import uk.org.hecaton.jet.Jet;
import uk.org.hecaton.jet.JetCacheManager;
import uk.org.hecaton.jet.JetInstance;
import uk.org.hecaton.jet.JetMemberSelector;
import uk.org.hecaton.jet.Job;
import uk.org.hecaton.jet.JobAlreadyExistsException;
import uk.org.hecaton.jet.JobStateSnapshot;
import uk.org.hecaton.jet.Observable;
import uk.org.hecaton.jet.config.JobConfig;
import uk.org.hecaton.jet.config.ProcessingGuarantee;
import uk.org.hecaton.jet.core.DAG;
import uk.org.hecaton.jet.core.JobStatus;
import uk.org.hecaton.jet.impl.observer.ObservableImpl;
import uk.org.hecaton.jet.impl.operation.GetJobIdsOperation.GetJobIdsResult;
import uk.org.hecaton.jet.impl.pipeline.PipelineImpl;
import uk.org.hecaton.jet.impl.util.Util;
import uk.org.hecaton.jet.pipeline.Pipeline;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.replicatedmap.ReplicatedMap;
import uk.org.hecaton.ringbuffer.impl.RingbufferService;
import uk.org.hecaton.security.permission.JobPermission;
import uk.org.hecaton.sql.SqlService;
import uk.org.hecaton.topic.ITopic;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.security.auth.Subject;
import java.security.AccessControlException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static uk.org.hecaton.jet.impl.JobRepository.exportedSnapshotMapName;
import static uk.org.hecaton.jet.impl.util.Util.distinctBy;
import static uk.org.hecaton.security.permission.ActionConstants.ACTION_ADD_RESOURCES;
import static uk.org.hecaton.security.permission.ActionConstants.ACTION_SUBMIT;
import static java.util.stream.Collectors.toList;

/**
 * To not break the static factory methods of {@link Jet} that
 * return the deprecated {@link JetInstance}, we continue to
 * implement {@link JetInstance}, because we need to cast
 * instances of {@link AbstractJetInstance} to {@link JetInstance}
 * there. Search for casts to {@link JetInstance} before you consider
 * removing this.
 *
 * @param <M> the type of member ID (UUID or Address)
 */
@SuppressWarnings("deprecation") // we implement a deprecated API here
public abstract class AbstractJetInstance<M> implements JetInstance {

    // These permissions are configured via JobPermission
    // When the user doesn't configure the JobPermission correctly they get the permission violation on an internal
    // data structure. We translate these to proper JobPermission violations.
    private static final String FLAKE_ID_GENERATOR_JET_IDS_CREATE_DENIED_MESSAGE = "Permission " +
            "\\(\"uk.org.hecaton.security.permission.FlakeIdGeneratorPermission\" \"__jet.ids\" \"create\"\\) denied!";
    private static final String MAP_JET_RESOURCES_CREATE_DENIED_MESSAGE = "Permission " +
            "\\(\"uk.org.hecaton.security.permission.MapPermission\" \"__jet\\.resources\\..*\" \"create\"\\) denied!";

    private final HecatonInstance hecatonInstance;
    private final JetCacheManagerImpl cacheManager;
    private final Supplier<JobRepository> jobRepository;
    private final Map<String, Observable> observables;

    protected AbstractJetInstance(HecatonInstance hecatonInstance) {
        this.hecatonInstance = hecatonInstance;
        this.cacheManager = new JetCacheManagerImpl(this);
        this.jobRepository = Util.memoizeConcurrent(() -> new JobRepository(hecatonInstance));
        this.observables = new ConcurrentHashMap<>();
    }

    public long newJobId() {
        try {
            return jobRepository.get().newJobId();
        } catch (AccessControlException e) {
            if (e.getMessage().matches(FLAKE_ID_GENERATOR_JET_IDS_CREATE_DENIED_MESSAGE)) {
                AccessControlException ace = new AccessControlException("Permission " +
                        new JobPermission(ACTION_SUBMIT) + " denied!");
                ace.addSuppressed(e);
                throw ace;
            } else {
                throw e;
            }
        }
    }

    @Nonnull
    public Job newJob(@Nonnull DAG dag, @Nonnull JobConfig config, @Nullable Subject subject) {
        return newJobInt(newJobId(), dag, config, subject, false);
    }

    @Nonnull @Override
    public Job newJob(@Nonnull DAG dag, @Nonnull JobConfig config) {
        return newJobInt(newJobId(), dag, config, false);
    }

    @Nonnull
    public Job newJob(long jobId, @Nonnull DAG dag, @Nonnull JobConfig config) {
        return newJobInt(jobId, dag, config, false);
    }

    @Nonnull @Override
    public Job newJob(@Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
        return newJobInt(newJobId(), pipeline, config, false);
    }

    @Nonnull
    public Job newJob(long jobId, @Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
        return newJobInt(jobId, pipeline, config, false);
    }

    @Nonnull
    public Job newJob(long jobId, @Nonnull DAG dag, @Nonnull JobConfig config, @Nullable Subject subject) {
        return newJobInt(jobId, dag, config, subject, false);
    }

    private Job newJobInt(long jobId, @Nonnull Object jobDefinition, @Nonnull JobConfig config, boolean isLightJob) {
        return newJobInt(jobId, jobDefinition, config, null, isLightJob);
    }

    private Job newJobInt(long jobId,
                          @Nonnull Object jobDefinition,
                          @Nonnull JobConfig config,
                          @Nullable Subject subject,
                          boolean isLightJob) {
        if (isLightJob) {
            validateConfigForLightJobs(config);
        }
        if (jobDefinition instanceof PipelineImpl pipelineImpl) {
            config = config.attachAll(pipelineImpl.attachedFiles());
        }
        if (!config.getResourceConfigs().isEmpty()) {
            uploadResources(jobId, config);
        }
        return newJobProxy(jobId, isLightJob, jobDefinition, config, subject);
    }

    protected static void validateConfigForLightJobs(JobConfig config) {
        Preconditions.checkTrue(config.getName() == null,
                "JobConfig.name not supported for light jobs");
        Preconditions.checkTrue(config.getResourceConfigs().isEmpty(),
                "Resources (jars, classes, attached files) not supported for light jobs");
        Preconditions.checkTrue(config.getProcessingGuarantee() == ProcessingGuarantee.NONE,
                "A processing guarantee not supported for light jobs");
        Preconditions.checkTrue(config.getClassLoaderFactory() == null,
                "JobConfig.classLoaderFactory not supported for light jobs");
        Preconditions.checkTrue(config.getInitialSnapshotName() == null,
                "JobConfig.initialSnapshotName not supported for light jobs");
        Preconditions.checkTrue(config.getUserCodeNamespace() == null,
                "User Code Namespace association is not supported for light jobs");
    }

    private Job newJobIfAbsent(@Nonnull Object jobDefinition, @Nonnull JobConfig config, @Nullable Subject subject) {
        if (config.getName() == null) {
            return newJobInt(newJobId(), jobDefinition, config, subject, false);
        } else {
            while (true) {
                Job job = getJob(config.getName());
                if (job != null) {
                    JobStatus status = job.getStatus();
                    if (status != JobStatus.FAILED && status != JobStatus.COMPLETED) {
                        return job;
                    }
                }
                try {
                    return newJobInt(newJobId(), jobDefinition, config, subject, false);
                } catch (JobAlreadyExistsException e) {
                    getLogger().fine("Could not submit job with duplicate name: %s, ignoring", config.getName());
                }
            }
        }
    }

    @Nonnull
    public Job newJobIfAbsent(@Nonnull DAG dag, @Nonnull JobConfig config, @Nullable Subject subject) {
        return newJobIfAbsent((Object) dag, config, subject);
    }

    @Nonnull @Override
    public Job newJobIfAbsent(@Nonnull DAG dag, @Nonnull JobConfig config) {
        return newJobIfAbsent((Object) dag, config, null);
    }

    @Nonnull @Override
    public Job newJobIfAbsent(@Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
        return newJobIfAbsent(pipeline, config, null);
    }

    @Nonnull @Override
    public Job newLightJob(@Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
        return newJobInt(newJobId(), pipeline, config, true);
    }

    @Nonnull @Override
    public Job newLightJob(@Nonnull DAG dag, @Nonnull JobConfig config) {
        return newJobInt(newJobId(), dag, config, true);
    }

    /**
     * Submits a job defined in the Core API with attached {@link Subject}.
     */
    @Nonnull
    public Job newLightJob(@Nonnull DAG dag, @Nonnull JobConfig config, @Nullable Subject subject) {
        return newJobInt(newJobId(), dag, config, subject, true);
    }

    @Nonnull
    public Job newLightJob(long jobId, @Nonnull DAG dag, @Nonnull JobConfig config) {
        return newJobInt(jobId, dag, config, true);
    }

    @Nonnull
    public Job newLightJob(long jobId, @Nonnull DAG dag, @Nonnull JobConfig config, @Nullable Subject subject) {
        return newJobInt(jobId, dag, config, subject, true);
    }

    @Nonnull @Override
    public List<Job> getJobs() {
        return mergeJobIdsResults(getAllJobs());
    }

    @Nonnull @Override
    public List<Job> getJobs(@Nonnull String name) {
        GetJobIdsResult result =  getJobByName(name);
        return convertToJobStream(null, result).collect(Collectors.toList());
    }

    @Nullable @Override
    public Job getJob(long jobId) {
        List<Job> jobs = mergeJobIdsResults(getJobsById(jobId));
        assert jobs.size() <= 1;
        return jobs.isEmpty() ? null : jobs.get(0);
    }

    @Nonnull
    private List<Job> mergeJobIdsResults(Map<M, GetJobIdsResult> results) {
        return results.entrySet().stream()
                .flatMap(entry -> convertToJobStream(entry.getKey(), entry.getValue()))
                // Filtering is preserved for compatibility with 5.5: new client with old server
                // Reason: Older server has an issue:
                // In edge cases there can be duplicates. E.g. the GetIdsOp is broadcast to all members.  member1
                // is master and responds and dies. It's removed from cluster, member2 becomes master and
                // responds with the same normal jobs. It's safe to remove duplicates because the same jobId should
                // be the same job - we use FlakeIdGenerator to generate the IDs.
                .filter(distinctBy(Job::getId))
                .collect(toList());
    }

    private Stream<Job> convertToJobStream(@Nullable M member, GetJobIdsResult result) {
        long[] jobIds = result.getJobIds();
        boolean[] isLightJobs = result.getIsLightJobs();
        return IntStream.range(0, jobIds.length)
                .mapToObj(i -> newJobProxy(jobIds[i], isLightJobs[i] ? member : null));
    }

    @Nullable
    @Override
    public JobStateSnapshot getJobStateSnapshot(@Nonnull String name) {
        String mapName = exportedSnapshotMapName(name);
        if (!this.existsDistributedObject(MapService.SERVICE_NAME, mapName)) {
            return null;
        }
        IMap<Object, Object> map = getHecatonInstance().getMap(mapName);
        Object validationRecord = map.get(SnapshotValidationRecord.KEY);
        if (validationRecord instanceof SnapshotValidationRecord snapshotValidationRecord) {
            // update the cache - for robustness. For example after the map was copied
            getHecatonInstance().getMap(JobRepository.EXPORTED_SNAPSHOTS_DETAIL_CACHE).set(name, validationRecord);
            return new JobStateSnapshot(getHecatonInstance(), name, snapshotValidationRecord);
        } else {
            return null;
        }
    }

    @Nonnull
    @Override
    public Collection<JobStateSnapshot> getJobStateSnapshots() {
        return getHecatonInstance().getMap(JobRepository.EXPORTED_SNAPSHOTS_DETAIL_CACHE)
                .entrySet().stream()
                .map(entry -> new JobStateSnapshot(getHecatonInstance(), (String) entry.getKey(),
                        (SnapshotValidationRecord) entry.getValue()))
                .collect(Collectors.toList());
    }

    @Nonnull @Override
    public Cluster getCluster() {
        return hecatonInstance.getCluster();
    }

    @Nonnull @Override
    public String getName() {
        return hecatonInstance.getName();
    }

    @Nonnull @Override
    public SqlService getSql() {
        return hecatonInstance.getSql();
    }

    @Nonnull @Override
    public HecatonInstance getHecatonInstance() {
        return hecatonInstance;
    }

    @Nonnull @Override
    public <K, V> IMap<K, V> getMap(@Nonnull String name) {
        return hecatonInstance.getMap(name);
    }

    @Nonnull @Override
    public <K, V> ReplicatedMap<K, V> getReplicatedMap(@Nonnull String name) {
        return hecatonInstance.getReplicatedMap(name);
    }

    @Nonnull @Override
    public <E> IList<E> getList(@Nonnull String name) {
        return hecatonInstance.getList(name);
    }

    @Nonnull @Override
    public <T> ITopic<T> getReliableTopic(@Nonnull String name) {
        return hecatonInstance.getReliableTopic(name);
    }

    @Nonnull @Override
    public JetCacheManager getCacheManager() {
        return cacheManager;
    }

    @Nonnull @Override
    public <T> Observable<T> getObservable(@Nonnull String name) {
        //noinspection unchecked
        return observables.computeIfAbsent(name, observableName ->
                new ObservableImpl<T>(observableName, hecatonInstance, this::onDestroy, getLogger()));
    }

    @Nonnull
    @Override
    public Collection<Observable<?>> getObservables() {
        return hecatonInstance.getDistributedObjects().stream()
                                .filter(o -> o.getServiceName().equals(RingbufferService.SERVICE_NAME))
                                .filter(o -> o.getName().startsWith(ObservableImpl.JET_OBSERVABLE_NAME_PREFIX))
                                .map(o -> o.getName().substring(ObservableImpl.JET_OBSERVABLE_NAME_PREFIX.length()))
                                .map(this::getObservable)
                                .collect(Collectors.toList());
    }

    @Override
    public void shutdown() {
        hecatonInstance.shutdown();
    }

    private void onDestroy(Observable<?> observable) {
        observables.remove(observable.name());
    }

    public abstract boolean existsDistributedObject(@Nonnull String serviceName, @Nonnull String objectName);

    private void uploadResources(long jobId, JobConfig config) {
        try {
            jobRepository.get().uploadJobResources(jobId, config);
        } catch (AccessControlException e) {
            if (e.getMessage().matches(MAP_JET_RESOURCES_CREATE_DENIED_MESSAGE)) {
                AccessControlException ace = new AccessControlException("Permission " +
                        new JobPermission(ACTION_ADD_RESOURCES) + " denied!");
                ace.addSuppressed(e);
                throw ace;
            } else {
                throw e;
            }
        }
    }

    public abstract ILogger getLogger();

    /**
     * Create a job proxy for a submitted job. {@code lightJobCoordinator} must
     * be non-null iff it's a light job.
     */
    public abstract Job newJobProxy(long jobId, M lightJobCoordinator);

    /**
     * Submit a new job with attached {@link Subject} and return the job proxy.
     */
    public abstract Job newJobProxy(long jobId,
                                    boolean isLightJob,
                                    @Nonnull Object jobDefinition,
                                    @Nonnull JobConfig config,
                                    @Nullable Subject subject);


    protected abstract GetJobIdsResult getJobByName(String onlyName);

    protected abstract Map<M, GetJobIdsResult> getJobsById(Long onlyJobId);

    protected abstract Map<M, GetJobIdsResult> getAllJobs();

    public abstract M getMasterId();

    @Override
    public JobBuilderImpl newJobBuilder(@Nonnull DAG dag) {
        return new JobBuilderImpl(dag);
    }

    @Override
    public JobBuilderImpl newJobBuilder(@Nonnull Pipeline pipeline) {
        return new JobBuilderImpl(pipeline);
    }

    public class JobBuilderImpl implements JobBuilder {
        private final @Nonnull Object jobDefinition;
        private @Nullable JobConfig config;
        private boolean isLightJob;

        JobBuilderImpl(@Nonnull DAG dag) {
            jobDefinition = dag;
        }

        JobBuilderImpl(@Nonnull Pipeline pipeline) {
            jobDefinition = pipeline;
        }

        @Override
        public JobBuilderImpl withConfig(@Nonnull JobConfig jobConfig) {
            this.config = jobConfig;
            return this;
        }

        @Override
        public JobBuilderImpl withMemberSelector(@Nonnull JetMemberSelector memberSelector) {
            if (jobDefinition instanceof DAG dag) {
                dag.setMemberSelector(memberSelector);
            } else {
                ((PipelineImpl) jobDefinition).setMemberSelector(memberSelector);
            }
            return this;
        }

        @Override
        public JobBuilderImpl asLightJob() {
            isLightJob = true;
            return this;
        }

        @Nonnull
        private JobConfig getConfig() {
            return config != null ? config : new JobConfig();
        }

        @Override
        public Job start() {
            return newJobInt(newJobId(), jobDefinition, getConfig(), isLightJob);
        }

        @Override
        public Job startIfAbsent() {
            if (isLightJob) {
                throw new UnsupportedOperationException();
            }
            return newJobIfAbsent(jobDefinition, getConfig(), null);
        }
    }
}
