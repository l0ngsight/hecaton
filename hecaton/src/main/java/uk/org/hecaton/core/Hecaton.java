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

package uk.org.hecaton.core;

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.instance.impl.HecatonBootstrap;
import uk.org.hecaton.instance.impl.HecatonInstanceFactory;
import uk.org.hecaton.instance.impl.OutOfMemoryErrorDispatcher;
import uk.org.hecaton.jet.config.JobConfig;

import javax.annotation.Nonnull;
import java.util.Set;

/**
 * Factory for {@link HecatonInstance}'s, a node in a cluster.
 */
public final class Hecaton {

    private Hecaton() {
    }

    /**
     * Shuts down all member {@link HecatonInstance}s running on this JVM.
     * It doesn't shut down all members of the cluster but just the ones running on this JVM.
     *
     * @see #newHecatonInstance(Config)
     */
    public static void shutdownAll() {
        HecatonInstanceFactory.shutdownAll();
    }

    /**
     * Creates a new HecatonInstance (a new node in a cluster).
     * This method allows you to create and run multiple instances
     * of Hecaton cluster members on the same JVM.
     * <p>
     * To shut down all running HecatonInstances (all members on this JVM)
     * call {@link #shutdownAll()}.
     *
     * @param config Configuration for the new HecatonInstance (member)
     * @return the new HecatonInstance
     * @see #shutdownAll()
     * @see #getHecatonInstanceByName(String)
     */
    public static HecatonInstance newHecatonInstance(Config config) {
        return HecatonInstanceFactory.newHecatonInstance(config);
    }

    /**
     * Creates a new HecatonInstance (a new node in a cluster).
     * This method allows you to create and run multiple instances
     * of Hecaton cluster members on the same JVM.
     * <p>
     * To shut down all running HecatonInstances (all members on this JVM)
     * call {@link #shutdownAll()}.
     * <p>
     * Hecaton will look into two places for the configuration file:
     * <ol>
     *     <li>
     *         System property: Hecaton will first check if "hecaton.config" system property is set to a file or a
     *         {@code classpath:...} path.
     *         Examples: -Dhecaton.config=C:/myhecaton.xml , -Dhecaton.config=classpath:the-hecaton-config.xml ,
     *         -Dhecaton.config=classpath:com/mydomain/hecaton.xml
     *     </li>
     *     <li>
     *         "hecaton.xml" file in current working directory
     *     </li>
     *     <li>
     *         Classpath: Hecaton will check classpath for hecaton.xml file.
     *     </li>
     * </ol>
     * If Hecaton doesn't find any config file, it will start with the default configuration (hecaton-default.xml)
     * located in hecaton.jar.
     *
     * @return the new HecatonInstance
     * @see #shutdownAll()
     * @see #getHecatonInstanceByName(String)
     */
    public static HecatonInstance newHecatonInstance() {
        return HecatonInstanceFactory.newHecatonInstance(null);
    }

    /**
     * Returns an existing HecatonInstance with instanceName.
     * <p>
     * To shut down all running HecatonInstances (all members on this JVM)
     * call {@link #shutdownAll()}.
     *
     * @param instanceName Name of the HecatonInstance (member)
     * @return an existing HecatonInstance
     * @see #newHecatonInstance(Config)
     * @see #shutdownAll()
     */
    public static HecatonInstance getHecatonInstanceByName(String instanceName) {
        return HecatonInstanceFactory.getHecatonInstance(instanceName);
    }

    /**
     * Gets or creates a HecatonInstance with the default XML configuration looked up in:
     * <ol>
     *     <li>
     *         System property: Hecaton will first check if "hecaton.config" system property is set to a file or a
     *         {@code classpath:...} path.
     *         Examples: -Dhecaton.config=C:/myhecaton.xml , -Dhecaton.config=classpath:the-hecaton-config.xml ,
     *         -Dhecaton.config=classpath:com/mydomain/hecaton.xml
     *     </li>
     *     <li>
     *         "hecaton.xml" file in current working directory
     *     </li>
     *     <li>
     *         Classpath: Hecaton will check classpath for hecaton.xml file.
     *     </li>
     * </ol>
     *
     * If a configuration file is not located, an {@link IllegalArgumentException} will be thrown.
     * <p>
     * If a Hecaton instance with the same name as the configuration exists, then it is returned, otherwise it is created.
     *
     * @return the HecatonInstance
     * @throws IllegalArgumentException if the instance name of the config is null or empty or if no config file can be
     * located.
     */
    public static HecatonInstance getOrCreateHecatonInstance() {
        return HecatonInstanceFactory.getOrCreateHecatonInstance(null);
    }

    /**
     * Gets or creates the HecatonInstance with a certain name.
     * <p>
     * If a Hecaton instance with the same name as the configuration exists, then it is returned, otherwise it is created.
     * <p>
     * If {@code config} is {@code null}, then an XML configuration file is looked up in the following order:
     * <ol>
     *     <li>
     *         System property: Hecaton will first check if "hecaton.config" system property is set to a file or a
     *         {@code classpath:...} path.
     *         Examples: -Dhecaton.config=C:/myhecaton.xml , -Dhecaton.config=classpath:the-hecaton-config.xml ,
     *         -Dhecaton.config=classpath:com/mydomain/hecaton.xml
     *     </li>
     *     <li>
     *         "hecaton.xml" file in current working directory
     *     </li>
     *     <li>
     *         Classpath: Hecaton will check classpath for hecaton.xml file.
     *     </li>
     * </ol>
     *
     * @param config the Config.
     * @return the HecatonInstance
     * @throws IllegalArgumentException if the instance name of the config is null or empty or if no config file can be
     * located.
     */
    public static HecatonInstance getOrCreateHecatonInstance(Config config) {
        return HecatonInstanceFactory.getOrCreateHecatonInstance(config);
    }


    /**
     * Returns all active/running HecatonInstances on this JVM.
     * <p>
     * To shut down all running HecatonInstances (all members on this JVM)
     * call {@link #shutdownAll()}.
     *
     * @return all active/running HecatonInstances on this JVM
     * @see #newHecatonInstance(Config)
     * @see #getHecatonInstanceByName(String)
     * @see #shutdownAll()
     */
    public static Set<HecatonInstance> getAllHecatonInstances() {
        return HecatonInstanceFactory.getAllHecatonInstances();
    }

    /**
     * Returns either a local Hecaton instance or a "bootstrapped" Hecaton
     * client for a remote Hecaton cluster, depending on the context. The main
     * goal of this factory method is to simplify submitting a Jet job to a remote
     * Hecaton cluster while also making it convenient to test on the local
     * machine.
     * <p>
     * When you submit a job to a Hecaton instance that runs locally in your
     * JVM, it will have all the dependency classes available. However, when
     * you take the same job to a remote Hecaton cluster, you'll often find
     * that it fails with a {@code ClassNotFoundException} because the remote
     * cluster doesn't have all the classes you use in the job.
     * <p>
     * Normally you would have to explicitly add all the dependency classes to
     * the {@link JobConfig#addClass(Class[]) JobConfig}, either one by one or
     * packaged into a JAR. If you're submitting a job using the command-line
     * tool {@code hecaton submit}, the JAR to attach is the same JAR that
     * contains the code that submits the job.
     * <p>
     * This factory takes all of the above into account in order to provide a
     * smoother experience:
     * <ul><li>
     *     When not called from {@code hecaton submit}, it returns a local {@code
     *     HecatonInstance}, either by creating a new one or looking up a cached one.
     *     The instance won't join any cluster.
     * <li>
     *     When called from {@code hecaton submit}, it returns a "bootstrapped"
     *     instance of Hecaton client that automatically attaches the JAR to all jobs
     *     you submit using it.
     * </ul>
     * With these semantics in place it's simple to write code that works both
     * in your local development/testing environment (using a local Hecaton
     * instance) and in production (using the remote cluster).
     * <p>
     * To use this feature, follow these steps:
     * <ol><li>
     *     Write your {@code main()} method and your code the usual way, making
     *     sure you use this method (instead of {@link HecatonClient#newHecatonClient()})
     *     to acquire a Hecaton client instance.
     * <li>
     *     Create a runnable JAR (e.g. {@code jet-job.jar}) with your entry point
     *     declared as the {@code Main-Class} in {@code MANIFEST.MF}. The JAR should
     *     include all dependencies required to run it (except the Hecaton classes
     *     &mdash; these are already available on the cluster classpath).
     * <li>
     *     Submit the job by writing {@code hecaton submit hz-job.jar} on
     *     the command line. This assumes you have downloaded the Hecaton
     *     distribution package and its {@code bin} directory is on your system
     *     path. The Hecaton client will use the configuration file {@code
     *     <distro_root>/config/hecaton-client.yaml}. Adjust that file as
     *     needed.
     * <li>
     *     The same code will work if you run it directly from your IDE. In this
     *     case it will create a local Hecaton instance for itself to run on.
     * </ol>
     * For example, you can write a class like this:
     * <pre>
     * public class CustomJetJob {
     *   public static void main(String[] args) {
     *     HecatonInstance hz = Hecaton.bootstrappedInstance();
     *     hz.getJet().newJob(buildPipeline()).join();
     *   }
     *
     *   public static Pipeline buildPipeline() {
     *       // ...
     *   }
     * }
     * </pre>
     *
     * @since 5.0
     */
    @Nonnull
    public static HecatonInstance bootstrappedInstance() {
        return HecatonBootstrap.getInstance();
    }

    /**
     * Sets <code>OutOfMemoryHandler</code> to be used when an <code>OutOfMemoryError</code>
     * is caught by Hecaton threads.
     *
     * <p>
     * <b>Warning: </b> <code>OutOfMemoryHandler</code> may not be called although JVM throws
     * <code>OutOfMemoryError</code>.
     * Because error may be thrown from an external (user thread) thread
     * and Hecaton may not be informed about <code>OutOfMemoryError</code>.
     * </p>
     *
     * @param outOfMemoryHandler set when an <code>OutOfMemoryError</code> is caught by Hecaton threads
     *
     * @see OutOfMemoryError
     * @see OutOfMemoryHandler
     */
    public static void setOutOfMemoryHandler(OutOfMemoryHandler outOfMemoryHandler) {
        OutOfMemoryErrorDispatcher.setServerHandler(outOfMemoryHandler);
    }
}
