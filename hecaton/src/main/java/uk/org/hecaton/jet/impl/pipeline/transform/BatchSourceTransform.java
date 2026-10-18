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

package uk.org.hecaton.jet.impl.pipeline.transform;

import uk.org.hecaton.jet.core.ProcessorMetaSupplier;
import uk.org.hecaton.jet.impl.ProcessorClassLoaderTLHolder;
import uk.org.hecaton.jet.impl.pipeline.PipelineImpl.Context;
import uk.org.hecaton.jet.impl.pipeline.Planner;
import uk.org.hecaton.jet.pipeline.BatchSource;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serial;

import static uk.org.hecaton.jet.impl.util.Util.doWithClassLoader;
import static java.util.Collections.emptyList;

public class BatchSourceTransform<T> extends AbstractTransform implements BatchSource<T> {

    @Serial
    private static final long serialVersionUID = 1L;

    @Nonnull
    public ProcessorMetaSupplier metaSupplier;
    private boolean isAssignedToStage;

    public BatchSourceTransform(
            @Nonnull String name,
            @Nonnull ProcessorMetaSupplier metaSupplier
    ) {
        super(name, emptyList());
        this.metaSupplier = metaSupplier;
    }

    public void onAssignToStage() {
        if (isAssignedToStage) {
            throw new IllegalStateException("Sink " + name() + " was already assigned to a sink stage");
        }
        isAssignedToStage = true;
    }

    @Override
    public void addToDag(Planner p, Context context) {
        determineLocalParallelism(metaSupplier.preferredLocalParallelism(), context, false);
        p.addVertex(this, name(), determinedLocalParallelism(), metaSupplier);
    }

    @Serial
    private void writeObject(ObjectOutputStream out) throws IOException {
        out.writeObject(metaSupplier);
        out.writeBoolean(isAssignedToStage);
    }

    @Serial
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        metaSupplier = doWithClassLoader(ProcessorClassLoaderTLHolder.get(name()), () -> (ProcessorMetaSupplier) in.readObject());
        isAssignedToStage = in.readBoolean();
    }

}
