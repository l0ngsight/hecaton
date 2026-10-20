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

package uk.org.hecaton.internal.ascii.memcache;


import uk.org.hecaton.internal.ascii.TextCommandService;
import uk.org.hecaton.logging.ILogger;

import static uk.org.hecaton.internal.ascii.TextCommandConstants.ERROR;
import static uk.org.hecaton.internal.ascii.TextCommandConstants.TextCommandType.QUIT;
import static uk.org.hecaton.internal.ascii.TextCommandConstants.TextCommandType.UNKNOWN;

public class SimpleCommandProcessor extends MemcacheCommandProcessor<SimpleCommand> {

    private final ILogger logger;

    public SimpleCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService);
        logger = textCommandService.getNode().getLogger(getClass());
    }

    @Override
    public void handle(SimpleCommand command) {
        if (command.getType() == QUIT) {
            try {
                command.getDecoder().closeConnection();
            } catch (Exception e) {
                logger.warning(e);
            }
        } else if (command.getType() == UNKNOWN) {
            command.setResponse(ERROR);
            textCommandService.sendResponse(command);
        }
    }

    @Override
    public void handleRejection(SimpleCommand command) {
        handle(command);
    }
}
