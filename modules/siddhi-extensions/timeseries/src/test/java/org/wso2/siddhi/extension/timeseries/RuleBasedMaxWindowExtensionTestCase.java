/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org)
 * All Rights Reserved.
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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.extension.timeseries;

import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

public class RuleBasedMaxWindowExtensionTestCase {
    static final Logger log = Logger.getLogger(RuleBasedMaxWindowExtensionTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;

    @Before
    public void init() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void testRuleBasedMaxWindowExtension() throws InterruptedException {
        log.info("RuleBasedMaxWindowExtension TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "@config(async = 'true')define stream inputStream (price double);";
        String query = ("@info(name = 'query1') from inputStream#window.timeseries:ruleMax(price, 4, 3) " +
                "select *" +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    switch (count) {
                        case 1:
                            Assert.assertEquals(63.73, event.getData(0));
                            break;
                        case 2:
                            Assert.assertEquals(61.25, event.getData(0));
                            break;
                        case 3:
                            Assert.assertEquals(60.97, event.getData(0));
                            break;
                        case 4:
                            Assert.assertEquals(60.98, event.getData(0));
                            break;
                        case 5:
                            Assert.assertEquals(56.59, event.getData(0));
                            break;
                        case 6:
                            Assert.assertEquals(57.41, event.getData(0));
                            break;
                        case 7:
                            Assert.assertEquals(59.0, event.getData(0));
                            break;
                        case 8:
                            Assert.assertEquals(59.17, event.getData(0));
                            break;
                        case 9:
                            Assert.assertEquals(51.75, event.getData(0));
                            break;
                        default:
                            org.junit.Assert.fail();
                    }
                }
            }
       });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{61.1d});
        inputHandler.send(new Object[]{61.38d});
        inputHandler.send(new Object[]{62.69d});
        inputHandler.send(new Object[]{62.8d});
        inputHandler.send(new Object[]{63.73d});
        inputHandler.send(new Object[]{63.68d});
        inputHandler.send(new Object[]{62.9d});
        inputHandler.send(new Object[]{61.85d});
        inputHandler.send(new Object[]{60.64d});
        inputHandler.send(new Object[]{61.86d});
        inputHandler.send(new Object[]{60.05d});
        inputHandler.send(new Object[]{60.35d});
        inputHandler.send(new Object[]{59.7d});
        inputHandler.send(new Object[]{58.1d});
        inputHandler.send(new Object[]{59.5d});
        inputHandler.send(new Object[]{60.05d});
        inputHandler.send(new Object[]{60.39d});
        inputHandler.send(new Object[]{60.66d});
        inputHandler.send(new Object[]{61.25d});
        inputHandler.send(new Object[]{60.67d});
        inputHandler.send(new Object[]{60.97d});
        inputHandler.send(new Object[]{60.07d});
        inputHandler.send(new Object[]{60.67d});
        inputHandler.send(new Object[]{59.83d});
        inputHandler.send(new Object[]{59.94d});
        inputHandler.send(new Object[]{59.81d});
        inputHandler.send(new Object[]{60.18d});
        inputHandler.send(new Object[]{60.72d});
        inputHandler.send(new Object[]{60.97d});
        inputHandler.send(new Object[]{59.6d});
        inputHandler.send(new Object[]{59.52d});
        inputHandler.send(new Object[]{60.63d});
        inputHandler.send(new Object[]{60.48d});
        inputHandler.send(new Object[]{60.02d});
        inputHandler.send(new Object[]{60.96d});
        inputHandler.send(new Object[]{60.98d});
        inputHandler.send(new Object[]{60.25d});
        inputHandler.send(new Object[]{60.94d});
        inputHandler.send(new Object[]{60.49d});
        inputHandler.send(new Object[]{60.14d});
        inputHandler.send(new Object[]{59.88d});
        inputHandler.send(new Object[]{60.52d});
        inputHandler.send(new Object[]{60.47d});
        inputHandler.send(new Object[]{59.27d});
        inputHandler.send(new Object[]{58.93d});
        inputHandler.send(new Object[]{58.5d});
        inputHandler.send(new Object[]{57d});
        inputHandler.send(new Object[]{57.55d});
        inputHandler.send(new Object[]{56.47d});
        inputHandler.send(new Object[]{55.97d});
        inputHandler.send(new Object[]{56.14d});
        inputHandler.send(new Object[]{55.93d});
        inputHandler.send(new Object[]{56.59d});
        inputHandler.send(new Object[]{55.84d});
        inputHandler.send(new Object[]{55.53d});
        inputHandler.send(new Object[]{55.69d});
        inputHandler.send(new Object[]{55.75d});
        inputHandler.send(new Object[]{55.58d});
        inputHandler.send(new Object[]{57.39d});
        inputHandler.send(new Object[]{57.41d});
        inputHandler.send(new Object[]{56.86d});
        inputHandler.send(new Object[]{57.02d});
        inputHandler.send(new Object[]{56.55d});
        inputHandler.send(new Object[]{56.5d});
        inputHandler.send(new Object[]{56.35d});
        inputHandler.send(new Object[]{57.38d});
        inputHandler.send(new Object[]{57.88d});
        inputHandler.send(new Object[]{58.88d});
        inputHandler.send(new Object[]{59d});
        inputHandler.send(new Object[]{57.54d});
        inputHandler.send(new Object[]{57.42d});
        inputHandler.send(new Object[]{58.5d});
        inputHandler.send(new Object[]{59.17d});
        inputHandler.send(new Object[]{58.16d});
        inputHandler.send(new Object[]{55.92d});
        inputHandler.send(new Object[]{53.4d});
        inputHandler.send(new Object[]{53.06d});
        inputHandler.send(new Object[]{51.85d});
        inputHandler.send(new Object[]{51.25d});
        inputHandler.send(new Object[]{51.85d});
        inputHandler.send(new Object[]{50.82d});
        inputHandler.send(new Object[]{49d});
        inputHandler.send(new Object[]{46.4d});
        inputHandler.send(new Object[]{45.74d});
        inputHandler.send(new Object[]{47.78d});
        inputHandler.send(new Object[]{46.07d});
        inputHandler.send(new Object[]{47.49d});
        inputHandler.send(new Object[]{50.2d});
        inputHandler.send(new Object[]{50.11d});
        inputHandler.send(new Object[]{51.75d});
        inputHandler.send(new Object[]{49.21d});
        inputHandler.send(new Object[]{48.66d});
        inputHandler.send(new Object[]{46.5d});
        inputHandler.send(new Object[]{48.49d});
        inputHandler.send(new Object[]{48.87d});
        inputHandler.send(new Object[]{51d});
        inputHandler.send(new Object[]{51.78d});
        inputHandler.send(new Object[]{51.73d});
        inputHandler.send(new Object[]{50.61d});
        inputHandler.send(new Object[]{52.27d});



        Thread.sleep(100);
        Assert.assertEquals(9, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();

    }
}
