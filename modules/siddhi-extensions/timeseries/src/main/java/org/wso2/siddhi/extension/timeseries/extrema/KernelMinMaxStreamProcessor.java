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

package org.wso2.siddhi.extension.timeseries.extrema;


import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.extension.timeseries.extrema.util.ExtremaCalculator;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class KernelMinMaxStreamProcessor extends StreamProcessor {
    int[] variablePosition;
    double bw = 0;
    int window = 0;
    Queue<StreamEvent> eventStack = null;
    Queue<Double> priceStack = null;
    Queue<StreamEvent> uniqueQueue = null;
    ExtremaCalculator extremaCalculator = null;

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor, StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        ComplexEventChunk<StreamEvent> returnEventChunk = new ComplexEventChunk<StreamEvent>();
        while (streamEventChunk.hasNext()) {
            StreamEvent event = streamEventChunk.next();
            streamEventChunk.remove();
            StreamEvent result = process(event, complexEventPopulater);
            if (result != null) {
                returnEventChunk.add(result);
            }
        }
        nextProcessor.process(returnEventChunk);
    }

    private StreamEvent process(StreamEvent event, ComplexEventPopulater complexEventPopulater) {
        Double eventKey = (Double) event.getAttribute(variablePosition);

        if (eventStack.size() < window) {
        eventStack.add(event);
        priceStack.add(eventKey);
        } else {
            eventStack.add(event);
            priceStack.add(eventKey);

            Queue<Double> output = extremaCalculator.smooth(priceStack, bw);
            //value 1 is an optimized value for stock market domain, this value may change for other domains
            Integer maxPos = extremaCalculator.findMax(output, 1);
            Integer minPos = extremaCalculator.findMin(output, 1);

            if (maxPos != null) {
                //values 5 and 3 are optimized values for stock market domain, these value may change for other domains
                Integer maxPosEvnt = extremaCalculator.findMax(priceStack, window / 5, window / 3);
                if (maxPosEvnt != null && maxPosEvnt - maxPos <= window / 5 && maxPos - maxPosEvnt <= window / 2) {
                    StreamEvent maximumEvent = (StreamEvent) eventStack.toArray()[maxPosEvnt];
                    if (!uniqueQueue.contains(maximumEvent)) {
                        //value 5 is an optimized value for stock market domain, this value may change for other domains
                        if (uniqueQueue.size() > 5) {
                            uniqueQueue.remove();
                        }
                        uniqueQueue.add(maximumEvent);
                        eventStack.remove();
                        priceStack.remove();

                        StreamEvent returnMaximumEvent = streamEventCloner.copyStreamEvent(maximumEvent);
                        complexEventPopulater.populateComplexEvent(returnMaximumEvent, new Object[]{"max"});

                        return returnMaximumEvent;
                    }
                }
            } else if (minPos != null) {
                //values 5 and 3 are optimized values for stock market domain, these value may change for other domains
                Integer minPosEvnt = extremaCalculator.findMin(priceStack, window / 5, window / 3);

                if (minPosEvnt != null && minPosEvnt - minPos <= window / 5 && minPos - minPosEvnt <= window / 2) {
                    StreamEvent minimumEvent = (StreamEvent) eventStack.toArray()[minPosEvnt];
                    if (!uniqueQueue.contains(minimumEvent)) {
                        //value 5 is an optimized value for stock market domain, this value may change for other domains
                        if (uniqueQueue.size() > 5) {
                            uniqueQueue.remove();
                        }
                        uniqueQueue.add(minimumEvent);
                        eventStack.remove();
                        priceStack.remove();

                        StreamEvent returnMinimumEvent = streamEventCloner.copyStreamEvent(minimumEvent);
                        complexEventPopulater.populateComplexEvent(returnMinimumEvent, new Object[]{"min"});

                        return returnMinimumEvent;
                    }
                }
            }
            eventStack.remove();
            priceStack.remove();
        }
        return null;
    }

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition, ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (attributeExpressionExecutors.length != 3) {
            throw new ExecutionPlanValidationException("Invalid no of arguments passed to KernelMinMaxStreamProcessor, required 3, " +
                    "but found " + attributeExpressionExecutors.length);
        }
        if (attributeExpressionExecutors[0].getReturnType() != Attribute.Type.DOUBLE) {
            throw new ExecutionPlanValidationException("Invalid parameter type found for the argument of KernelMinMaxStreamProcessor, " +
                    "required " + Attribute.Type.DOUBLE +
                    " but found " + attributeExpressionExecutors[0].getReturnType().toString());
        }

        variablePosition = ((VariableExpressionExecutor) attributeExpressionExecutors[0]).getPosition();
        bw = Double.parseDouble(String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue()));
        window = Integer.parseInt(String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue()));

        extremaCalculator = new ExtremaCalculator();
        eventStack = new LinkedList<StreamEvent>();
        priceStack = new LinkedList<Double>();
        uniqueQueue = new LinkedList<StreamEvent>();

        List<Attribute> attributeList = new ArrayList<Attribute>();
        attributeList.add(new Attribute("extremaType", Attribute.Type.STRING));
        return attributeList;

    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Object[] currentState() {
        return new Object[]{eventStack, priceStack, uniqueQueue};
    }

    @Override
    public void restoreState(Object[] state) {
        eventStack = (Queue<StreamEvent>) state[0];
        priceStack = (Queue<Double>) state[1];
        uniqueQueue = (Queue<StreamEvent>) state[2];
    }
}
