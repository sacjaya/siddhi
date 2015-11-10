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
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.MetaComplexEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.window.FindableProcessor;
import org.wso2.siddhi.core.query.processor.stream.window.WindowProcessor;
import org.wso2.siddhi.core.table.EventTable;
import org.wso2.siddhi.core.util.collection.operator.Finder;
import org.wso2.siddhi.core.util.parser.CollectionOperatorParser;
import org.wso2.siddhi.extension.timeseries.extrema.util.ExtremaCalculator;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class RuleBasedMinWindowExtension extends WindowProcessor implements FindableProcessor {
    int[] variablePosition;
    int bwl = 0;
    int bwr = 0;
    Queue<Double> priceStack = null;
    Queue<StreamEvent> eventStack = null;

    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (attributeExpressionExecutors.length != 3) {
            throw new ExecutionPlanValidationException("Invalid no of arguments passed to MinimaWindow, required 3, but found " + attributeExpressionExecutors.length);
        }
        if (attributeExpressionExecutors[0].getReturnType() != Attribute.Type.DOUBLE) {
            throw new ExecutionPlanValidationException("Invalid parameter type found for the argument of RuleBasedMinWindow, " +
                    "required " + Attribute.Type.DOUBLE +
                    " but found " + attributeExpressionExecutors[0].getReturnType().toString());
        }
        variablePosition = ((VariableExpressionExecutor) attributeExpressionExecutors[0]).getPosition();
        bwl = Integer.parseInt(String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue()));
        bwr = Integer.parseInt(String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue()));

        eventStack = new LinkedList<StreamEvent>();
        priceStack = new LinkedList<Double>();
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor, StreamEventCloner streamEventCloner) {
        ComplexEventChunk<StreamEvent> returnChunk = new ComplexEventChunk<StreamEvent>();
        while (streamEventChunk.hasNext()) {
            StreamEvent event = streamEventChunk.next();
            streamEventChunk.remove();
            Double eventKey = (Double) event.getAttribute(variablePosition);
            ExtremaCalculator extremaCalculator = new ExtremaCalculator();

            if (eventStack.size() < (bwl + bwr)) {
            eventStack.add(event);
            priceStack.add(eventKey);
            } else {
                eventStack.add(event);
                priceStack.add(eventKey);

                if (extremaCalculator.min(priceStack) == priceStack.toArray()[bwl]) {
                    returnChunk.add(streamEventCloner.copyStreamEvent((StreamEvent) eventStack.toArray()[bwl]));
                }
                eventStack.remove();
                priceStack.remove();
            }
        }
        nextProcessor.process(returnChunk);
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public StreamEvent find(ComplexEvent matchingEvent, Finder finder) {
        return finder.find(matchingEvent, eventStack, streamEventCloner);
    }


    @Override
    public Finder constructFinder(Expression expression, MetaComplexEvent metaComplexEvent, ExecutionPlanContext executionPlanContext, List<VariableExpressionExecutor> variableExpressionExecutors, Map<String, EventTable> eventTableMap, int matchingStreamIndex, long withinTime) {
        return CollectionOperatorParser.parse(expression, metaComplexEvent, executionPlanContext, variableExpressionExecutors, eventTableMap, matchingStreamIndex, inputDefinition, withinTime);

    }

    @Override
    public Object[] currentState() {
        return new Object[]{eventStack, priceStack};
    }

    @Override
    public void restoreState(Object[] state) {
        eventStack = (Queue<StreamEvent>) state[0];
        priceStack = (Queue<Double>) state[1];
    }

}
