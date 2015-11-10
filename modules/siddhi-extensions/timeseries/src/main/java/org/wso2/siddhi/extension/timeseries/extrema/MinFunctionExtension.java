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
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

public class MinFunctionExtension extends FunctionExecutor {

    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        for (ExpressionExecutor expressionExecutor : attributeExpressionExecutors) {
            if (expressionExecutor.getReturnType() != Attribute.Type.DOUBLE) {
                throw new ExecutionPlanValidationException("Invalid parameter type found for the argument of minima() function, " +
                        "required " + Attribute.Type.DOUBLE +
                        " but found " + expressionExecutor.getReturnType().toString());
            }
        }
    }

    /**
     * return maximum of arbitrary long set of Double values
     *
     * @param data array of Double values
     * @return max
     */
    @Override
    protected Object execute(Object[] data) {
        double min = Double.MAX_VALUE;
        for (Object aObj : data) {
            Double value = (Double) aObj;
            if (value < min) {
                min = value;
            }
        }
        return min;
    }

    @Override
    protected Object execute(Object data) {
        return data;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.DOUBLE;
    }

    @Override
    public Object[] currentState() {
        return new Object[0];
    }

    @Override
    public void restoreState(Object[] state) {

    }
}
