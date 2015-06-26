/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.siddhi.gpl.extension.pmml;

import org.jpmml.evaluator.EvaluatorUtil;
import org.jpmml.evaluator.FieldValue;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;
import org.xml.sax.InputSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.jpmml.manager.PMMLManager;
import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.StringReader;
import java.util.*;

import javax.xml.transform.Source;

public class PmmlModelProcessor extends StreamProcessor {

    private static final Log logger = LogFactory.getLog(PmmlModelProcessor.class);

    private String pmmlDefinition;
    private boolean attributeSelectionAvailable;
    private Map<FieldName, Integer> attributeIndexMap;           // <feature-name, attribute-index> pairs
    
    private List<FieldName> inputFields;        // All the input fields defined in the pmml definition
    private List<FieldName> outputFields;       // Output fields of the pmml definition
    private Evaluator evaluator;

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor, StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {

        StreamEvent event = streamEventChunk.getFirst();
        Map<FieldName, FieldValue> inData = new HashMap<FieldName, FieldValue>();

        Object[] data;
        if (attributeSelectionAvailable) {
            data = event.getBeforeWindowData();
        } else {
            data = event.getOutputData();
        }

        for(Map.Entry<FieldName, Integer> entry : attributeIndexMap.entrySet()) {
            FieldName featureName = entry.getKey();
            int attributeIndex = entry.getValue();
            inData.put(featureName, EvaluatorUtil.prepare(evaluator, featureName, String.valueOf(data[attributeIndex])));
        }

        if(!inData.isEmpty()) {
            try {
                Map<FieldName, ?> result = evaluator.evaluate(inData);
                Object[] output = new Object[result.size()];
                int i = 0;
                for (FieldName fieldName : result.keySet()) {
                    output[i] = EvaluatorUtil.decode(result.get(fieldName));
                    i++;
                }

                complexEventPopulater.populateComplexEvent(event, output);
                nextProcessor.process(streamEventChunk);
            } catch (Exception e) {
                log.error("Error while predicting", e);
                throw new ExecutionPlanRuntimeException("Error while predicting" ,e);
            }
        }
    }

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition, ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {

        if(attributeExpressionExecutors.length == 0) {
            throw new ExecutionPlanValidationException("PMML model definition not available.");
        } else if(attributeExpressionExecutors.length == 1) {
            attributeSelectionAvailable = false;    // model-definition only
        } else {
            attributeSelectionAvailable = true;     // model-definition and stream-attributes list
        }

        // Check whether the first parameter in the expression is the pmml definition
        if(attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor)  {
            Object constantObj = ((ConstantExpressionExecutor) attributeExpressionExecutors[0]).getValue();
            pmmlDefinition = (String) constantObj;
        } else {
            throw new ExecutionPlanValidationException("PMML model definition has not been set as the first parameter");
        }
        
        // Unmarshal the definition and get an executable pmml model
        PMML pmmlModel = unmarshal(pmmlDefinition);
        // Get the different types of fields defined in the pmml model
        PMMLManager pmmlManager = new PMMLManager(pmmlModel);
        
        evaluator = (Evaluator) pmmlManager.getModelManager(ModelEvaluatorFactory.getInstance());
        inputFields = evaluator.getActiveFields();
        outputFields = evaluator.getOutputFields();

        return generateOutputAttributes();
    }

    /**
     * Generate the output attribute list
     * @return
     */
    private List<Attribute> generateOutputAttributes() {

        List<Attribute> outputAttributes = new ArrayList<Attribute>();
        for(FieldName field : outputFields) {
            String dataType = evaluator.getOutputField(field).getDataType().toString();
            Attribute.Type type = null;
            if (dataType.equalsIgnoreCase("double")) {
                type = Attribute.Type.DOUBLE;
            } else if (dataType.equalsIgnoreCase("float")) {
                type = Attribute.Type.FLOAT;
            } else if (dataType.equalsIgnoreCase("integer")) {
                type = Attribute.Type.INT;
            } else if (dataType.equalsIgnoreCase("long")) {
                type = Attribute.Type.LONG;
            } else if (dataType.equalsIgnoreCase("string")) {
                type = Attribute.Type.STRING;
            } else if (dataType.equalsIgnoreCase("boolean")) {
                type = Attribute.Type.BOOL;
            }
            outputAttributes.add(new Attribute(field.getValue(), type));
        }
        return outputAttributes;
    }

    @Override
    public void start() {
        try {
            populateFeatureAttributeMapping();
        } catch (Exception e) {
            log.error("Error while mapping attributes with pmml model features : " + pmmlDefinition, e);
            throw new ExecutionPlanCreationException("Error while mapping attributes with pmml model features : " + pmmlDefinition + "\n" + e.getMessage());
        }
    }

    /**
     * Match the attribute index values of stream with feature names of the model
     * @throws Exception
     */
    private void populateFeatureAttributeMapping() throws Exception {

        attributeIndexMap = new HashMap<FieldName, Integer>();
        HashMap<String, FieldName> features = new HashMap<String, FieldName>();

        for (FieldName fieldName : inputFields) {
            features.put(fieldName.getValue(), fieldName);
        }

        if(attributeSelectionAvailable) {
            int index = 0;
            for (ExpressionExecutor expressionExecutor : attributeExpressionExecutors) {
                if(expressionExecutor instanceof VariableExpressionExecutor) {
                    VariableExpressionExecutor variable = (VariableExpressionExecutor) expressionExecutor;
                    String variableName = variable.getAttribute().getName();
                    if (features.get(variableName) != null) {
                        int attributeIndex = index;
                        attributeIndexMap.put(features.get(variableName), attributeIndex);
                    } else {
                        throw new ExecutionPlanCreationException("No matching feature name found in the model " +
                                "for the attribute : " + variableName);
                    }
                    index++;
                }
            }
        } else {
            String[] attributeNames = inputDefinition.getAttributeNameArray();
            for(String attributeName : attributeNames) {
                if (features.get(attributeName) != null) {
                    int attributeIndex = inputDefinition.getAttributePosition(attributeName);
                    attributeIndexMap.put(features.get(attributeName), attributeIndex);
                } else {
                    throw new ExecutionPlanCreationException("No matching feature name found in the model " +
                            "for the attribute : " + attributeName);
                }
            }
        }
    }

    /**
     * TODO : move to a Util class (PmmlUtil)
     * Unmarshal the definition and get an executable pmml model.
     * 
     * @return  pmml model
     */
    private PMML unmarshal(String pmmlDefinition) {
        try {
            File pmmlFile = new File(pmmlDefinition);
            InputSource pmmlSource;
            Source source;
            // if the given is a file path, read the pmml definition from the file
            if (pmmlFile.isFile() && pmmlFile.canRead()) {
                pmmlSource = new InputSource(new FileInputStream(pmmlFile));
            } else {
                // else, read from the given definition
                pmmlSource = new InputSource(new StringReader(pmmlDefinition));
            }
            source = ImportFilter.apply(pmmlSource);
            return JAXBUtil.unmarshalPMML(source);
        } catch (Exception e) {
            logger.error("Failed to unmarshal the pmml definition: " + e.getMessage());
            throw new ExecutionPlanCreationException("Failed to unmarshal the pmml definition: " + pmmlDefinition + ". "
                    + e.getMessage(), e);
        }
    }
    
    @Override
    public void stop() {

    }

    @Override
    public Object[] currentState() {
        return new Object[0];
    }

    @Override
    public void restoreState(Object[] state) {

    }
}
