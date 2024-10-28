/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.azure.search;

import com.azure.ai.openai.OpenAIClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.azure.search.AzureOpenAIConnectionService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Tags({"azure", "cosmos", "insert", "record", "put"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("This processor is a record-aware processor for inserting data into Cosmos DB with Core SQL API. It uses a configured record reader and " +
        "schema to read an incoming record set from the body of a Flowfile and then inserts those records into " +
        "a configured Cosmos DB Container.")
@SystemResourceConsideration(resource = SystemResource.MEMORY)
public class GenerateEmbeddingRecord extends AbstractAzureOpenAIProcessor {

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("RECORD_READER")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("RECORD_WRITER")
            .displayName("Record Writer")
            .description("Record writer service to use for enriching the flowfile contents.")
            .required(true)
            .identifiesControllerService(RecordSetWriterFactory.class)
            .build();

    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("BATCH_SIZE")
            .displayName("Embedding Batch Size")
            .description("The number of records to group together for one single insert operation against Cosmos DB")
            .defaultValue("20")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor EMBEDDING_FIELD = new PropertyDescriptor.Builder()
            .name("EMBEDDING_FIELD")
            .displayName("Country Record Path")
            .description("Record path for putting the country identified for this IP address")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor EMBEDDING_MODEL = new PropertyDescriptor.Builder()
            .name("EMBEDDING_MODEL")
            .displayName("Country Record Path")
            .description("Record path for putting the country identified for this IP address")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES;

    static {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(RECORD_READER);
        properties.add(RECORD_WRITER);
        properties.add(EMBEDDING_FIELD);
        properties.add(EMBEDDING_MODEL);
        properties.add(BATCH_SIZE);
        properties.addAll(AbstractAzureOpenAIProcessor.PROPERTIES);
        PROPERTIES = Collections.unmodifiableList(properties);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }
    private OpenAIClient clientService;
    private RecordReaderFactory recordReaderFactory;
    private RecordSetWriterFactory recordSetWriterFactory;
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    @OnScheduled
    public void onScheduled(ProcessContext context) {
        clientService = context.getProperty(OPENAI_CONNECTION_SERVICE).asControllerService(AzureOpenAIConnectionService.class).getOpenAIClient();
        recordReaderFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        recordSetWriterFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
    }

//    protected void bulkInsert(final List<Map<String, Object>> records) {
//        // In the future, this method will be replaced by calling createItems API
//        // for example, this.container.createItems(records);
//        // currently, no createItems API available in Azure Cosmos Java SDK
//        final ComponentLog logger = getLogger();
//        final OpenAIClient openAIClient = getOpenAIClient();
//    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile input = session.get();
        if ( input == null ) {
            return;
        }

        long delta;
        FlowFile failedRecords = session.create(input);
        WriteResult failedWriteResult;
        try (InputStream is = session.read(input);
             RecordReader reader = recordReaderFactory.createRecordReader(input, is, getLogger());
             OutputStream os = session.write(failedRecords);
             RecordSetWriter failedWriter = recordSetWriterFactory.createWriter(getLogger(), reader.getSchema(), os, input.getAttributes())
        ) {
            Record record;
            long start = System.currentTimeMillis();
            failedWriter.beginRecordSet();
            int records = 0;
            while ((record = reader.nextRecord()) != null) {
                FlowFile graph = session.create(input);

                try {
                    Map<String, Object> dynamicPropertyMap = new HashMap<>();
                    for (String entry : dynamic.keySet()) {
                        if (!dynamicPropertyMap.containsKey(entry)) {
                            dynamicPropertyMap.put(entry, getRecordValue(record, dynamic.get(entry)));
                        }
                    }

                    dynamicPropertyMap.putAll(input.getAttributes());
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug("Dynamic Properties: {}", dynamicPropertyMap);
                    }
                    List<Map<String, Object>> graphResponses = new ArrayList<>(executeQuery(recordScript, dynamicPropertyMap));

                    OutputStream graphOutputStream = session.write(graph);
                    String graphOutput = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(graphResponses);
                    graphOutputStream.write(graphOutput.getBytes(StandardCharsets.UTF_8));
                    graphOutputStream.close();
                    session.transfer(graph, GRAPH);
                } catch (Exception e) {
                    getLogger().error("Error processing record at index {}", records, e);
                    // write failed records to a flowfile destined for the failure relationship
                    failedWriter.write(record);
                    session.remove(graph);
                } finally {
                    records++;
                }
            }
            long end = System.currentTimeMillis();
            delta = (end - start) / 1000;
            if (getLogger().isDebugEnabled()) {
                getLogger().debug(String.format("Took %s seconds.\nHandled %d records", delta, records));
            }
            failedWriteResult = failedWriter.finishRecordSet();
            failedWriter.flush();

        } catch (Exception ex) {
            getLogger().error("Error reading records, routing input FlowFile to failure", ex);
            session.remove(failedRecords);
            session.transfer(input, FAILURE);
            return;
        }

        // Generate provenance and send input flowfile to success
        session.getProvenanceReporter().send(input, clientService.getTransitUrl(), delta * 1000);

        if (failedWriteResult.getRecordCount() < 1) {
            // No failed records, remove the failure flowfile and send the input flowfile to success
            session.remove(failedRecords);
            input = session.putAttribute(input, GRAPH_OPERATION_TIME, String.valueOf(delta));
            session.transfer(input, SUCCESS);
        } else {
            failedRecords = session.putAttribute(failedRecords, RECORD_COUNT, String.valueOf(failedWriteResult.getRecordCount()));
            session.transfer(failedRecords, FAILURE);
            // There were failures, don't send the input flowfile to SUCCESS
            session.remove(input);
        }
    }
}
