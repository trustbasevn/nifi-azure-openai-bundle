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
import com.azure.ai.openai.models.Embeddings;
import com.azure.ai.openai.models.EmbeddingsOptions;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.azure.search.AzureOpenAIConnectionService;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Tags({"azure", "cosmos", "insert", "record", "put"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("This processor is a record-aware processor for inserting data into Cosmos DB with Core SQL API. It uses a configured record reader and " +
        "schema to read an incoming record set from the body of a Flowfile and then inserts those records into " +
        "a configured Cosmos DB Container.")
@SystemResourceConsideration(resource = SystemResource.MEMORY)
public class GenerateEmbeddingRecord extends AbstractAzureOpenAIProcessor {
    static final AllowableValue TEXT_EMBEDDING_ADA_002 = new AllowableValue("text-embedding-ada-002", "text-embedding-ada-002",
            "Text Embedding Ada 002");
    static final AllowableValue TEXT_EMBEDDING_3_SMALL = new AllowableValue("text-embedding-3-small", "text-embedding-3-small",
            "Text Embedding 3 Small");
    static final AllowableValue TEXT_EMBEDDING_3_LARGE = new AllowableValue("text-embedding-3-large", "text-embedding-3-large",
            "Text Embedding 3 Large");

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .description("Specifies the Controller Service to use for reading incoming data")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();
    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .description("Specifies the Controller Service to use for writing out the records")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor INCLUDE_ZERO_RECORD_FLOWFILES = new PropertyDescriptor.Builder()
            .name("Include Zero Record FlowFiles")
            .description("When converting an incoming FlowFile, if the conversion results in no data, "
                    + "this property specifies whether a FlowFile will be sent to the corresponding relationship")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    public static final PropertyDescriptor EMBEDDING_MODEL_NAME = new PropertyDescriptor.Builder()
            .name("Vector Embedding Model Name")
            .displayName("Vector Embedding Model Name")
            .description("The OpenAI embedding model to use for embedding data")
            .allowableValues(TEXT_EMBEDDING_ADA_002, TEXT_EMBEDDING_3_SMALL, TEXT_EMBEDDING_3_LARGE)
            .defaultValue(TEXT_EMBEDDING_ADA_002.getValue())
            .required(true)
            .build();

    static final PropertyDescriptor EMBEDDING_DIMENSIONS = new PropertyDescriptor.Builder()
            .name("Vector Embedding Dimensions")
            .displayName("Embedding Dimensions")
            .description("The number of dimensions to use for embedding data")
            .defaultValue("1536")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("BATCH_SIZE")
            .displayName("Embedding Batch Size")
            .description("The number of records to group together for one single insert operation against Cosmos DB")
            .defaultValue("20")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor SOURCE_FIELD = new PropertyDescriptor.Builder()
            .name("Input Field")
            .description("The field name containing the text to be embedded.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ADDED_FIELD = new PropertyDescriptor.Builder()
            .name("Embedding Field")
            .description("The field name where the embedding vector will be added.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List <PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(RECORD_READER);
        properties.add(RECORD_WRITER);
        properties.add(INCLUDE_ZERO_RECORD_FLOWFILES);
        properties.add(EMBEDDING_MODEL_NAME);
        properties.add(EMBEDDING_DIMENSIONS);
        properties.add(BATCH_SIZE);
        properties.add(SOURCE_FIELD);
        properties.add(ADDED_FIELD);
        return properties;
    }

//    @Override
//    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
//        return new PropertyDescriptor.Builder()
//                .name(propertyDescriptorName)
//                .description("Specifies the value to use to replace fields in the record that match the RecordPath: " + propertyDescriptorName)
//                .required(false)
//                .dynamic(true)
//                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
//                .addValidator(new RecordPathPropertyNameValidator())
//                .build();
//    }
//
//    @Override
//    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
//        final boolean containsDynamic = validationContext.getProperties().keySet().stream().anyMatch(PropertyDescriptor::isDynamic);
//
//        if (containsDynamic) {
//            return Collections.emptyList();
//        }
//
//        return Collections.singleton(new ValidationResult.Builder()
//                .subject("User-defined Properties")
//                .valid(false)
//                .explanation("At least one RecordPath must be specified")
//                .build());
//    }

    private OpenAIClient openAIClient;
    private RecordReaderFactory readerFactory;
    private RecordSetWriterFactory writerFactory;
    private String sourceField;
    private String addedField;
    private String embeddingModelName;
    private Integer embeddingDimensions;
    private boolean includeZeroRecordFlowFiles;

    @Override
    @OnScheduled
    public void onScheduled(ProcessContext context) {
        openAIClient = context.getProperty(OPENAI_CONNECTION_SERVICE).asControllerService(AzureOpenAIConnectionService.class).getOpenAIClient();
        readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        sourceField = context.getProperty(SOURCE_FIELD).getValue();
        addedField = context.getProperty(ADDED_FIELD).getValue();
        embeddingModelName = context.getProperty(EMBEDDING_MODEL_NAME).getValue();
        embeddingDimensions = context.getProperty(EMBEDDING_DIMENSIONS).asInteger();
        includeZeroRecordFlowFiles = context.getProperty(INCLUDE_ZERO_RECORD_FLOWFILES).isSet() ? context.getProperty(INCLUDE_ZERO_RECORD_FLOWFILES).asBoolean() : true;
    }

//    protected void bulkInsert(final List<Map<String, Object>> records) {
//        // In the future, this method will be replaced by calling createItems API
//        // for example, this.container.createItems(records);
//        // currently, no createItems API available in Azure Cosmos Java SDK
//        final ComponentLog logger = getLogger();
//        final OpenAIClient openAIClient = getOpenAIClient();
//    }

//    @Override
//    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
//        FlowFile flowFile = session.get();
//        if (flowFile == null) {
//            return;
//        }
//        final FlowFile original = flowFile;
//        final Map<String, String> originalAttributes = flowFile.getAttributes();
//        try {
//            session.read(flowFile, in -> {
//                RecordReader reader;
//                try {
//                    reader = readerFactory.createRecordReader(flowFile, in, getLogger());
//                } catch (MalformedRecordException | SchemaNotFoundException e) {
//                    throw new RuntimeException(e);
//                }
//
//                final RecordSchema schema;
//                try {
//                    schema = reader.getSchema();
//                } catch (MalformedRecordException e) {
//                    throw new RuntimeException(e);
//                }
//                List<Record> records = new ArrayList<>();
//                List<String> inputTexts = new ArrayList<>();
//
//                // Collect all records and their input texts
//                Record record;
//                while (true) {
//                    try {
//                        if ((record = reader.nextRecord()) == null) break;
//                    } catch (MalformedRecordException e) {
//                        throw new RuntimeException(e);
//                    }
//                    String inputText = (String) record.getValue(inputField);
//                    records.add(record);
//                    inputTexts.add(inputText);
//                }
//
//                if (!inputTexts.isEmpty()) {
//                    // Create embedding options with batch input texts
//                    EmbeddingsOptions embeddingOptions = new EmbeddingsOptions(inputTexts).setDimensions(embeddingDimensions);
//
//                    // Call the embedding API once for all records
//                    Embeddings embeddings = openAIClient.getEmbeddings(embeddingModelName, embeddingOptions);
//
//                    // Collect all embedding vectors
//                    List<List<Float>> embeddingVectors = embeddings.getData().stream()
//                            .map(EmbeddingItem::getEmbedding)
//                            .toList();
//                    getLogger().info("ABC Embedding vectors: {}", embeddingVectors.size());
//                    // Add embedding vectors to each corresponding record
//                    List<Record> updatedRecords = new ArrayList<>();
//                    for (int i = 0; i < records.size(); i++) {
//                        Record originalRecord = records.get(i);
//                        Map<String, Object> updatedFields = new HashMap<>(originalRecord.toMap());
//    //                    Map<String, Object> updatedFields = originalRecord.toMap();
//                        updatedFields.put(embeddingField, embeddingVectors.get(i)); // Add the embedding vector
//
//                        Record updatedRecord = new MapRecord(schema, updatedFields);
//                        updatedRecords.add(updatedRecord);
//                    }
//
//                    // Write the updated records back to a new FlowFile
//                    FlowFile outputFlowFile = session.create();
//                    session.write(outputFlowFile, out -> {
//                        try (RecordSetWriter writer = writerFactory.createWriter(getLogger(), schema, out, originalAttributes)) {
//                            writer.beginRecordSet();
//                            for (Record updatedRecord : updatedRecords) {
//                                writer.write(updatedRecord);
//                            }
//                            writer.finishRecordSet();
//                        } catch (SchemaNotFoundException e) {
//                            throw new RuntimeException(e);
//                        }
//                    });
//
//                    // Transfer the output FlowFile to the success relationship
//                    session.transfer(outputFlowFile, REL_SUCCESS);
//                    session.remove(flowFile);
//                }
//            });
//        } catch (final Exception e) {
//            getLogger().error("Failed to generate embedding vector", e);
//            session.transfer(flowFile, REL_FAILURE);
//        }
//    }
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final Map<String, String> attributes = new HashMap<>();
        final AtomicInteger recordCount = new AtomicInteger();

        final FlowFile original = flowFile;
        final Map<String, String> originalAttributes = flowFile.getAttributes();
        try {
            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(final InputStream in, final OutputStream out) throws IOException {

                    try (final RecordReader reader = readerFactory.createRecordReader(originalAttributes, in, original.getSize(), getLogger())) {

                        // Get the first record and process it before we create the Record Writer. We do this so that if the Processor
                        // updates the Record's schema, we can provide an updated schema to the Record Writer. If there are no records,
                        // then we can simply create the Writer with the Reader's schema and begin & end the Record Set.
                        Record firstRecord = reader.nextRecord();
                        if (firstRecord == null) {
                            final RecordSchema writeSchema = writerFactory.getSchema(originalAttributes, reader.getSchema());
                            try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out, originalAttributes)) {
                                writer.beginRecordSet();

                                final WriteResult writeResult = writer.finishRecordSet();
                                attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                                attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                                attributes.putAll(writeResult.getAttributes());
                                recordCount.set(writeResult.getRecordCount());
                            }
                            return;
                        }

                        firstRecord = myprocess(firstRecord, original, context, 1L);

                        final RecordSchema writeSchema = writerFactory.getSchema(originalAttributes, firstRecord.getSchema());
                        try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out, originalAttributes)) {
                            writer.beginRecordSet();

                            writer.write(firstRecord);

                            Record record;
                            long count = 1L;
                            while ((record = reader.nextRecord()) != null) {
                                final Record processed = myprocess(record, original, context, ++count);
                                writer.write(processed);
                            }

                            final WriteResult writeResult = writer.finishRecordSet();
                            attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                            attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                            attributes.putAll(writeResult.getAttributes());
                            recordCount.set(writeResult.getRecordCount());
                        }
                    } catch (final SchemaNotFoundException e) {
                        throw new ProcessException(e.getLocalizedMessage(), e);
                    } catch (final MalformedRecordException e) {
                        throw new ProcessException("Could not parse incoming data", e);
                    }
                }
            });
        } catch (final Exception e) {
            getLogger().error("Failed to process {}; will route to failure", flowFile, e);
            // Since we are wrapping the exceptions above there should always be a cause
            // but it's possible it might not have a message. This handles that by logging
            // the name of the class thrown.
            Throwable c = e.getCause();
            if (c != null) {
                session.putAttribute(flowFile, "record.error.message", (c.getLocalizedMessage() != null) ? c.getLocalizedMessage() : c.getClass().getCanonicalName() + " Thrown");
            } else {
                session.putAttribute(flowFile, "record.error.message", e.getClass().getCanonicalName() + " Thrown");
            }
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        flowFile = session.putAllAttributes(flowFile, attributes);
        if (!includeZeroRecordFlowFiles && recordCount.get() == 0) {
            session.remove(flowFile);
        } else {
            session.transfer(flowFile, REL_SUCCESS);
        }

        final int count = recordCount.get();
        session.adjustCounter("Records Processed", count, false);
        getLogger().info("Successfully converted {} records for {}", count, flowFile);
    }

    protected Record myprocess(Record record, final FlowFile flowFile, final ProcessContext context, final long count) {
        final RecordPath recordPath = RecordPath.compile(addedField);
        final RecordPathResult result = recordPath.evaluate(record);

        final RecordPath replacementRecordPath = RecordPath.compile(sourceField);

        // If we have an Absolute RecordPath, we need to evaluate the RecordPath only once against the Record.
        // If the RecordPath is a Relative Path, then we have to evaluate it against each FieldValue.

        record = processAbsolutePath(result.getSelectedFields(), replacementRecordPath, record);
        record.incorporateInactiveFields();
        return record;
    }

    private Record processAbsolutePath( final Stream<FieldValue> destinationFields, final RecordPath replacementRecordPath, final Record record) {
        final List<FieldValue> destinationFieldValues = destinationFields.collect(Collectors.toList());
        final List<FieldValue> selectedFields = getSelectedFields(replacementRecordPath, null, record);
        return updateRecord(destinationFieldValues, selectedFields, record);
    }

    private boolean isReplacingRoot(final List<FieldValue> destinationFields) {
        return destinationFields.size() == 1 && !destinationFields.get(0).getParentRecord().isPresent();
    }

    private Record updateRecord(final List<FieldValue> destinationFields, final List<FieldValue> selectedFields, final Record record) {
//        if (isReplacingRoot(destinationFields)) {
//            final Object replacement = getReplacementObject(selectedFields);
//            if (replacement == null) {
//                return record;
//            }
//
//            if (replacement instanceof Record) {
//                return (Record) replacement;
//            }
//
//            final FieldValue replacementFieldValue = (FieldValue) replacement;
//            if (replacementFieldValue.getValue() instanceof Record) {
//                return (Record) replacementFieldValue.getValue();
//            }
//
//            final List<RecordField> fields = selectedFields.stream().map(FieldValue::getField).collect(Collectors.toList());
//            final RecordSchema schema = new SimpleRecordSchema(fields);
//            final Record mapRecord = new MapRecord(schema, new HashMap<>());
//            for (final FieldValue selectedField : selectedFields) {
//                mapRecord.setValue(selectedField.getField(), selectedField.getValue());
//            }
//
//            return mapRecord;
//        } else {
            for (final FieldValue fieldVal : destinationFields) {
                final Object replacementObject = getReplacementObject(selectedFields);
                updateFieldValue(fieldVal, replacementObject);
            }
            return record;
//        }
    }

    private void updateFieldValue(final FieldValue fieldValue, final Object replacement) {
//        if (replacement instanceof FieldValue) {
//            final FieldValue replacementFieldValue = (FieldValue) replacement;
//            fieldValue.updateValue(replacementFieldValue.getValue(), replacementFieldValue.getField().getDataType());
//        } else {
//            fieldValue.updateValue(replacement);
//        }
        fieldValue.updateValue(replacement, new ArrayDataType(RecordFieldType.FLOAT.getDataType()));
    }

    private List<FieldValue> getSelectedFields(final RecordPath replacementRecordPath, final FieldValue fieldValue, final Record record) {
        final RecordPathResult replacementResult = replacementRecordPath.evaluate(record, fieldValue);
        return replacementResult.getSelectedFields().collect(Collectors.toList());
    }

    private Object getReplacementObject(final List<FieldValue> selectedFields) {
        if (selectedFields.size() > 1) {
            final List<RecordField> fields = selectedFields.stream().map(FieldValue::getField).collect(Collectors.toList());
            final RecordSchema schema = new SimpleRecordSchema(fields);
            final Record record = new MapRecord(schema, new HashMap<>());
            for (final FieldValue fieldVal : selectedFields) {
                record.setValue(fieldVal.getField(), fieldVal.getValue());
            }

            return record;
        }

        if (selectedFields.isEmpty()) {
            return null;
        } else {
//            return selectedFields.get(0);
            ArrayList<String> sentences = new ArrayList<>();
            sentences.add(selectedFields.get(0).getValue().toString());
            EmbeddingsOptions embeddingsOptions = new EmbeddingsOptions(sentences).setDimensions(embeddingDimensions);
            Embeddings embeddings = openAIClient.getEmbeddings(embeddingModelName, embeddingsOptions);
            return embeddings.getData().getFirst().getEmbedding();
        }
    }

//    private FieldValue getEmbeddingFieldValue(FieldValue sourceFieldValue) {
//        ArrayList<String> sentences = new ArrayList<>();
//
//        sentences.add(sourceFieldValue.getValue().toString());
//        EmbeddingsOptions embeddingsOptions = new EmbeddingsOptions(sentences).setDimensions(embeddingDimensions);
//        Embeddings embeddings = openAIClient.getEmbeddings(embeddingModelName, embeddingsOptions);
//        // Step 2: Create an EmbeddingOptions request with the input text
//
//        List<Float> embeddingVector = embeddings.getData().getFirst().getEmbedding();
//
//        // Step 4: Convert the embedding vector to a FieldValue compatible format
//        List<FieldValue> fieldValueVector = embeddingVector.stream()
//                .map(FieldValue::create)
//                .collect(Collectors.toList());
//
//        // Return the FieldValue object holding the vector embedding
//        return FieldValue.create(fieldValueVector);
//    }
}
