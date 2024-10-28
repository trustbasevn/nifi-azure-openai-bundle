package org.apache.nifi.processors.azure.search;

import com.azure.ai.openai.OpenAIClient;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;

import org.apache.nifi.azure.search.AzureOpenAIConnectionService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class AbstractAzureOpenAIProcessor extends AbstractProcessor {
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Example success relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Example success relationship")
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("If there is an input flowfile, the original input flowfile will be " +
                    "written to this relationship if the operation succeeds.")
            .build();

    public static final PropertyDescriptor OPENAI_CONNECTION_SERVICE = new PropertyDescriptor.Builder()
            .name("SEARCH_CONNECTION_SERVICE")
            .displayName("Azure Search Connection Service")
            .description("If configured, the controller service used to obtain the connection string and access key")
            .required(true)
            .identifiesControllerService(AzureOpenAIConnectionService.class)
            .build();

    protected static final List<PropertyDescriptor> PROPERTIES;
    protected static final Set<Relationship> RELATIONSHIPS;

    static {
        List<PropertyDescriptor> descriptorList = new ArrayList<>();
        descriptorList.add(OPENAI_CONNECTION_SERVICE);

        PROPERTIES = Collections.unmodifiableList(descriptorList);

        Set<Relationship> relationshipSet = new HashSet<>();
        relationshipSet.add(REL_SUCCESS);
        relationshipSet.add(REL_FAILURE);
        relationshipSet.add(REL_ORIGINAL);
        RELATIONSHIPS = Collections.unmodifiableSet(relationshipSet);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }


    private OpenAIClient openAIClient;
    private AzureOpenAIConnectionService connectionService;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final ComponentLog logger = getLogger();
        if (context.getProperty(OPENAI_CONNECTION_SERVICE).isSet()) {
            this.connectionService = context.getProperty(OPENAI_CONNECTION_SERVICE).asControllerService(AzureOpenAIConnectionService.class);
            this.openAIClient = this.connectionService.getOpenAIClient();
        }
    }

    @OnStopped
    public final void onStopped() {
        final ComponentLog logger = getLogger();
        if (connectionService == null && openAIClient != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Closing OpenAI client");
            }
            this.openAIClient = null;
        }
    }

    protected String getURI(final ProcessContext context) {
        return this.connectionService.getURI();
    }

    protected String getAccessKey(final ProcessContext context) {
        return this.connectionService.getAccessKey();
    }

    protected OpenAIClient getOpenAIClient() {
        return openAIClient;
    }
}
