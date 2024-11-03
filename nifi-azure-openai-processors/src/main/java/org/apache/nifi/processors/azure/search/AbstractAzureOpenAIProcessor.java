package org.apache.nifi.processors.azure.search;

import com.azure.ai.openai.OpenAIClient;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.azure.search.AzureOpenAIConnectionService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;

import java.util.List;
import java.util.Set;

public abstract class AbstractAzureOpenAIProcessor extends AbstractProcessor {

    static final PropertyDescriptor OPENAI_CONNECTION_SERVICE = new PropertyDescriptor.Builder()
            .name("OPENAI_CONNECTION_SERVICE")
            .displayName("Azure Search Connection Service")
            .description("If configured, the controller service used to obtain the connection string and access key")
            .required(true)
            .identifiesControllerService(AzureOpenAIConnectionService.class)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are successfully transformed will be routed to this relationship")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile cannot be transformed from the configured input format to the configured output format, "
                    + "the unchanged FlowFile will be routed to this relationship")
            .build();

    private static final List<PropertyDescriptor> properties = List.of(OPENAI_CONNECTION_SERVICE);
    private static final Set<Relationship> relationships = Set.of(REL_SUCCESS, REL_FAILURE);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
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
