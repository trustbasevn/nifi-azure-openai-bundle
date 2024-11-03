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
package org.apache.nifi.azure.openai;

import java.util.List;

import com.azure.ai.openai.OpenAIClient;
import com.azure.ai.openai.OpenAIClientBuilder;
import com.azure.core.credential.AzureKeyCredential;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;


@Tags({ "azure", "search"})
@CapabilityDescription("Example ControllerService implementation of MyService.")
public class StandardAzureOpenAIConnectionService extends AbstractControllerService implements AzureOpenAIConnectionService {
    private String uri;
    private String accessKey;
    private OpenAIClient openAIClient;

    public static final PropertyDescriptor URI = new PropertyDescriptor.Builder()
            .name("URI")
            .displayName("Azure OpenAI URI")
            .description("Azure OpenAI URI, typically in the form of https://{aisearch}.search.windows.net:443/"
                    + " Note this host URL is for Cosmos DB with Core SQL API"
                    + " from Azure Portal (Overview->URI)")
            .required(true)
            .addValidator(StandardValidators.URI_VALIDATOR)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor ACCESS_KEY = new PropertyDescriptor.Builder()
            .name("ACCESS_KEY")
            .displayName("Azure OpenAI Access Key")
            .description("Azure AI Search Access Key from Azure Portal (Settings->Keys). "
                    + "Choose a read-write key to enable database or container creation at run time")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .sensitive(true)
            .build();

    private static final List<PropertyDescriptor> properties = List.of(
            URI,
            ACCESS_KEY
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     * @param context
     *            the configuration context
     * @throws InitializationException
     *             if unable to create a database connection
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        this.uri = context.getProperty(URI).getValue();
        this.accessKey = context.getProperty(ACCESS_KEY).getValue();
        createOpenAIClient(uri, accessKey);
    }

    @OnShutdown
    @OnDisabled
    public void shutdown() {
        this.openAIClient = null;
    }

    protected void createOpenAIClient(final String uri, final String accessKey) {
        this.openAIClient = new OpenAIClientBuilder()
                .endpoint(uri)
                .credential(new AzureKeyCredential(accessKey))
                .buildClient();
    }

    @Override
    public String getURI() {
        return this.uri;
    }

    @Override
    public String getAccessKey() {
        return this.accessKey;
    }

    @Override
    public OpenAIClient getOpenAIClient() {
        return this.openAIClient;
    }

    public void setOpenAIClient(OpenAIClient  client) {
        this.openAIClient = client;
    }
}
