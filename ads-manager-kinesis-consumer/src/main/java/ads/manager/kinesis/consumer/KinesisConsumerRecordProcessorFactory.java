package ads.manager.kinesis.consumer;/*
 * Copyright 2012-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

/**
 * Used to create new record processors.
 */
public class KinesisConsumerRecordProcessorFactory implements IRecordProcessorFactory {

    private String propertiesFilePath;
    /**
     * Constructor.
     */
    public KinesisConsumerRecordProcessorFactory() {
        super();
    }

    /**
     * Constructor
     * @param propertiesFilePath the path of the properties file
     */
    public KinesisConsumerRecordProcessorFactory(String propertiesFilePath) {
        this.propertiesFilePath = propertiesFilePath;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IRecordProcessor createProcessor() {
        return new KinesisConsumerRecordProcessor(this.propertiesFilePath);
    }

}
