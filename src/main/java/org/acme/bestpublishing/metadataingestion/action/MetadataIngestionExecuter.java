/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package org.acme.bestpublishing.metadataingestion.action;

import org.acme.bestpublishing.actions.AbstractIngestionExecuter;
import org.alfresco.service.cmr.repository.NodeRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedResource;

import java.io.File;

/**
 * The Metadata Ingestion component is called from the scheduled job.
 * The work that should be done by the metadata ingestion
 * is delegated to the Metadata Ingestion Service component.
 *
 * @author martin.bergljung@marversolutions.org
 * @version 1.0
 */
@ManagedResource(
        objectName = "org.acme:application=BestPublishing,type=Ingestion,name=MetadataIngestion",
        description = "BestPub Metadata Ingestion scanning for Book Metadata ZIPs")
public class MetadataIngestionExecuter extends AbstractIngestionExecuter {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataIngestionExecuter.class);

    /**
     * Executer implementation just calls super class
     */
    public void execute() {
        super.execute("Metadata");
    }

    @Override
    public Logger getLog() {
        return LOG;
    }

    /**
     * Process one ZIP file and upload its metadata to Alfresco
     *
     * @param zipFile              the ZIP file that should be processed and uploaded
     * @param extractedISBN the ISBN number that was extracted from ZIP file name
     * @param alfrescoUploadFolderNodeRef the target folder for new ISBN metadata
     * @return true if processed file ok, false if there was an error
     */
    @Override
    public boolean processZipFile(File zipFile, String extractedISBN, NodeRef alfrescoUploadFolderNodeRef) {
        getLog().debug("Processing metadata zip file [{}]", zipFile.getName());

        try {
            ingestionService.importZipFileContent(zipFile, alfrescoUploadFolderNodeRef, extractedISBN);
            return true;
        } catch (Exception e) {
            getLog().error("Error processing metadata zip file " + zipFile.getName(), e);
        }

        return false;
    }
}
