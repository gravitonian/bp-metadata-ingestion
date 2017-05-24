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

import org.acme.bestpublishing.metadataingestion.exception.MetadataIngestionException;
import org.alfresco.error.AlfrescoRuntimeException;
import org.alfresco.service.cmr.repository.NodeRef;
import org.acme.bestpublishing.metadataingestion.services.MetadataIngestionService;
import org.acme.bestpublishing.services.AlfrescoRepoUtilsService;
import org.acme.bestpublishing.services.BestPubUtilsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedAttribute;
//import org.springframework.jmx.export.annotation.ManagedMetric;
import org.springframework.jmx.export.annotation.ManagedResource;
//import org.springframework.jmx.support.MetricType;

import java.io.File;
import java.util.Date;

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
public class MetadataIngestionExecuter {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataIngestionExecuter.class);

    /**
     * Best Pub Specific services
     */
    private BestPubUtilsService bestPubUtilsService;
    private MetadataIngestionService metadataIngestionService;
    private AlfrescoRepoUtilsService alfrescoRepoUtilsService;

    /**
     * Metadata Ingestion config
     */
    private String filesystemPathToCheck;
    private String alfrescoFolderPath;
    private String cronExpression;
    private int cronStartDelay;

    /**
     * Metadata Ingestion stats
     */
    private Date lastRunTime;
    private long numberOfRuns;
    private int zipQueueSize;

    /**
     * Spring Dependency Injection
     */
    public void setFilesystemPathToCheck(String filesystemPathToCheck) {
        this.filesystemPathToCheck = filesystemPathToCheck;
    }

    public void setAlfrescoFolderPath(String alfrescoFolderPath) {
        this.alfrescoFolderPath = alfrescoFolderPath;
    }

    public void setCronExpression(String cronExpression) {
        this.cronExpression = cronExpression;
    }

    public void setCronStartDelay(int cronStartDelay) {
        this.cronStartDelay = cronStartDelay;
    }

    public void setMetadataIngestionService(MetadataIngestionService metadataIngestionService) {
        this.metadataIngestionService = metadataIngestionService;
    }

    public void setAlfrescoRepoUtilsService(
            AlfrescoRepoUtilsService alfrescoRepoUtilsService) {
        this.alfrescoRepoUtilsService = alfrescoRepoUtilsService;
    }

    public void setBestPubUtilsService(BestPubUtilsService bestPubUtilsService) {
        this.bestPubUtilsService = bestPubUtilsService;
    }

    /**
     * Managed Properties (JMX)
     */
    @ManagedAttribute(description = "Path to metadata ZIP files")
    public String getFilesystemPathToCheck() {
        return this.filesystemPathToCheck;
    }

    @ManagedAttribute(description = "Cron expression controlling execution")
    public String getCronExpression() {
        return this.cronExpression;
    }

    @ManagedAttribute(description = "Ingestion start delay after bootstrap (ms)")
    public int getCronStartDelay() {
        return this.cronStartDelay;
    }

    @ManagedAttribute(description = "Last time it was called")
    public Date getLastRunTime() {
        return this.lastRunTime;
    }

    @ManagedAttribute(description = "Number of times it has run")
    public long getNumberOfRuns() {
        return this.numberOfRuns;
    }

    //    @ManagedMetric(category="utilization", displayName="ZIP Queue Size",
    //          description="The size of the ZIP File Queue",
    //        metricType = MetricType.COUNTER, unit="zips")
    public long getZipQueueSize() {
        return this.zipQueueSize;
    }

    /**
     * This method is the entry point for Metadata Ingestion.
     */
    public void execute() {
        LOG.debug("Checking for Metadata ZIPs...");

        // Running stats
        lastRunTime = new Date();
        numberOfRuns++;
        zipQueueSize = 0;

        // Get the node references for the /Company Home/Data Dictionary/BestPub/Incoming/Metadata
        // folder where this metadata ingestion action will upload the metadata
        NodeRef metadataIncomingFolderNodeRef = alfrescoRepoUtilsService.getNodeByXPath(alfrescoFolderPath);

        try {
            LOG.debug("File path to check = [{}]", filesystemPathToCheck);

            File folder = new File(filesystemPathToCheck);
            if (!folder.exists()) {
                throw new MetadataIngestionException("Folder to check does not exist.");
            }
            if (!folder.isDirectory()) {
                throw new MetadataIngestionException("The file path must be to a directory.");
            }

            File[] zipFiles = bestPubUtilsService.findFilesUsingExtension(folder, "zip");
            zipQueueSize = zipFiles.length;
            LOG.debug("Found [{}] metadata files", zipFiles.length);

            for (File zipFile : zipFiles) {
                if (processZipFile(zipFile, metadataIncomingFolderNodeRef)) {
                    // All done, delete the ZIP
                    zipFile.delete();

                    //checkerLog.addEvent(new LogEventProcessedSuccessfully(zipFile.getName()), new Date());
                } else {
                    // Something went wrong when processing the zip file,
                    // move it to a directory for ZIPs that failed processing
                    bestPubUtilsService.moveZipToDirForFailedProcessing(zipFile, filesystemPathToCheck);
                }

                zipQueueSize--;
            }

            LOG.debug("Processed [{}] metadata ZIP files", zipFiles.length);
        } catch (Exception e) {
            LOG.error("Encountered an error when ingesting metadata - exiting", e);
        }
    }

    /**
     * Process one metadata ZIP file and upload its content to Alfresco
     *
     * @param zipFile              the ZIP file that should be processed and uploaded
     * @param metadataFolderNodeRef the target folder for new ISBN metadata packages
     * @return true if processed file ok, false if there was an error
     */
    private boolean processZipFile(File zipFile, NodeRef metadataFolderNodeRef)
            throws IOException {
        LOG.debug("Processing zip file [{}]", zipFile.getName());

        String isbn = FilenameUtils.removeExtension(zipFile.getName());
        if (!bestPubUtilsService.isISBN(isbn)) {
            LOG.error("Error processing zip file [{}], filename is not an ISBN number", zipFile.getName());

            return false;
        }

        // Check if ISBN already exists under /Company Home/Data Dictionary/BestPub/Incoming/Metadata
        NodeRef targetMetadataFolderNodeRef = null;
        NodeRef isbnFolderNodeRef = alfrescoRepoUtilsService.getChildByName(metadataFolderNodeRef, isbn);
        if (isbnFolderNodeRef == null) {
            // We got a new ISBN that has not been published before
            // And this means uploading the content package to /Data Dictionary/BestPub/Incoming/Metadata
            targetMetadataFolderNodeRef = metadataFolderNodeRef;

            LOG.debug("Found new ISBN {} that has not been published before, " +
                    "uploading to /Data Dictionary/BestPub/Incoming/Metadata", isbn);
        } else {
            // We got an ISBN that has already been published, so we need to republish
        }

        try {
            metadataIngestionService.importZipFileContent(zipFile, targetMetadataFolderNodeRef, isbn);
            return true;
        } catch (Exception e) {
            LOG.error("Error processing zip file " + zipFile.getName(), e);
        }

        return false;
    }
}
