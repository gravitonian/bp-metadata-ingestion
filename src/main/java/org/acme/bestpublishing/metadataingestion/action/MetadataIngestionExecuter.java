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

    @ManagedAttribute(description = "Checker start delay after bootstrap (ms)")
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
            LOG.debug("Found [{}] content files", zipFiles.length);

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

            LOG.debug("Processed [{}] content ZIP files", zipFiles.length);
        } catch (Exception e) {
            LOG.error("Encountered an error when ingesting content - exiting", e);
        }
    }

    /**
     * Gets all the backlist zip files form file system location passed in, validates the contained CSVs and then
     * sends for processing and saving in Alfresco.
     *
     * @param incomingMetadataFolderNodeRef  folder node reference for where in Alfresco to store incoming metadata files
     * @param backlistMetadataFilesystemPath the path in the filesystem where backlist metadata can be found in
     *                                       the form of ZIP files
     * @return the number of backlist zip files that were processed
     */
    private int processBacklistMetadataFromFilesystem(final NodeRef incomingMetadataFolderNodeRef,
                                                      final String backlistMetadataFilesystemPath) {
        File backlistFolder = new File(backlistMetadataFilesystemPath);
        if (!backlistFolder.exists()) {
            throw new IllegalArgumentException("The " + backlistMetadataFilesystemPath +
                    " path does not exist, cannot check for backlist metadata");
        }

        if (!backlistFolder.isDirectory()) {
            throw new IllegalArgumentException("The " + backlistMetadataFilesystemPath +
                    " path is not for a directory, cannot check for backlist metadata");
        }

        File[] zipFiles = boppUtilsService.findFilesUsingExtension(backlistFolder, "zip");
        zipQueueSize = zipFiles.length;

        for (File zipFile : zipFiles) {
            zipQueueSize--;
            metadataIngestionService.processBacklistMetadataZip(incomingMetadataFolderNodeRef, zipFile,
                    backlistMetadataFilesystemPath, maxCsvSizeUncompressed);
        }

        LOG.debug("Processed [{}] backlist metadata ZIP files", zipFiles.length);

        return zipFiles.length;
    }

    /**
     * Gets all the backlist zip files form file system location passed in, validates the contained CSVs and then
     * sends for processing and saving in Alfresco.
     *
     * @param incomingMetadataFolderNodeRef   folder node reference for where in Alfresco to store incoming metadata files
     * @param frontlistMetadataFilesystemPath the path in the filesystem where frontlist metadata can be found in
     *                                        the form of ZIP files
     * @return the number of frontlist zip files that were processed
     */
    private int processFrontListMetadataFromFilesystem(final NodeRef incomingMetadataFolderNodeRef,
                                                       final String frontlistMetadataFilesystemPath) {
        File frontlistFolder = new File(frontlistMetadataFilesystemPath);
        if (!frontlistFolder.exists()) {
            throw new AlfrescoRuntimeException("The " + frontlistMetadataFilesystemPath +
                    " path does not exist, cannot check for frontlist metadata");
        }

        if (!frontlistFolder.isDirectory()) {
            throw new AlfrescoRuntimeException("The " + frontlistMetadataFilesystemPath +
                    " path is not for a directory, cannot check for frontlist metadata");
        }

        File[] zipFiles = boppUtilsService.findFilesUsingExtension(frontlistFolder, "zip");
        frontlistZipQueueSize = zipFiles.length;

        for (File zipFile : zipFiles) {
            frontlistZipQueueSize--;
            metadataIngestionService.processFrontlistMetadataZip(incomingMetadataFolderNodeRef, zipFile,
                    frontlistMetadataFilesystemPath, maxCsvSizeUncompressed);
        }

        LOG.debug("Processed [{}] frontlist metadata ZIP files", zipFiles.length);

        return zipFiles.length;
    }
}
