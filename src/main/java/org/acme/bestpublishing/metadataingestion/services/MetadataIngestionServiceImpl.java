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
package org.acme.bestpublishing.metadataingestion.services;

import org.acme.bestpublishing.error.ProcessingErrorCode;
import org.acme.bestpublishing.exceptions.IngestionException;
import org.acme.bestpublishing.model.BestPubContentModel;
import org.acme.bestpublishing.model.BestPubWorkflowModel;
import org.acme.bestpublishing.services.AlfrescoRepoUtilsService;
import org.acme.bestpublishing.services.AlfrescoWorkflowUtilsService;
import org.acme.bestpublishing.services.BestPubUtilsService;
import org.acme.bestpublishing.services.IngestionService;
import org.alfresco.repo.workflow.WorkflowModel;
import org.alfresco.service.ServiceRegistry;
import org.alfresco.service.cmr.repository.*;
import org.alfresco.service.cmr.workflow.WorkflowInstance;
import org.alfresco.service.namespace.QName;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.io.*;
import java.util.*;

/**
 * Implementation of the IngestionService interface to support metadata ingestion.
 *
 * @author martin.bergljung@marversolutions.org
 * @version 1.0
 */
@Transactional(readOnly = true)
public class MetadataIngestionServiceImpl implements IngestionService {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataIngestionServiceImpl.class);

    private static final String PROCESSING_STATUS_NEW = "new";
    private static final String PROCESSING_STATUS_SUCCESSFUL = "success";
    private static final String PROCESSING_STATUS_ALREADY_PROCESSED = "alreadyProcessed";
    private static final String BOOK_TITLE_UNKNOWN = "Unkown";

    /**
     * Best Publishing util Services
     */
    private AlfrescoRepoUtilsService alfrescoRepoUtilsService;
    private AlfrescoWorkflowUtilsService alfrescoWorkflowUtilsService;
    private BestPubUtilsService bestPubUtilsService;

    //private CreateChapterFolderService createChapterFolder;

    /**
     * Alfresco services
     */
    private ServiceRegistry serviceRegistry;

    /**
     * Workflow timers, values passed in from alfresco-global.properties
     * settings
     */
    private String interruptT1TimerDuration;
    private String interruptT5TimerDuration;
    private String interruptT11TimerDuration;
    private String wait2Check4MetadataTimerDuration;
    private String wait2Check4ContentTimerDuration;

    /**
     * Spring DI
     */

    public void setServiceRegistry(ServiceRegistry serviceRegistry) {
        this.serviceRegistry = serviceRegistry;
    }
    public void setAlfrescoRepoUtilsService(AlfrescoRepoUtilsService alfrescoRepoUtilsService) {
        this.alfrescoRepoUtilsService = alfrescoRepoUtilsService;
    }
    public void setAlfrescoWorkflowUtilsService(AlfrescoWorkflowUtilsService alfrescoWorkflowUtilsService) {
        this.alfrescoWorkflowUtilsService = alfrescoWorkflowUtilsService;
    }
    public void setBestPubUtilsService(BestPubUtilsService bestPubUtilsService) {
        this.bestPubUtilsService = bestPubUtilsService;
    }

    public void setInterruptT1TimerDuration(final String interruptT1TimerDuration) {
        this.interruptT1TimerDuration = interruptT1TimerDuration;
    }

    public void setInterruptT5TimerDuration(final String interruptT5TimerDuration) {
        this.interruptT5TimerDuration = interruptT5TimerDuration;
    }

    public void setInterruptT11TimerDuration(final String interruptT11TimerDuration) {
        this.interruptT11TimerDuration = interruptT11TimerDuration;
    }

    public void setWait2Check4MetadataTimerDuration(final String wait2Check4MetadataTimerDuration) {
        this.wait2Check4MetadataTimerDuration = wait2Check4MetadataTimerDuration;
    }

    public void setWait2Check4ContentTimerDuration(final String wait2Check4ContentTimerDuration) {
        this.wait2Check4ContentTimerDuration = wait2Check4ContentTimerDuration;
    }

   /* public void setCreateChapterFolder(final CreateChapterFolderService createChapterFolder) {
        this.createChapterFolder = createChapterFolder;
    }*/

   /**
     * Interface Implementation
     */

    @Override
    @Transactional(readOnly = false, propagation = Propagation.REQUIRES_NEW)
    public void importZipFileContent(File zipFile, NodeRef alfrescoFolderNodeRef, String isbn) {
        String zipFilename = zipFile.getName().trim();

        if (isValidGenre(zipFilename) == false) {
            LOG.error("Metadata ZIP file [{}] contains an invalid genre name, cannot process it", zipFilename);
            throw new IngestionException(ProcessingErrorCode.METADATA_INGESTION_INVALID_GENRE);
        }

        // Verify that metadata ZIP is new and has not already been processed (is being processed)
        String processingStatus = getZipProcessingStatus(isbn, zipFilename, alfrescoFolderNodeRef);
        if (StringUtils.equals(processingStatus, PROCESSING_STATUS_NEW)) {
            NodeRef zipFileNodeRef = alfrescoRepoUtilsService.createFile(alfrescoFolderNodeRef, zipFile);
            LOG.debug("Metadata ZIP file [{}] uploaded to Alfresco", zipFilename);

            // Get the book title from the main book metadata txt file
            String bookTitle = getBookTitleFromBookMetadataTxt();

            // Start up publishing workflow instance for ISBN
            List<NodeRef> workflowPackageFiles = new ArrayList<>();
            workflowPackageFiles.add(zipFileNodeRef);
            WorkflowInstance newWorkflowInstance = startBestPubWorkflowInstance(
                    isbn, bookTitle, workflowPackageFiles);
            LOG.debug("Publishing workflow has been started for [isbn={}][definition={}][instance={}]",
                    new Object[]{isbn, newWorkflowInstance.getDefinition(), newWorkflowInstance.getId()});
        }
    }

    /**
     * Extracts subject name from zip filename and matches it against the list
     * of valid subject names
     *
     * @return true if a valid subject name was found in filename, otherwise
     * false
     */
    private boolean isValidGenre(String zipFilename) {
        // TODO
        //String subjectNameFromZip = bestPubUtilsService.getBookGenreName(zipFilename);
        //return bestPubUtilsService.getAvailableSubjects().contains(subjectNameFromZip);
        return true;
    }

    /**
     * Check if the metadata ZIP has already been ingested into Alfresco.
     * A metadata ZIP contains metadata for all chapters so no need to ingest more than once.
     *
     * @return "new" or already processed error message
     */
    private String getZipProcessingStatus(String isbn, String zipFilename, NodeRef incomingMetadataFolderNodeRef) {
        // First check if we got a ZIP in the /Data Dictionary/BestPub/Incoming/Metadata folder
        NodeRef zipFileNodeRef = alfrescoRepoUtilsService.getChildByName(incomingMetadataFolderNodeRef, zipFilename);
        if (zipFileNodeRef != null) {
            LOG.error("Metadata [{}] has already been uploaded to Alfresco, " +
                    "cannot process it again [nodeRef={}]", zipFilename, zipFileNodeRef);
            return PROCESSING_STATUS_ALREADY_PROCESSED;
        }

        // Then check if it has already been processed and workflow instance exists,
        // but ZIP file removed from /Data Dictionary/BestPub/Incoming/Metadata
        WorkflowInstance workflowInstance = alfrescoWorkflowUtilsService.getWorkflowInstanceForIsbn(
                BestPubWorkflowModel.BESTPUB_PUBLISHING_WORKFLOW_NAME, isbn);
        if (workflowInstance != null) {
            LOG.error("Metadata [{}] has already been processed, cannot process it again [workflowId={}]",
                    zipFilename, workflowInstance.getId());
            return PROCESSING_STATUS_ALREADY_PROCESSED;
        }

        // Finally check if ISBN folder exists under /BestPub/Published, workflow completed for ISBN and maybe removed,
        // so first 2 checks would not catch this
        /**
        NodeRef rhoFolderNodeRef = alfrescoRepoUtilsService.getNodeByDisplayPath(RHO_FOLDER_NAME);
        NodeRef isbnFolderNodeRef = alfrescoRepoUtilsService.getChildByName(rhoFolderNodeRef, isbn);
        if (isbnFolderNodeRef != null) {
            LOG.error("Metadata [{}] has already been processed successfully, cannot process it again",
                    zipFilename);
            return PROCESSING_STATUS_ALREADY_PROCESSED;
        }
         */

        return PROCESSING_STATUS_NEW;
    }

    /**
     * Get the book title from the passed in Book Metadata Text file.
     *
     * @param bookMetadataTxtIs
     * @return book title or null if could not be found
     */
    private String getBookTitleFromBookMetadataTxt(final InputStream bookMetadataTxtIs) {
        String bookTitle = BOOK_TITLE_UNKNOWN;

        Reader bufferedReader = new BufferedReader(new InputStreamReader((bookMetadataTxtIs)));
        try {
            byte[] metadataText = IOUtils.toByteArray(bookMetadataTxtIs);
            bookTitle = parseTxtForBookTitle(metadataText);
        } catch (IOException e) {
            LOG.error("Could not get the book title from Book Metadata Txt file", e);
        } finally {
            IOUtils.closeQuietly(bufferedReader);
            IOUtils.closeQuietly(bookMetadataTxtIs);
        }

        return bookTitle;
    }

    /**
     * Parse out book title from book metadata properties from text file
     *
     * @param metadataText
     * @return
     */
    private String parseTxtForBookTitle(byte[] metadataText) {
        // TODO: Must be a way to read txt files as properties files...
        return "Unkown";
    }

    /**
     * Starts a new Best Publishing workflow for a specific ISBN with book metadata.
     *
     * @param isbn                 the related ISBN
     * @param workflowPackageFiles the metadata ZIP file to attach to bpm_package
     * @param bookTitle            the book title
     * @return the new Activiti workflow instance
     */
    private WorkflowInstance startBestPubWorkflowInstance(String isbn,
                                                          String bookTitle,
                                                          List<NodeRef> workflowPackageFiles) {
        // Setup workflow properties
        Map<QName, Serializable> props = new HashMap<>();
        props.put(WorkflowModel.PROP_WORKFLOW_DESCRIPTION, "Best Publishing workflow for " + isbn);
        props.put(BestPubWorkflowModel.PROP_RELATED_ISBN, isbn);
        props.put(BestPubContentModel.BookInfoAspect.Prop.BOOK_TITLE, bookTitle);
        props.put(BestPubWorkflowModel.PROP_CONTENT_FOUND, false);
        props.put(BestPubWorkflowModel.PROP_CONTENT_ERROR_FOUND, false);
        props.put(BestPubWorkflowModel.PROP_METADATA_CHAPTER_MATCHING_OK, false);
        props.put(BestPubWorkflowModel.PROP_CHAPTER_FOLDER_HIERARCHY_EXISTS, false);
        props.put(BestPubWorkflowModel.PROP_INTERRUPT_T1_TIMER_DURATION, interruptT1TimerDuration);
        props.put(BestPubWorkflowModel.PROP_INTERRUPT_T5_TIMER_DURATION, interruptT5TimerDuration);
        props.put(BestPubWorkflowModel.PROP_INTERRUPT_T11_TIMER_DURATION, interruptT11TimerDuration);
        props.put(BestPubWorkflowModel.PROP_WAIT_2_CHECK_METADATA_TIMER_DURATION, wait2Check4MetadataTimerDuration);
        props.put(BestPubWorkflowModel.PROP_WAIT_2_CHECK_CONTENT_TIMER_DURATION, wait2Check4ContentTimerDuration);

        // Start it
        return alfrescoWorkflowUtilsService.startWorkflowInstance(BestPubWorkflowModel.BESTPUB_PUBLISHING_WORKFLOW_NAME,
                workflowPackageFiles, props);
    }
}
