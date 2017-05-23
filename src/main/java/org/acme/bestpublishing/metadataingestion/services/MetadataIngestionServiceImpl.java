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

import com.ixxus.taylorfrancis.surveymonkey.parser.SurveyMonkeyParser;
import com.ixxus.taylorfrancis.surveymonkey.parser.model.Survey;
import com.ixxus.taylorfrancis.surveymonkey.parser.model.UnzippedCSV;
import org.alfresco.model.ContentModel;
import org.alfresco.repo.content.MimetypeMap;
import org.alfresco.repo.workflow.WorkflowModel;
import org.alfresco.service.cmr.repository.*;
import org.alfresco.service.cmr.workflow.WorkflowInstance;
import org.alfresco.service.namespace.QName;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import uk.co.tandf.bopp.constants.BoppConstants;
import uk.co.tandf.bopp.metadatachecker.constants.MetadataCheckerConstants;
import uk.co.tandf.bopp.metadatachecker.constants.SurveyMonkeyConstants;
import uk.co.tandf.bopp.metadatachecker.models.CSVContent;
import uk.co.tandf.bopp.metadatachecker.utils.SurveyMonkeyHelper;
import uk.co.tandf.bopp.model.BoppContentModel;
import uk.co.tandf.bopp.model.BoppWorkflowModel;
import uk.co.tandf.bopp.model.ChapterMetadataInfo;
import uk.co.tandf.bopp.services.AlfrescoRepoUtilsService;
import uk.co.tandf.bopp.services.BoppUtilsService;
import uk.co.tandf.bopp.services.CreateChapterFolderService;
import uk.co.tandf.bopp.services.WorkflowUtilsService;
import uk.co.tandf.bopp.services.logger.CheckerLogger;
import uk.co.tandf.bopp.services.logger.events.LogEventAlreadyProcessedSuccessfully;
import uk.co.tandf.bopp.services.logger.events.LogEventErrorProcessing;
import uk.co.tandf.bopp.services.logger.events.LogEventProcessedSuccessfully;

import java.io.*;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static uk.co.tandf.bopp.constants.BoppConstants.RHO_FOLDER_NAME;
import static uk.co.tandf.bopp.model.BoppWorkflowModel.*;

/**
 * This is the implementation for MetadataIngestionService interface.
 *
 * @author martin.bergljung@ixxus.com
 * @author alexandru.neatu@ixxus.com
 * @version 1.0
 */
@Transactional(readOnly = true)
public class MetadataIngestionServiceImpl implements MetadataIngestionService {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataIngestionServiceImpl.class);

    private static final String PROCESSING_STATUS_NEW = "new";
    private static final String PROCESSING_STATUS_SUCCESSFUL = "success";
    private static final String PROCESSING_STATUS_ALREADY_PROCESSED = "alreadyProcessed";
    private static final String BOOK_TITLE_UNKNOWN = "Unkown";

    /**
     * BOPP util Services
     */
    private AlfrescoRepoUtilsService alfrescoRepoUtilsService;

    private WorkflowUtilsService workflowUtilsService;
    private BoppUtilsService boppUtilsService;
    private CheckerLogger checkerLog;
    private CreateChapterFolderService createChapterFolder;

    /**
     * Alfresco services
     */
    private ContentService contentService;
    private NodeService nodeService;

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
     * Spring Dependency Injection
     */


    public void setWorkflowUtilsService(final WorkflowUtilsService workflowUtilsService) {
        this.workflowUtilsService = workflowUtilsService;
    }

    public void setAlfrescoRepoUtilsService(final AlfrescoRepoUtilsService alfrescoRepoUtilsService) {
        this.alfrescoRepoUtilsService = alfrescoRepoUtilsService;
    }

    public void setNodeService(final NodeService nodeService) {
        this.nodeService = nodeService;
    }

    public void setBoppUtilsService(final BoppUtilsService boppUtilsService) {
        this.boppUtilsService = boppUtilsService;
    }

    public void setContentService(final ContentService contentService) {
        this.contentService = contentService;
    }

    public void setCheckerLog(final CheckerLogger checkerLog) {
        this.checkerLog = checkerLog;
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

    public void setCreateChapterFolder(final CreateChapterFolderService createChapterFolder) {
        this.createChapterFolder = createChapterFolder;
    }

    /**
     * Interface implementation
     */

    @Override
    @Transactional(readOnly = false, propagation = Propagation.REQUIRES_NEW)
    public void processBacklistMetadataZip(final NodeRef incomingMetadataFolderNodeRef,
                                           final File zipFile,
                                           final String backlistMetadataFilesystemPath,
                                           final long maxCsvSizeUncompressed) {
        try {
            if (validateMetadataZip(zipFile, maxCsvSizeUncompressed) == false) {
                // Now move the ZIP to a directory for ZIPs that failed processing
                boppUtilsService.moveZipToDirForFailedProcessing(zipFile, backlistMetadataFilesystemPath);

                // ZIP contains abnormally big CSV, abort
                return;
            }

            String processingResult = processAndStoreBacklistZipFile(zipFile, incomingMetadataFolderNodeRef);
            if (StringUtils.equals(processingResult, PROCESSING_STATUS_SUCCESSFUL)) {
                checkerLog.addEvent(new LogEventProcessedSuccessfully(zipFile.getName()), new Date());
                LOG.debug("Deleting successfully processed backlist [{}] file from filesystem", zipFile.getName());
                zipFile.delete();
            } else if (StringUtils.equals(processingResult, PROCESSING_STATUS_ALREADY_PROCESSED)) {
                checkerLog.addEvent(new LogEventAlreadyProcessedSuccessfully(zipFile.getName()), new Date());
                LOG.debug("Deleting already processed backlist [{}] file from filesystem", zipFile.getName());
                zipFile.delete();
            } else {
                checkerLog.addEvent(new LogEventErrorProcessing(zipFile.getName(), processingResult), new Date());

                // Delete the uploaded ZIP file from Alfresco, needs to be fixed and loaded again
                NodeRef zipFileNodeRef = alfrescoRepoUtilsService.getChildByName(
                        incomingMetadataFolderNodeRef, zipFile.getName());
                if (zipFileNodeRef != null) {
                    nodeService.deleteNode(zipFileNodeRef);
                }

                // Now move the ZIP to a directory for ZIPs that failed processing
                boppUtilsService.moveZipToDirForFailedProcessing(zipFile, backlistMetadataFilesystemPath);
            }
        } catch (Exception e) {
            LOG.error("Error writing Log Event to metadata checker log: ", e);
        }
    }

    @Override
    @Transactional(readOnly = false, propagation = Propagation.REQUIRES_NEW)
    public void processFrontlistMetadataZip(final NodeRef incomingMetadataFolderNodeRef,
                                            final File zipFile,
                                            final String frontlistMetadataFilesystemPath,
                                            final long maxCsvSizeUncompressed) {
        try {
            if (validateMetadataZip(zipFile, maxCsvSizeUncompressed) == false) {
                // Now move the ZIP to a directory for ZIPs that failed processing
                boppUtilsService.moveZipToDirForFailedProcessing(zipFile, frontlistMetadataFilesystemPath);

                // ZIP contains abnormally big CSV, abort
                return;
            }

            String processingResult = processFrontlistZipFile(zipFile, incomingMetadataFolderNodeRef);
            if (StringUtils.equals(processingResult, PROCESSING_STATUS_SUCCESSFUL)) {
                checkerLog.addEvent(new LogEventProcessedSuccessfully(zipFile.getName()), new Date());
                LOG.debug("Deleting successfully processed frontlist [{}] file from filesystem", zipFile.getName());
                zipFile.delete();
            } else {
                checkerLog.addEvent(new LogEventErrorProcessing(zipFile.getName(), processingResult), new Date());

                // Delete the uploaded ZIP file from Alfresco, needs to be fixed and loaded again
                NodeRef zipFileNodeRef = alfrescoRepoUtilsService.getChildByName(
                        incomingMetadataFolderNodeRef, zipFile.getName());
                if (zipFileNodeRef != null) {
                    nodeService.deleteNode(zipFileNodeRef);
                }

                // Now move the ZIP to a directory for ZIPs that failed processing
                boppUtilsService.moveZipToDirForFailedProcessing(zipFile, frontlistMetadataFilesystemPath);
            }
        } catch (Exception e) {
            LOG.error("Error writing Log Event to metadata checker log: ", e);
        }
    }





    /**
     * Walk through each CSV entry in the ZIP file and make sure it does not have an abnormal size uncompressed.
     * Some ZIPs have been found to have CSV files with sizes in the range of 150MB, which is because
     * a lot of CSV file rows were generated empty, so the ZIP looks normal with a size of ~1MB compressed...
     *
     * @param zipFile the File object for the ZIP file to be validated
     * @param maxCsvSizeUncompressed the max size of the CSV file, if bigger it will be considered abnormal
     * @return true if it looks like a normal ZIP, false if it looks like a CSV might not have been
     * generated properly and has abnormal size (i.e. > maxCsvSizeUncompressed)
     */
    private boolean validateMetadataZip(final File zipFile, long maxCsvSizeUncompressed) {
        try {
            ZipFile zf = new ZipFile(zipFile);
            Enumeration e = zf.entries();
            while (e.hasMoreElements()) {
                ZipEntry ze = (ZipEntry) e.nextElement();
                String name = ze.getName();
                long uncompressedSize = ze.getSize();

                // Make sure we are checking only CSV files
                if (name.toLowerCase().endsWith(BoppConstants.CSV_FILENAME_EXTENSION)) {
                    if (uncompressedSize > maxCsvSizeUncompressed) {
                        String msg = "CSV file " + name + " has abnormal size " + (uncompressedSize / 1000) + "KB";
                        LOG.error(msg + ", aborting processing of " + zipFile.getName());
                        checkerLog.addEvent(new LogEventErrorProcessing(zipFile.getName(), msg), new Date());
                        return false;

                    }
                }
            }
        } catch (Exception ex) {
            LOG.error("Could not validate ZIP file " + zipFile.getName() + ", processing aborted", ex);
            return false;
        }

        return true;
    }



    /**
     * Process a backlist metadata ZIP file and save it to Alfresco. The ZIP file name follows the naming convention:
     * "{isbn13}_{subject} {from chap #}-{to chap #}.zip".
     * For example:
     * 9780203807217_Sports and Leisure_Chapter 1-28.zip Or could also just be
     * 9780203807217_Sports and Leisure.zip, without the Chapter... bit.
     * <p/>
     * Check whether or not an workflow instance is associated with ZIP file ISBN, if not start one.
     *
     * @param zipFile                       the ZIP file with backlist metadata
     * @param incomingMetadataFolderNodeRef the folder in Alfresco where the metadata should be stored
     * @return "success" if processing of ZIP was successful, otherwise error
     * message
     */
    private String processAndStoreBacklistZipFile(final File zipFile, final NodeRef incomingMetadataFolderNodeRef) {
        String zipFilename = zipFile.getName().trim();

        String isbn = boppUtilsService.getISBNfromFilename(zipFilename);
        if (isbn == null) {
            LOG.error("Backlist metadata ZIP filename [{}] doesn't contain an ISBN number, cannot process it", zipFilename);
            return "Filename is missing ISBN number";
        }

        if (isValidSubject(zipFilename) == false) {
            LOG.error("Backlist metadata ZIP filename [{}] contains an invalid subject name, cannot process it", zipFilename);
            return "Filename has invalid subject name";
        }

        String bookTitle = "";
        NodeRef zipFileNodeRef = null;

        // Verify that backlist metadata ZIP is new and has not already been processed (is being processed)
        String processingStatus = getBacklistZipProcessingStatus(isbn, zipFilename, incomingMetadataFolderNodeRef);
        if (StringUtils.equals(processingStatus, PROCESSING_STATUS_NEW)) {
            zipFileNodeRef = alfrescoRepoUtilsService.createFile(incomingMetadataFolderNodeRef, zipFile);
            LOG.debug("Backlist metadata ZIP filename [{}] uploaded to Alfresco", zipFilename);

            // Now check if the CSVs contained in the ZIP contains duplicate
            // Chapter Titles,
            // Should not be the case for Backlist items but have happened
            List<NodeRef> nodeRefs = new ArrayList<>();
            nodeRefs.add(zipFileNodeRef);
            List<ChapterMetadataInfo> chapterMetadataList = null;
            try {
                chapterMetadataList = boppUtilsService.extractChapterMetadata(nodeRefs);
                bookTitle = getBookTitle(chapterMetadataList);
            } catch (Exception e) {
                e.printStackTrace();
                LOG.error("Could not extract CSVs ", e);
                return "Could not extract CSVs [" + e + "]";
            }
            Set<String> uniqueChapterTitleList = new HashSet<>();
            Map<Integer, String> chapNo2TitleDuplicateMap = new HashMap<>();
            for (ChapterMetadataInfo chapterMetadataInfo : chapterMetadataList) {
                String chapterTitle = chapterMetadataInfo.getSurvey().getBasicSurveyFields().getBook().getChapterTitle();
                boolean duplicate = !uniqueChapterTitleList.add(chapterTitle);
                if (duplicate) {
                    chapNo2TitleDuplicateMap.put(chapterMetadataInfo.getChapterNr(), chapterTitle);
                }
            }
            if (!chapNo2TitleDuplicateMap.isEmpty()) {
                String duplicates = "";
                for (Map.Entry<Integer, String> duplicateChapterTitle : chapNo2TitleDuplicateMap.entrySet()) {
                    duplicates += duplicateChapterTitle.getKey() + "-" + duplicateChapterTitle.getValue() + ",";
                }
                LOG.error("Backlist metadata ZIP [{}] has CSVs with duplicate chapter title [{}], cannot process it",
                        zipFilename, duplicates);
                return "CSVs contains duplicate chapter titles [" + duplicates + "]";
            }

            // Also check if any CSV/Survey is missing chapter title
            String chapterNoMissingTitle = "";
            for (ChapterMetadataInfo chapterMetadataInfo : chapterMetadataList) {
                String chapterTitle = chapterMetadataInfo.getSurvey().getBasicSurveyFields().getBook().getChapterTitle();
                if (StringUtils.isBlank(chapterTitle)) {
                    chapterNoMissingTitle += chapterMetadataInfo.getChapterNr();
                }
            }
            if (StringUtils.isNotBlank(chapterNoMissingTitle)) {
                LOG.error("Backlist metadata ZIP [{}] has CSVs with missing chapter title [{}], cannot process it",
                        zipFilename, chapterNoMissingTitle);
                return "CSVs contains missing chapter titles [" + chapterNoMissingTitle + "]";
            }

            // Start up workflow instance for Backlist ISBN
            List<NodeRef> workflowPackageFiles = new ArrayList<>();
            workflowPackageFiles.add(zipFileNodeRef);
            WorkflowInstance newWorkflowInstance = startBoppWorkflowInstance(
                    isbn, workflowPackageFiles, bookTitle, BoppWorkflowModel.BOOK_METADATA_TYPE_BACKLIST);
            LOG.debug("Backlist workflow has been started for [isbn={}][definition={}][instance={}]",
                    new Object[]{isbn, newWorkflowInstance.getDefinition(), newWorkflowInstance.getId()});

            processingStatus = PROCESSING_STATUS_SUCCESSFUL;
        }

        return processingStatus;
    }

    /**
     * Check if the backlist metadata ZIP has already been ingested into Alfresco.
     * A backlist metadata ZIP contains metadata for all chapters so no need to ingest more than once.
     *
     * @return "new" or already processed error message
     */
    private String getBacklistZipProcessingStatus(String isbn, String zipFilename, NodeRef incomingMetadataFolderNodeRef) {
        // First check if we got a ZIP in the /Data Dictionary/Incoming/Metadata folder
        NodeRef zipFileNodeRef = alfrescoRepoUtilsService.getChildByName(incomingMetadataFolderNodeRef, zipFilename);
        if (zipFileNodeRef != null) {
            LOG.error("Backlist metadata [{}] has already been uploaded to Alfresco, " +
                    "cannot process it again [nodeRef={}]", zipFilename, zipFileNodeRef);
            return PROCESSING_STATUS_ALREADY_PROCESSED;
        }

        // Then check if it has already been processed and workflow instance exists,
        // but ZIP file removed from /Data Dictionary/Incoming/Metadata
        WorkflowInstance workflowInstance = workflowUtilsService.getWorkflowInstanceForIsbn(
                BoppWorkflowModel.BOPP_INGEST_AND_PUBLISH_WORKFLOW_NAME, isbn);
        if (workflowInstance != null) {
            LOG.error("Backlist metadata [{}] has already been processed, cannot process it again [workflowId={}]",
                    zipFilename, workflowInstance.getId());
            return PROCESSING_STATUS_ALREADY_PROCESSED;
        }

        // Finally check if ISBN folder exists under /RHO, workflow completed for ISBN and maybe removed,
        // so first 2 checks would not catch this
        NodeRef rhoFolderNodeRef = alfrescoRepoUtilsService.getNodeByDisplayPath(RHO_FOLDER_NAME);
        NodeRef isbnFolderNodeRef = alfrescoRepoUtilsService.getChildByName(rhoFolderNodeRef, isbn);
        if (isbnFolderNodeRef != null) {
            LOG.error("Backlist metadata [{}] has already been processed successfully, cannot process it again",
                    zipFilename);
            return PROCESSING_STATUS_ALREADY_PROCESSED;
        }

        return PROCESSING_STATUS_NEW;
    }

    /**
     * Extracts subject name from zip filename and matches it against the list
     * of valid subject names
     *
     * @return true if a valid subject name was found in filename, otherwise
     * false
     */
    private boolean isValidSubject(String zipFilename) {
        String subjectNameFromZip = boppUtilsService.getBookSubjectName(zipFilename);
        return boppUtilsService.getAvailableSubjects().contains(subjectNameFromZip);
    }

    /**
     * Get the book title from the first CSV in the ZIP that has this field set.
     *
     * @param chapterMetadataList a list of chapter information extracted from the ZIP file
     * @return book title, or "Unknown" if it could not be found
     */
    private String getBookTitle(final List<ChapterMetadataInfo> chapterMetadataList) {
        String bookTitle = BOOK_TITLE_UNKNOWN;

        for (ChapterMetadataInfo chapterMetadataInfo : chapterMetadataList) {
            bookTitle = chapterMetadataInfo.getSurvey().getBasicSurveyFields().getBook().getBookTitle();
            if (StringUtils.isNotBlank(bookTitle)) {
                break;
            }
        }

        return bookTitle;
    }

    /**
     * Get the book title from the passed in csv if it has this field set.
     *
     * @param csvIS
     * @return book title or null if could not be found
     */
    private String getBookTitleFromCSV(final InputStream csvIS, final InputStream csvIS2) {
        String bookTitle = BOOK_TITLE_UNKNOWN;

        Reader bufferedReader = new BufferedReader(new InputStreamReader((csvIS)));
        try {
            byte[] csv1 = IOUtils.toByteArray(csvIS);
            byte[] csv2 = csvIS2 == null ? null : IOUtils.toByteArray(csvIS2);
            List<Survey> surveysInCSV = csv2 == null ? SurveyMonkeyParser.parse(csv1) : SurveyMonkeyParser.parse(csv1, csv2);
            bookTitle = surveysInCSV.get(0).getBasicSurveyFields().getBook().getBookTitle();
        } catch (IOException e) {
            LOG.error("Could not get the book title from CSV file", e);
        } finally {
            IOUtils.closeQuietly(bufferedReader);
            IOUtils.closeQuietly(csvIS);
        }

        return bookTitle;
    }

    /**
     * Extract the CSV files from the ZIP file and save them to Alfresco. The
     * ZIP file name follows the naming convention
     * "{isbn13}_{subject} {from chap #}-{to chap #}.zip". For example:
     * 9780203807321_Sports and Leisure_Chapter 4-12.zip It will not contain all
     * chapters for an ISBN but the missing chapters, such as for example
     * chapter 4,8,12. Note. workflow for related ISBNs should already have been
     * started.
     *
     * @param zipFile                       the ZIP file with frontlist metadata CSVs
     * @param incomingMetadataFolderNodeRef the folder in Alfresco where the CSV files should be stored
     * @return true , or false it it could not be processed
     */
    private String processFrontlistZipFile(final File zipFile, final NodeRef incomingMetadataFolderNodeRef) {
        String zipFilename = zipFile.getName().trim();
        String isbn = boppUtilsService.getISBNfromFilename(zipFilename);
        if (isbn == null) {
            LOG.error("Frontlist metadata ZIP filename [{}] doesn't contain an ISBN number, cannot process it", zipFilename);
            return "Filename is missing ISBN number";
        }


        InputStream is = null;
        try {
            is = new FileInputStream(zipFile);
        } catch (FileNotFoundException e) {
            try {
                LOG.error("ZIP file " + zipFile.getCanonicalPath() + " could not be opened", e);
                return "ZIP file could not opened: " + e.getMessage();
            } catch (IOException e1) {
                e1.printStackTrace();
                return "ZIP file could not opened: " + e1.getMessage();
            }
        }

        Map<String, UnzippedCSV> csvFiles = null;
        try {
            csvFiles = SurveyMonkeyParser.getCSVFilesFromZip(is);
            for (UnzippedCSV csvFile : csvFiles.values()) {
                Reader bufferedReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(csvFile.getContent())));
                splitAndStoreSurveyMonkeyCSVFileByISBN(bufferedReader, incomingMetadataFolderNodeRef);
            }

        } finally {
            IOUtils.closeQuietly(is);
        }

        // Check if workflow is started.
        WorkflowInstance workflowInstance = workflowUtilsService.getWorkflowInstanceForIsbn(BoppWorkflowModel.BOPP_INGEST_AND_PUBLISH_WORKFLOW_NAME, isbn);
        if (workflowInstance == null) {
            // We should not start workflow but report an error as it
            // should have been started by
            // incoming surveys from SurveyMonkey
            LOG.error("Frontlist metadata workflow does not exist for [{}], " + "have all authors failed to supply surveys via SurveyMonkey?", isbn);

            return "Missing workflow instance for incoming frontlist ZIP";
        } else {
            if (csvFiles != null) {
                LOG.debug("Found [{}] csv files in zip [{}]", new Object[]{csvFiles.size(), zipFilename});
                // TFBCRI-14
                boolean updatedMetadata = false;
                // Update the workflow with the new metadata
                for (String csvTitle : csvFiles.keySet()) {
                    LOG.debug("Parsing csv files for " + csvTitle);
                    UnzippedCSV currentCSV = csvFiles.get(csvTitle);
                    try {
                        List<Survey> currentSurveyList = SurveyMonkeyParser.parse(currentCSV.getContent());
                        for (Survey currentSurvey : currentSurveyList) {
                            String title = currentSurvey.getBasicSurveyFields().getBook().getBookTitle();
                            Collection<String> chapterTitles = workflowUtilsService.getCollectionProcessVariable(workflowInstance, VAR_CHAPTER_LIST);
                            chapterTitles.add(title);
                            workflowUtilsService.setProcessVariable(workflowInstance.getId(), VAR_CHAPTER_LIST, chapterTitles);
                            int chapterCount = chapterTitles.size();
                            workflowUtilsService.setProcessVariable(workflowInstance.getId(), VAR_CHAPTER_COUNT, chapterCount);
                            LOG.debug("Added process variables title [{}] and chapterCount [{}] for isbn [{}}", new Object[]{title, chapterCount, isbn});
                            String bookSubject = workflowUtilsService.getStringProcessVariable(workflowInstance, VAR_BOOK_SUBJECT);
                            createChapterFolder.createChapterFolder("MetadataIngestionService[" + isbn + "]", isbn, bookSubject, title, chapterCount, chapterTitles);
                            updatedMetadata = true;
                        }
                    } catch (IOException ioe) {
                        LOG.error("Couldn't parse CSV [{}] in zip [{}]. Error: {}", new Object[]{currentCSV.getEntry().getName(), csvTitle, ioe});
                    }
                }

                // Workflow has already started. If we updated the metadata, we have to force the process to review it again
                if (updatedMetadata) {
                    workflowUtilsService.setProcessVariable(workflowInstance.getId(), BoppWorkflowModel.VAR_METADATA_COMPLETE, false);
                }
            }
        }

        return PROCESSING_STATUS_SUCCESSFUL;
    }


    /**
     * Start workflow to handle multiple authors per chapter metadata
     *
     * @param nodeRefParam
     * @param csvFileNameParam the CSV file that has the multiple authors per chapter occurence
     * @param isbn             related ISBN
     */
    private void startWorkflowForMultipleAuthors(final String nodeRefParam,
                                                 final String csvFileNameParam,
                                                 final String isbn) {
        WorkflowInstance workflowInstance = workflowUtilsService.getWorkflowInstanceForNodeRefs(
                BoppWorkflowModel.BOPP_AUTHOR_MANAGEMENT_WORKFLOW_NAME, nodeRefParam);
        if (workflowInstance != null) {
            LOG.debug("The workflow [{}] is already started for files: [{}]",
                    BoppWorkflowModel.BOPP_AUTHOR_MANAGEMENT_WORKFLOW_NAME, csvFileNameParam);
        } else {
            // Set up empty package
            List<NodeRef> workflowPackageFiles = new ArrayList<>();

            // Setup workflow variables such as related ISBN
            Map<QName, Serializable> props = new HashMap<>();
            props.put(BoppWorkflowModel.PROP_RELATED_ISBN, isbn);
            props.put(BoppWorkflowModel.PROP_CSV_NAME, csvFileNameParam);
            props.put(BoppWorkflowModel.PROP_CSV_NODEREF, nodeRefParam);

            // Start it
            workflowUtilsService.startWorkflowInstance(BoppWorkflowModel.BOPP_AUTHOR_MANAGEMENT_WORKFLOW_NAME,
                    workflowPackageFiles, props);
        }
    }

    /**
     * Start to loop the first list with position 12 and check the elements in
     * the second list
     *
     * @param values1
     * @param values2
     * @return true if just the author's name is different
     */
    private boolean isJustAuthorNameDiffrent(final List<String> values1, final List<String> values2) {
        // we start from position 12 because the first ones are not related to
        // the survey's questions
        int startPosition = 12;
        boolean result = true;
        for (int i = startPosition; i < values1.size(); i++) {
            if (!values2.contains(values1.get(i))) {
                result = false;
                break;
            }
        }
        return result;
    }



    /**
     * Store passed in String content as a new CSV file node in passed in
     * Alfresco folder. The SurveyMonkey REST API is returning JSON so we create
     * CSV files from JSON and store in Alfresco.
     *
     * @param parentFolderNodeRef where to store the new CSV file
     * @param csvFileContent      the content of the new CSV file
     * @param isbn                the ISBN number that the new CSV file is associated with
     * @return the Alfresco node reference for the newly created CSV file
     */
    private NodeRef storeFrontlistCSVFileInAlfresco(final NodeRef parentFolderNodeRef, final String csvFileContent, final String isbn) {
        String csvFilename = isbn + "_" + boppUtilsService.formatDate(BoppConstants.DATETIME_FORMAT_IN_FRONTLIST_CSV_FILENAME, new Date()) + BoppConstants.CSV_FILENAME_EXTENSION;

        NodeRef newCSVNodeRef = alfrescoRepoUtilsService.createFileMetadataOnly(parentFolderNodeRef, csvFilename);
        ContentWriter contentWriter = contentService.getWriter(newCSVNodeRef, ContentModel.PROP_CONTENT, true);
        contentWriter.setMimetype(MimetypeMap.MIMETYPE_TEXT_CSV);
        contentWriter.putContent(csvFileContent);

        LOG.debug("Created new CSV file [{}] and stored in Alfresco [{}]", csvFilename, newCSVNodeRef);

        return newCSVNodeRef;
    }

    /**
     * Startes a new BOPP Ingest and Publish workflow for a specific ISBN with
     * backlist or frontlist book metadata.
     *
     * @param isbn                 the related ISBN
     * @param workflowPackageFiles the CSVs or ZIP file to attach to bpm_package
     * @param bookTitle            the book title
     * @param bookMetadataType     "frontlist" (book to be published) or "backlist" (book already
     *                             published)
     * @return the new Activiti workflow instance
     */
    private WorkflowInstance startBoppWorkflowInstance(final String isbn, final List<NodeRef> workflowPackageFiles, final String bookTitle, final String bookMetadataType) {
        // Setup workflow properties
        Map<QName, Serializable> props = new HashMap<>();
        props.put(WorkflowModel.PROP_WORKFLOW_DESCRIPTION, "BOPP Ingest & Publish for " + isbn);
        props.put(BoppWorkflowModel.PROP_RELATED_ISBN, isbn);
        props.put(BoppContentModel.BookMetadataAspect.Prop.BOOK_TITLE, bookTitle);
        props.put(BoppWorkflowModel.PROP_BOOK_METADATA_TYPE, bookMetadataType);
        props.put(BoppWorkflowModel.PROP_CONTENT_FOUND, false);
        props.put(BoppWorkflowModel.PROP_CONTENT_ERROR_FOUND, false);
        props.put(BoppWorkflowModel.PROP_METADATA_CHAPTER_MATCHING_OK, false);
        props.put(BoppWorkflowModel.PROP_CHAPTER_FOLDER_HIERARCHY_EXISTS, false);
        props.put(BoppWorkflowModel.PROP_INTERRUPT_T1_TIMER_DURATION, interruptT1TimerDuration);
        props.put(BoppWorkflowModel.PROP_INTERRUPT_T5_TIMER_DURATION, interruptT5TimerDuration);
        props.put(BoppWorkflowModel.PROP_INTERRUPT_T11_TIMER_DURATION, interruptT11TimerDuration);
        props.put(BoppWorkflowModel.PROP_WAIT_2_CHECK_METADATA_TIMER_DURATION, wait2Check4MetadataTimerDuration);
        props.put(BoppWorkflowModel.PROP_WAIT_2_CHECK_CONTENT_TIMER_DURATION, wait2Check4ContentTimerDuration);

        // Start it
        return workflowUtilsService.startWorkflowInstance(BoppWorkflowModel.BOPP_INGEST_AND_PUBLISH_WORKFLOW_NAME, workflowPackageFiles, props);
    }

    private void storeNonRHOCSV(String content, String isbn, NodeRef parent) {
        NodeRef nonRHOParent = alfrescoRepoUtilsService.getOrCreateFolder(parent, "NonRHO");
        storeFrontlistCSVFileInAlfresco(nonRHOParent, content, isbn);
    }
}
