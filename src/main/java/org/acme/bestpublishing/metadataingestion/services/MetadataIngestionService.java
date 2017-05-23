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

import org.alfresco.service.cmr.repository.NodeRef;

import java.io.File;

/**
 * The metadata checker component fetches and processes metadata from three places:
 *
 * 1) Backlist (already published book) metadata from filesystem
 * 2) Frontlist (new book) metadata from filesystem
 * 3) Frontlist metadata from SurveyMonkey (i.e. www.surveymonkey.com)
 *
 * @author martin.bergljung@ixxus.com
 * @author alexandru.neatu@ixxus.com
 * @version 1.0
 */
public interface MetadataIngestionService {

	/**
     * Processes and stores the backlist ZIP in Alfresco and checks if a workflow instance has been created for
     * it. Each backlist ZIP contains CSVs for all the chapters of one book/ISBN.
     * If a workflow instance doesn't exists for an ISBN then start a new one with the ZIP file as workflow package.
	 * 
	 * @param incomingMetadataFolderNodeRef folder node reference for where in Alfresco to store incoming metadata files
     * @param zipFile the backlist metadata zip file that should be processed
     * @param backlistMetadataFilesystemPath the path in the filesystem where backlist metadata can be found in
     *                                       the form of ZIP files
     * @param maxCsvSizeUncompressed the max size of the CSV file, if bigger it will be considered abnormal
     */
	public void processBacklistMetadataZip(final NodeRef incomingMetadataFolderNodeRef,
                                           final File zipFile,
                                           final String backlistMetadataFilesystemPath,
                                           final long maxCsvSizeUncompressed);
	
	/**
     * Processes and extracts the CSVs from the ZIP and then store the CSVs in Alfresco.
     * Each frontlist ZIP contains CSVs for one or more chapters of one book/ISBN. This method is used
     * when the ISBN/Book for a frontlist item (i.e. a book about to be published) is missing metadata (i.e.
     * the authors have not contributed it via SurveyMonkey).
     * A workflow should already have been started (see processFrontlistMetadataFromSurveyMonkey)
     * for any ISBN encountered in these ZIPs.
     *
     * @param incomingMetadataFolderNodeRef folder node reference for where in Alfresco to store incoming metadata files
     * @param zipFile the frontlist metadata zip file that should be processed
     * @param frontlistMetadataFilesystemPath the path in the filesystem where frontlist metadata can be
     *                                        found in the form of ZIP files
     * @param maxCsvSizeUncompressed the max size of the CSV file, if bigger it will be considered abnormal
     */
    public void processFrontlistMetadataZip(final NodeRef incomingMetadataFolderNodeRef,
                                            final File zipFile,
                                            final String frontlistMetadataFilesystemPath,
                                            final long maxCsvSizeUncompressed);



}
