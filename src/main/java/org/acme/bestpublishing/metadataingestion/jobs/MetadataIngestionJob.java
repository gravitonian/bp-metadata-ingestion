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
package org.acme.bestpublishing.metadataingestion.jobs;

import org.acme.bestpublishing.metadataingestion.action.MetadataIngestionExecuter;
import org.alfresco.error.AlfrescoRuntimeException;
import org.alfresco.repo.security.authentication.AuthenticationUtil;
import org.alfresco.schedule.AbstractScheduledLockedJob;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.StatefulJob;

/**
 * Run the Metadata Ingestion Job
 * <p/>
 * Extends the AbstractScheduledLockedJob class that has job lock service functionality to lock job so
 * it can run safely in a cluster.
 * <p/>
 * Important: implement StatefulJob so the job is not triggered concurrently by the scheduler
 *
 * @author martin.bergljung@marversolutions.org
 */
public class MetadataIngestionJob extends AbstractScheduledLockedJob implements StatefulJob {
    @Override
    public void executeJob(JobExecutionContext context) throws JobExecutionException {
        JobDataMap jobData = context.getJobDetail().getJobDataMap();

        // Extract the Metadata Ingestion component to use
        Object metadataIngestionExecuterObj = jobData.get("metadataIngestionExecuter");
        if (metadataIngestionExecuterObj == null || !(metadataIngestionExecuterObj instanceof MetadataIngestionExecuter)) {
            throw new AlfrescoRuntimeException(
                    "MetadataIngestionJob data must contain valid 'metadataIngestionExecuter' reference");
        }

        final MetadataIngestionExecuter metadataIngestionExecuter = (MetadataIngestionExecuter) metadataIngestionExecuterObj;

        AuthenticationUtil.runAs(new AuthenticationUtil.RunAsWork<Object>() {
            public Object doWork() throws Exception {
                metadataIngestionExecuter.execute();
                return null;
            }
        }, AuthenticationUtil.getAdminUserName());
    }
}