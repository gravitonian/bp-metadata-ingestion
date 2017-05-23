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

import org.alfresco.error.AlfrescoRuntimeException;
import org.alfresco.repo.security.authentication.AuthenticationUtil;
import org.alfresco.schedule.AbstractScheduledLockedJob;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.StatefulJob;
import uk.co.tandf.bopp.metadatachecker.actions.MetadataCheckerExecuter;

/**
 * Run the Metadata Checker Job
 * <p/>
 * Extends the AbstractScheduledLockedJob class that has job lock service functionality to lock job so
 * it can run safely in a cluster.
 * <p/>
 * Important: implement StatefulJob so the job is not triggered concurrently by the scheduler
 *
 * @author martin.bergljung@ixxus.com
 */
public class MetadataIngestionJob extends AbstractScheduledLockedJob implements StatefulJob {
    @Override
    public void executeJob(JobExecutionContext context) throws JobExecutionException {
        JobDataMap jobData = context.getJobDetail().getJobDataMap();

        // Extract the Metadata Checker to use
        Object metadataCheckerExecuterObj = jobData.get("metadataCheckerExecuter");
        if (metadataCheckerExecuterObj == null || !(metadataCheckerExecuterObj instanceof MetadataCheckerExecuter)) {
            throw new AlfrescoRuntimeException(
                    "MetadataCheckerJob data must contain valid 'metadataCheckerExecuter' reference");
        }

        final MetadataCheckerExecuter metadataCheckerExecuter = (MetadataCheckerExecuter) metadataCheckerExecuterObj;

        AuthenticationUtil.runAs(new AuthenticationUtil.RunAsWork<Object>() {
            public Object doWork() throws Exception {
                metadataCheckerExecuter.execute();
                return null;
            }
        }, AuthenticationUtil.getAdminUserName());
    }
}