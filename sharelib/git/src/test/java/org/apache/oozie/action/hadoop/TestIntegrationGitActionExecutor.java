
/** * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.oozie.action.hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.util.XConfiguration;
//import org.apache.oozie.action.hadoop.GitServer;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class TestIntegrationGitActionExecutor extends ActionExecutorTestCase{

    @Override
    protected void setSystemProps() throws Exception {
        super.setSystemProps();
        setSystemProperty("oozie.service.ActionService.executor.classes", GitActionExecutor.class.getName());
    }

    public void testWhenRepoIsClonedThenGitIndexContentIsReadSuccessfully() throws Exception {
        final Path outputPath = getFsTestCaseDir();
        final Path gitRepo = Path.mergePaths(outputPath, new Path("/repoDir"));
        final Path gitIndex = Path.mergePaths(gitRepo, new Path("/.git/config"));
        
        String localRepo = "git://127.0.0.1/repo.git";
        String actionXml = "<git>" +
                "<resource-manager>" + getJobTrackerUri() + "</resource-manager>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<git-uri>" + localRepo + "</git-uri>"+
                "<destination-uri>" + gitRepo + "</destination-uri>" +
                "</git>";

        Context context = createContext(actionXml);
        final String launcherId = submitAction(context);

        GitServer myGitRepo = null;
        try {
            myGitRepo = new GitServer();
           
            waitUntilYarnAppDoneAndAssertSuccess(launcherId);
        } finally {
            if (myGitRepo != null) {
                myGitRepo.stopAndCleanupReposServer();
            }
        }
        Map<String, String> actionData = LauncherHelper.getActionData(getFileSystem(), context.getActionDir(),
                context.getProtoActionConf());
        assertFalse(LauncherHelper.hasIdSwap(actionData));

        GitActionExecutor ae = new GitActionExecutor();
        ae.check(context, context.getAction());
        assertEquals("launcherId and action.externalId should be the same", launcherId, context.getAction().getExternalId());
        assertEquals("action should have been SUCCEEDED", "SUCCEEDED", context.getAction().getExternalStatus());

        ae.end(context, context.getAction());
        assertEquals("action.status should be OK", WorkflowAction.Status.OK, context.getAction().getStatus());

        assertTrue("could not create test case output path", getFileSystem().exists(outputPath));
        assertTrue("could not save git index", getFileSystem().exists(gitIndex));

        try (InputStream is = getFileSystem().open(gitIndex); ByteArrayOutputStream readContent = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024];
            int length;
            while ((length = is.read(buffer)) != -1) {
                readContent.write(buffer, 0, length);
            }
            String gitIndexContent = readContent.toString(StandardCharsets.UTF_8.name());

            assertTrue("could not read git index", gitIndexContent.toLowerCase().contains("core"));
            assertTrue("could not read git index", gitIndexContent.toLowerCase().contains("remote"));
        }
    }

    private Context createContext(String actionXml) throws Exception {
        GitActionExecutor ae = new GitActionExecutor();

        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());

        FileSystem fs = getFileSystem();
        SharelibUtils.addToDistributedCache(GitActionExecutor.GIT_ACTION_TYPE, fs, getFsTestCaseDir(), protoConf);

        WorkflowJobBean wf = createBaseWorkflow(protoConf, GitActionExecutor.GIT_ACTION_TYPE + "-action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());
        action.setConf(actionXml);

        return new Context(wf, action);
    }

    private String submitAction(Context context) throws Exception {
        GitActionExecutor ae = new GitActionExecutor();

        WorkflowAction action = context.getAction();

        ae.prepareActionDir(getFileSystem(), context);
        ae.submitLauncher(getFileSystem(), context, action);

        String jobId = action.getExternalId();
        String resourceManager = action.getTrackerUri();
        String consoleUrl = action.getConsoleUrl();
        assertNotNull("action.externalId should be filled", jobId);
        assertNotNull("action.trackerUri should be filled", resourceManager);
        assertNotNull("action.consoleUrl should be filled", consoleUrl);

        JobConf jobConf = createJobConf();
        jobConf.set("mapred.job.tracker", resourceManager);

        createJobClient();
        String runningJobExternalId = context.getAction().getExternalId();

        assertNotNull("running job has a valid externalId", runningJobExternalId);

        return runningJobExternalId;
    }

}
