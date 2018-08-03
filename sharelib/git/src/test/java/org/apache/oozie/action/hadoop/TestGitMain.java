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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowJob;

import org.apache.oozie.fluentjob.api.action.EmailActionBuilder;
import org.apache.oozie.fluentjob.api.action.ErrorHandler;
import org.apache.oozie.fluentjob.api.action.GitAction;
import org.apache.oozie.fluentjob.api.action.GitActionBuilder;
import org.apache.oozie.fluentjob.api.factory.WorkflowFactory;
import org.apache.oozie.fluentjob.api.workflow.Workflow;
import org.apache.oozie.fluentjob.api.workflow.WorkflowBuilder;

import org.jdom.Element;
import org.junit.Assert;

import org.apache.oozie.test.MiniOozieTestCase;

public class TestGitMain extends MiniOozieTestCase {

    private GitMain gitmain = null;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        gitmain = new GitMain();
        gitmain.nameNode = getFileSystem().getUri().toString();
    }

    public void testGitRepoMustHaveScheme() throws Exception {
        OozieClient client = this.getClient();
        Properties conf = client.createConfiguration();

        final class GitWorkflowFactory implements WorkflowFactory {
           @Override
           public Workflow create() {
               final GitAction gitAction = GitActionBuilder.create()
                       .withName("git-action")
                       .withResourceManager(getJobTrackerUri())
                       .withNameNode(getNameNodeUri())
                       .withConfigProperty("mapred.job.queue.name", "default")
                       //Note: no URI schema
                       .withGitUri("aURLWithoutAScheme/andSomeMorePathStuff")
                       .withDestinationUri("repoDir")
                       .build();

               final Workflow gitWorkflow = new WorkflowBuilder()
                       .withName("git-workflow")
                       .withDagContainingNode(gitAction).build();

               return gitWorkflow;
           }
        }
        Path workflowUri = new Path(getFsTestCaseDir().toString(), "workflow.xml");
        conf.setProperty(OozieClient.APP_PATH, workflowUri.toString());

        writeHDFSFile(workflowUri, new GitWorkflowFactory().create().asXml());

        String jobId = client.run(conf);

        while (client.getJobInfo(jobId).getStatus() == WorkflowJob.Status.RUNNING) {
            System.out.println("Workflow job running ...");
            Thread.sleep(10 * 1000);
        }

        // Should fail with something akin to:
        // Failing Oozie Launcher, Action Configuration does not have a proper URI repoDir null scheme.
        // org.apache.oozie.action.hadoop.GitMain$GitMainException: Action Configuration does not have a proper URI repoDir null scheme.
        // One needs to look into the YARN logs though to see this as client.getJobLog() is not implemented
        assertTrue(client.getJobInfo(jobId).getStatus() == WorkflowJob.Status.KILLED);
    }

    public void testGitKeyFileIsCopiedToHDFS() throws Exception {
        final Path credentialFilePath = Path.mergePaths(getFsTestCaseDir(), new Path("/key_dir/my_key.dsa"));
        final String credentialFileData = "Key file data";
        Path.mergePaths(getFsTestCaseDir(), new Path("/destDir"));

        writeHDFSFile(credentialFilePath, credentialFileData);

        File localFile = gitmain.getKeyFromFS(credentialFilePath);
        String testOutput = new String(Files.readAllBytes(localFile.toPath()));

        assertEquals("credential file length mismatch", 13, credentialFileData.length());
        assertEquals("credential file data mismatch", credentialFileData, String.valueOf(testOutput));

        FileUtils.deleteDirectory(new File(localFile.getParent()));
    }

    private void writeHDFSFile(Path hdfsFilePath, String fileData) throws IOException {
        try (FSDataOutputStream hdfsFileOS = getFileSystem().create(hdfsFilePath)) {
            hdfsFileOS.write(fileData.getBytes());
            hdfsFileOS.flush();
        }
    }
}
