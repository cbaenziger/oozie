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
        String workflowUri = getFsTestCaseDir().toString() + "/workflow.xml";
        conf.setProperty(OozieClient.APP_PATH, new Path(workflowUri, "workflow.xml").toString());
        String jobId = client.run(conf);
    	GitActionExecutor ae = new GitActionExecutor();
        assertTrue("Can not find GitMain class in launcher classes",
          ae.getLauncherClasses().contains(GitMain.class));
        
        final String repoUrl = "aURLWithoutAScheme/andSomeMorePathStuff";
        final String destDir = "repoDir";
        final String branch = "myBranch";
        Element actionXml = XmlUtils.parseXml("<git>" +
                "<resource-manager>" + getJobTrackerUri() + "</resource-manager>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<git-uri>" + repoUrl + "</git-uri>"+
                "<branch>" + branch + "</branch>"+
                "<destination-uri>" + destDir + "</destination-uri>" +
                "</git>");
        writeHDFSFile(new Path(workflowUri), actionXml.toString());
        
        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());

        /* XXX
        WorkflowJobBean wf = createBaseWorkflow(protoConf, GitActionExecutor.GIT_ACTION_TYPE + "-action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());

        Context context = new Context(wf, action);
        Configuration conf = ae.createBaseHadoopConf(context, actionXml);
        ae.setupActionConf(conf, context, actionXml, getFsTestCaseDir());*/
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
