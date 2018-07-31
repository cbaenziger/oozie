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

import java.util.List;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.jdom.Element;
import org.junit.Assert;

public class TestGitActionExecutor extends ActionExecutorTestCase{

    @SuppressWarnings("unchecked")
    public void testWhenParametersFilledThenConfigurationFieldsPopulated() throws Exception {
        GitActionExecutor ae = new GitActionExecutor();
        assertTrue("Can not find GitMain class in launcher classes",
          ae.getLauncherClasses().contains(GitMain.class));

        Path testKey = new Path(getAppPath().toString() + "/test_key");
        createTestFile(testKey);
        FileSystem fs = getFileSystem();
        fs.setPermission(testKey, FsPermission.valueOf("-r--------"));
        
        final String repoUrl = "https://github.com/apache/oozie";
        final String keyUrl = testKey.toString();
        final String destDir = "repoDir";
        final String branch = "myBranch";
        Element actionXml = XmlUtils.parseXml("<git>" +
                "<resource-manager>" + getJobTrackerUri() + "</resource-manager>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<git-uri>" + repoUrl + "</git-uri>"+
                "<branch>" + branch + "</branch>"+
                "<key-path>" + keyUrl + "</key-path>"+
                "<destination-uri>" + destDir + "</destination-uri>" +
                "</git>");
        
        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());

        WorkflowJobBean wf = createBaseWorkflow(protoConf, GitActionExecutor.GIT_ACTION_TYPE + "-action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());

        Context context = new Context(wf, action);
        Configuration conf = ae.createBaseHadoopConf(context, actionXml);
        ae.setupActionConf(conf, context, actionXml, getFsTestCaseDir());

        assertEquals("git uri must be set", repoUrl, conf.get(GitActionExecutor.GIT_URI));
        assertEquals("key path must be set", keyUrl, conf.get(GitActionExecutor.KEY_PATH));
        assertEquals("branch must be set", branch, conf.get(GitActionExecutor.GIT_BRANCH));
        assertEquals("destination uri must be set", destDir, conf.get(GitActionExecutor.DESTINATION_URI));
    }
    
    public void testAccessKeyPermissionsInsecure() throws Exception {
        GitActionExecutor ae = new GitActionExecutor();

        Path testKey = new Path(getAppPath().toString() + "/test_key");
        createTestFile(testKey);
        // set file permissions to be u=r, g=, o=rw
        FileSystem fs = getFileSystem();
        fs.setPermission(testKey, FsPermission.valueOf("-r-----rw-"));
        
        final String repoUrl = "https://github.com/apache/oozie";
        final String keyUrl = testKey.toString();
        final String destDir = "repoDir";
        final String branch = "myBranch";
        Element actionXml = XmlUtils.parseXml("<git>" +
                "<resource-manager>" + getJobTrackerUri() + "</resource-manager>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<git-uri>" + repoUrl + "</git-uri>"+
                "<branch>" + branch + "</branch>"+
                "<key-path>" + keyUrl + "</key-path>"+
                "<destination-uri>" + destDir + "</destination-uri>" +
                "</git>");

        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());

        WorkflowJobBean wf = createBaseWorkflow(protoConf, GitActionExecutor.GIT_ACTION_TYPE + "-action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());

        Context context = new Context(wf, action);
        Configuration conf = ae.createBaseHadoopConf(context, actionXml);
        try {
            // we expect this to throw an ActionExecutorException:
            ae.setupActionConf(conf, context, actionXml, getFsTestCaseDir());
            Assert.fail("Failed Test");
        } catch (ActionExecutorException e) {
            if (!e.getMessage().contains("insecure")) {
                Assert.fail("Failed Test");
            }
        }
    }
    
    public void testAccessKeyACLsSecure() throws Exception {
        GitActionExecutor ae = new GitActionExecutor();

        Path testKey = new Path(getAppPath().toString() + "/test_key");
        createTestFile(testKey);
        // set file permissions to be secure -- allowing only the owner to read
        FileSystem fs = getFileSystem();
        fs.setPermission(testKey, FsPermission.valueOf("-r--------"));
        fs.setAcl(testKey, AclEntry.parseAclSpec("user::rwx,user:foo:rw-,group::r--,other::---", true));
        
        final String repoUrl = "https://github.com/apache/oozie";
        final String keyUrl = testKey.toString();
        final String destDir = "repoDir";
        final String branch = "myBranch";
        Element actionXml = XmlUtils.parseXml("<git>" +
                "<resource-manager>" + getJobTrackerUri() + "</resource-manager>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<git-uri>" + repoUrl + "</git-uri>"+
                "<branch>" + branch + "</branch>"+
                "<key-path>" + keyUrl + "</key-path>"+
                "<destination-uri>" + destDir + "</destination-uri>" +
                "</git>");

        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());

        WorkflowJobBean wf = createBaseWorkflow(protoConf, GitActionExecutor.GIT_ACTION_TYPE + "-action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());

        Context context = new Context(wf, action);
        Configuration conf = ae.createBaseHadoopConf(context, actionXml);
        ae.setupActionConf(conf, context, actionXml, getFsTestCaseDir());
    }

    public void testAccessKeyACLsInsecure() throws Exception {
        GitActionExecutor ae = new GitActionExecutor();

        Path testKey = new Path(getAppPath().toString() + "/test_key");
        createTestFile(testKey);
        // set file permissions to be secure -- allowing only the owner to read
        FileSystem fs = getFileSystem();
        fs.setPermission(testKey, FsPermission.valueOf("-r--------"));
        fs.setAcl(testKey, AclEntry.parseAclSpec("user::rwx,user:foo:rw-,group::r--,other::r--", true));
        
        final String repoUrl = "https://github.com/apache/oozie";
        final String keyUrl = testKey.toString();
        final String destDir = "repoDir";
        final String branch = "myBranch";
        Element actionXml = XmlUtils.parseXml("<git>" +
                "<resource-manager>" + getJobTrackerUri() + "</resource-manager>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<git-uri>" + repoUrl + "</git-uri>"+
                "<branch>" + branch + "</branch>"+
                "<key-path>" + keyUrl + "</key-path>"+
                "<destination-uri>" + destDir + "</destination-uri>" +
                "</git>");

        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());

        WorkflowJobBean wf = createBaseWorkflow(protoConf, GitActionExecutor.GIT_ACTION_TYPE + "-action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());

        Context context = new Context(wf, action);
        Configuration conf = ae.createBaseHadoopConf(context, actionXml);
    }
    
    private void createTestFile(Path testFile) throws IOException {
        FileSystem fs = getFileSystem();
        FSDataOutputStream file = fs.create(testFile);
        file.writeUTF("");
        file.close();
    }
    
    @Override
    protected void setSystemProps() throws Exception {
        super.setSystemProps();
        setSystemProperty("oozie.service.ActionService.executor.classes", GitActionExecutor.class.getName());
    }
}
