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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;
import java.io.IOException;
import java.lang.Class;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URISyntaxException;

import java.io.File;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.oozie.action.hadoop.ActionExecutorTestCase.Context;
import org.apache.oozie.action.hadoop.SharelibUtils;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.command.wf.ActionXCommand.ActionExecutorContext;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.test.XTestCase.Predicate;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.WorkflowJobBean;
import org.jdom.Element;

import org.apache.oozie.test.XFsTestCase;

public class TestGitMain extends XFsTestCase {

    private static List<String> getAllFilePath(Path filePath, FileSystem fs) throws FileNotFoundException, IOException {
        List<String> fileList = new ArrayList<String>();
        FileStatus[] fileStatus = fs.listStatus(filePath);
        for (FileStatus fileStat : fileStatus) {
            if (fileStat.isDirectory()) {
                fileList.addAll(getAllFilePath(fileStat.getPath(), fs));
            } else {
                fileList.add(fileStat.getPath().toString());
            }
        }
        return fileList;
    }

    /*
     * Test handling the key properly and that the file is secured
     */
    public void testKeyFile() throws Exception {

/* XXX       GitActionExecutor ae = new GitActionExecutor();
        assertTrue("Can not find GitMain class is launcher classes",
          ae.getLauncherClasses().contains(GitMain.class)); */

        final Path credentialFilePath = Path.mergePaths(getFsTestCaseDir(), new Path("/key_dir/my_key.dsa"));
        final String credentialFileData = "Key file data";
        final Path destDir = Path.mergePaths(getFsTestCaseDir(), new Path("/destDir"));
        final String repoUrl = "https://github.com/apache/oozie";

        // setup credential file data
        FSDataOutputStream credentialFile = getFileSystem().create(credentialFilePath);
        credentialFile.write(credentialFileData.getBytes());
        credentialFile.flush();
        credentialFile.close();

/*        Element actionXml = XmlUtils.parseXml("<git>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<git-uri>" + repoUrl + "</git-uri>"+
                "<key-path>" + credentialFilePath + "</key-path>"+
                "<destination-uri>" + destDir + "</destination-uri>" +
                "</git>");
*/
// XXX        XConfiguration protoConf = new XConfiguration();
// XX        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());

// XXX        WorkflowJobBean wf = createBaseWorkflow(protoConf, "git-action");
 //       WorkflowJobBean wf = new WorkflowJobBean();
        WorkflowActionBean ga = new WorkflowActionBean();
		ga.setType("git-action");
 //       ActionExecutorContext context = new ActionExecutorContext(wf, ga);
 //       WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
  //      action.setType(ae.getType());

   //     Configuration conf = ae.createBaseHadoopConf(context, actionXml);

        GitMain gitmain = new GitMain();

        // allow us to call getKeyFromFS()
        Class<?>[] args = new Class<?>[] { Path.class } ;
        Method getKeyMethod = gitmain.getClass().getDeclaredMethod("getKeyFromFS", args);
        getKeyMethod.setAccessible(true);

        File localFile = (File) getKeyMethod.invoke(gitmain, credentialFilePath);
//        File localFile = gitmain.getKeyFromFS(credentialFilePath);

        FileReader reader = new FileReader(localFile);
        char[] testOutput = new char[credentialFileData.length()];
        assertEquals(13, credentialFileData.length());
        reader.read(testOutput, 0, credentialFileData.length());

        assertEquals(credentialFileData, String.valueOf(testOutput));
        reader.close();

/*        Context context = createContext(actionXml);
        final RunningJob launcherJob = submitAction(context);
        String launcherId = context.getAction().getExternalId();
//XXX        waitFor(120 * 1000, new Predicate() {
        waitFor(30 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return launcherJob.isComplete();
            }
        });
        assertTrue(launcherJob.isSuccessful());
        Map<String, String> actionData = LauncherMapperHelper.getActionData(getFileSystem(), context.getActionDir(),
                context.getProtoActionConf());
        assertFalse(LauncherMapperHelper.hasIdSwap(actionData));

        GitActionExecutor ae = new GitActionExecutor();
        ae.check(context, context.getAction());
        assertTrue(launcherId.equals(context.getAction().getExternalId()));
        assertEquals("SUCCEEDED", context.getAction().getExternalStatus());
        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());

*/

//        assertTrue(getFileSystem().exists(outputPath));
//        assertTrue(getFileSystem().exists(gitIndex));

        // the Git conf index should easily fit in memory
//        ByteArrayOutputStream readContent = new ByteArrayOutputStream();
//        byte[] buffer = new byte[1024];
//        InputStream is = getFileSystem().open(gitIndex);
//        int length;
//        while ((length = is.read(buffer)) != -1) {
//            readContent.write(buffer, 0, length);
//        }
//        String gitIndexContent = readContent.toString("UTF-8");
//        is.close();

        // The Git index file should have a core, remote and branch section
        // verify it was properly pulled down
//        assertTrue(gitIndexContent.toLowerCase().contains("core"));
//        assertTrue(gitIndexContent.toLowerCase().contains("remote"));
//        assertTrue(gitIndexContent.toLowerCase().contains("branch"));
    }

}
