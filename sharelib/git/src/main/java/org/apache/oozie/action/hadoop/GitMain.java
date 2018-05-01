/*
 * Licensed to the Apache Software Foundation (ASF) under one
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
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.oozie.util.XLog;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.TransportConfigCallback;
import org.eclipse.jgit.transport.*;
import org.eclipse.jgit.util.FS;
import com.google.common.annotations.VisibleForTesting;

public class GitMain extends LauncherMain {

    private static final XLog LOG = XLog.getLog(GitMain.class);
    private static final String OOZIE_ACTION_CONF = "oozie.action.conf.xml";

    @VisibleForTesting
    String nameNode;
    private String keyPath;
    private String destinationUri;
    private String gitUri;
    private String gitBranch;

    public static void main(String[] args) throws Exception {
        run(GitMain.class, args);
    }

    @Override
    protected void run(String[] args) throws Exception {
        System.out.println();
        outAndLog("Oozie Git Action Configuration");

        outAndLog("=============================================");

        Configuration actionConf = prepareActionConf();
        parseActionConfiguration(actionConf);
        File localKey = getLocalKeyFile();

        try {
            cloneRepoToFS(new Path(destinationUri), new URI(gitUri), gitBranch, localKey);
        }
        catch (IOException | GitMainException e){
            errorAndLog(e.getMessage());

            throw new GitMainException(e.getCause());
        }
    }

    private void outAndLog(String message) {
        System.out.println(message);
        LOG.info(message);
    }

    private void errorAndLog(String errorMessage) {
        System.err.println(errorMessage);
        LOG.error(errorMessage);
    }

    private Configuration prepareActionConf() {
        Configuration actionConf = new Configuration(false);

        String actionXml = System.getProperty(OOZIE_ACTION_CONF);
        if (actionXml == null) {
            throw new RuntimeException(
                    "Missing Java System Property [" + OOZIE_ACTION_CONF + "]");
        }
        if (!new File(actionXml).exists()) {
            throw new RuntimeException("Action Configuration XML file ["
                    + actionXml + "] does not exist");
        }

        actionConf.addResource(new Path("file:///", actionXml));
        return actionConf;
    }

    private File getLocalKeyFile() throws IOException, URISyntaxException {
        File localKey = null;

        if (keyPath != null) {
            localKey = getKeyFromFS(new Path(keyPath));
        }

        return localKey;
    }

    /**
     * Gathers the Git authentication key from a FileSystem and copies it to a local
     * filesystem location
     *
     * @param location where the key is located (an HDFS URI)
     * @return the location to where the key was saved
     */
    @VisibleForTesting
    protected File getKeyFromFS(Path location) throws IOException, URISyntaxException {
        String keyCopyMsg = "Copied keys to local container!";

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.newInstance(new URI(nameNode), conf);
        File key = new File(Files.createTempDirectory(
            Paths.get("."),
            "keys_" + Long.toString(System.nanoTime()),
            PosixFilePermissions
                .asFileAttribute(PosixFilePermissions
                   .fromString("rwx------")))
            .toString());

        String mkdirMsg = "Local mkdir called creating temp. dir at: " + key.getAbsolutePath();
        outAndLog(mkdirMsg);

        fs.copyToLocalFile(location, new Path("file:///" +
            key.getAbsolutePath() + "/privkey"));
        outAndLog(keyCopyMsg);

        return new File(key.getAbsolutePath() + "/privkey");
    }

    /**
     * Clones a Git repository
     *
     * @param gitSrc - URI to the Git repository being cloned
     * @param branch - String for the Git branch (or null)
     * @param outputDir - local file reference where the repository should be cloned
     * @param credentialFile - local file path containing repository authentication key or null
     */
    private void cloneRepo(URI gitSrc, String branch, File outputDir, final File credentialFile) throws GitMainException {
        final SshSessionFactory sshSessionFactory = new JschConfigSessionFactory() {
            @Override
            protected void configure(OpenSshConfig.Host host, Session session) {

            }

            @Override
            protected JSch createDefaultJSch(FS fs) throws JSchException {
                JSch.setConfig("StrictHostKeyChecking", "no");
                JSch defaultJSch = super.createDefaultJSch(fs);

                if (credentialFile != null) {
                    defaultJSch.addIdentity(credentialFile.toString());
                }

                return defaultJSch;
            }
        };

        CloneCommand cloneCommand = Git.cloneRepository();
        cloneCommand.setURI(gitSrc.toString());

        if (gitSrc.getScheme().toLowerCase().equals("ssh")) {
          cloneCommand.setTransportConfigCallback(new TransportConfigCallback() {
              @Override
              public void configure(Transport transport) {
                  SshTransport sshTransport = (SshTransport)transport;
                  sshTransport.setSshSessionFactory(sshSessionFactory);
              }
          });
        }

        cloneCommand.setDirectory(outputDir);
        // set our branch identifier
        if (branch != null) {
            cloneCommand.setBranchesToClone(Arrays.asList("refs/heads/" + branch));
        }

        try {
            cloneCommand.call();
        } catch (GitAPIException e) {
            String unableToCloneMsg = "Unable to clone Git repo: " + e;
            errorAndLog(unableToCloneMsg);
            throw new GitMainException(unableToCloneMsg);
        }
    }

    /**
     * Clone a Git repo up to a FileSystem
     *
     * @param destination - FileSystem path to which repository should be cloned
     * @param gitSrc - Git repo URI to clone from
     * @param branch - Git branch to clone
     * @param credentialFile - local file path containing repository authentication key or null
     * @throws Exception
     */
    private String cloneRepoToFS(Path destination, URI gitSrc, String branch, File credentialFile)
            throws IOException, GitMainException {
        String finishedCopyMsg = "Finished the copy to " + destination.toString() + "!";
        String finishedCloneingMsg = "Finished cloning to local";

        File tempD = new File(Files.createTempDirectory(
            Paths.get("."),
            "git_" + Long.toString(System.nanoTime()),
            PosixFilePermissions
                .asFileAttribute(PosixFilePermissions
                   .fromString("rwx------")))
            .toString());

        String localMkdirMsg = "Local mkdir called creating temp. dir at: " + tempD.getAbsolutePath();
        outAndLog(localMkdirMsg);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(destination.toUri(), conf);

        cloneRepo(gitSrc, branch,
            tempD, credentialFile);

        // create a list of files and directories to upload
        File src = new File(tempD.getAbsolutePath());
        ArrayList<Path> srcs = new ArrayList<Path>(1000);
        for (File p:src.listFiles()) {
          srcs.add(new Path(p.toString()));
        }

        outAndLog(finishedCloneingMsg);

        fs.mkdirs(destination);
        fs.copyFromLocalFile(false, true, srcs.toArray(new Path[0]), destination);
        outAndLog(finishedCopyMsg);

        return destination.toString();
    }

    /**
     * Validate a URI is well formed
     *
     * @param testUri URI string to test
     * @returns URI from string
     * @throws GitMainException
     */
    private URI validUri(String testUri) throws GitMainException {
        try {
            return(new URI(testUri));
        } catch (URISyntaxException e) {
            throw new GitMainException("Action Configuration does not have "
                    + "a proper URI: " + testUri + " exception "
                    + e.toString());
        }
    }

    /**
     * Parse action configuration and set configuration variables
     *
     * @param actionConf Oozie action configuration
     * @throws NullPointerException upon any properties missing
     */
    private void parseActionConfiguration(Configuration actionConf) throws GitMainException {
        GitActionExecutor.VerifyActionConf confChecker = new GitActionExecutor.VerifyActionConf(actionConf);

        confChecker.checkAndGetTrimmed(GitActionExecutor.APP_NAME);
        confChecker.checkAndGetTrimmed(GitActionExecutor.WORKFLOW_ID);
        confChecker.checkAndGetTrimmed(GitActionExecutor.CALLBACK_URL);
        confChecker.checkAndGetTrimmed(GitActionExecutor.RESOURCE_MANAGER);

        nameNode = confChecker.checkAndGetTrimmed(GitActionExecutor.NAME_NODE);
        destinationUri = confChecker.checkAndGetTrimmed(GitActionExecutor.DESTINATION_URI);
        try {
            FileSystem fs = FileSystem.get(validUri(destinationUri), actionConf);
            destinationUri = fs.makeQualified(new Path(destinationUri)).toString();
        } catch (IOException e) {
            throw new GitMainException("Action Configuration does not have "
                    + "a valid filesystem for URI " + GitActionExecutor.DESTINATION_URI + "exception "
                    + e.toString());
        }
        gitUri = confChecker.checkAndGetTrimmed(GitActionExecutor.GIT_URI);
        if (validUri(gitUri).getScheme() == null) {
          throw new GitMainException("Action Configuration does not have "
                  + "a proper URI " + gitUri);
        }
        gitBranch = actionConf.get(GitActionExecutor.GIT_BRANCH);
        keyPath = actionConf.get(GitActionExecutor.KEY_PATH);

        confChecker.checkAndGetTrimmed(GitActionExecutor.ACTION_TYPE);
        confChecker.checkAndGetTrimmed(GitActionExecutor.ACTION_NAME);
    }

    /**
     * Used by GitMain to wrap a Throwable when an Exception occurs
     */
    @SuppressWarnings("serial")
    static class GitMainException extends Exception {
        public GitMainException(Throwable t) {
            super(t);
        }

        public GitMainException(String t) {
            super(t);
        }
    }
}
