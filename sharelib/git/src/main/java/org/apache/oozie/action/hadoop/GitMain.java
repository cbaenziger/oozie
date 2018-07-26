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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.oozie.action.hadoop.GitOperations;
import org.apache.oozie.action.hadoop.GitOperations.GitOperationsException;

import com.google.common.annotations.VisibleForTesting;

public class GitMain extends LauncherMain {

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
        GitOperations gitRepo = new GitOperations(new URI(gitUri), gitBranch, localKey);

        try {
            gitRepo.cloneRepoToFS(new Path(destinationUri));
        }
        catch (IOException | GitOperationsException e){
            errorAndLog(e.getMessage());

            throw new GitMainException(e.getCause());
        }
    }

    protected static void outAndLog(String message) {
        System.out.println(message);
    }

    protected static void errorAndLog(String errorMessage) {
        System.err.println(errorMessage);
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

        File key = createTempDir("git");

        fs.copyToLocalFile(location, new Path("file:///" +
            key.getAbsolutePath() + "/privkey"));
        outAndLog(keyCopyMsg);

        return new File(key.getAbsolutePath() + "/privkey");
    }

    /**
     * Create a local temporary directory
     *
     * @param string to use as a prefix to the directory
     * @returns file path of temp. directory (will be set to delete on exit)
     * @throws Exception
     */
    protected static File createTempDir(String prefix) throws IOException {
        File tempD = new File(Files.createTempDirectory(
            Paths.get("."),
            prefix + "_" + Long.toString(System.nanoTime()),
            PosixFilePermissions
                .asFileAttribute(PosixFilePermissions
                   .fromString("rwx------")))
            .toString());
        tempD.deleteOnExit();

        String localMkdirMsg = "Local mkdir called creating temp. dir at: " + tempD.getAbsolutePath();
        outAndLog(localMkdirMsg);
        return tempD;
    }

    /**
     * Validate a URI is well formed and has a scheme
     *
     * @param testUri URI string to test
     * @returns URI from string
     * @throws GitMainException
     */
    private URI isValidUri(String testUri) throws GitMainException {
        URI uri;
        try {
            uri = new URI(testUri);
        } catch (URISyntaxException e) {
            throw new GitMainException("Action Configuration does not have "
                    + "a proper URI: " + testUri + " exception "
                    + e.toString());
        }
        if (uri.getScheme() == null) {
            throw new GitMainException("Action Configuration does not have "
                    + "a proper URI " + testUri + " null scheme.");
        }
        return uri;
    }

    /**
     * Parse action configuration and set configuration variables
     *
     * @param actionConf Oozie action configuration
     * @throws NullPointerException upon any properties missing
     */
    private void parseActionConfiguration(Configuration actionConf) throws GitMainException {
        GitActionExecutor.VerifyActionConf confChecker = new GitActionExecutor.VerifyActionConf(actionConf);

        nameNode = confChecker.checkAndGetTrimmed(GitActionExecutor.NAME_NODE);
        destinationUri = confChecker.checkAndGetTrimmed(GitActionExecutor.DESTINATION_URI);
        try {
            FileSystem fs = FileSystem.get(isValidUri(destinationUri), actionConf);
            destinationUri = fs.makeQualified(new Path(destinationUri)).toString();
        } catch (IOException e) {
            throw new GitMainException("Action Configuration does not have "
                    + "a valid filesystem for URI " + GitActionExecutor.DESTINATION_URI + "exception "
                    + e.toString());
        }
        gitUri = isValidUri(confChecker.checkAndGetTrimmed(GitActionExecutor.GIT_URI)).toString();
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
