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
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.action.hadoop.GitMain.GitMainException;
import org.apache.oozie.action.hadoop.GitOperations;
import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.TransportConfigCallback;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.transport.JschConfigSessionFactory;
import org.eclipse.jgit.transport.OpenSshConfig;
import org.eclipse.jgit.transport.SshSessionFactory;
import org.eclipse.jgit.transport.SshTransport;
import org.eclipse.jgit.transport.Transport;
import org.eclipse.jgit.util.FS;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.TransportConfigCallback;
import org.eclipse.jgit.transport.*;
import org.eclipse.jgit.util.FS;

public class GitOperations {
    private final URI srcURL;
    private final String branch;
    private final File credentialFile;

    public GitOperations(URI gitSrc, String branch, File credentialFile) {
       this.srcURL = gitSrc;
       this.branch = branch;
       this.credentialFile = credentialFile;
    }

    /**
     * Used by GitOperations to wrap a Throwable when an Exception occurs
     */
    @SuppressWarnings("serial")
    static class GitOperationsException extends Exception {
        public GitOperationsException(Throwable t) {
            super(t);
        }

        public GitOperationsException(String t) {
            super(t);
        }
    }

    /**
     * Clones a Git repository
     */
    public void cloneRepo(File outputDir) throws GitOperationsException {
        final SshSessionFactory sshSessionFactory = new JschConfigSessionFactory() {
            @Override
            protected void configure(OpenSshConfig.Host host, Session session) {

            }

            @Override
            protected JSch createDefaultJSch(FS fs) throws JSchException {
                JSch.setConfig("StrictHostKeyCheckiging", "no");
                JSch defaultJSch = super.createDefaultJSch(fs);

                if (credentialFile != null) {
                    defaultJSch.addIdentity(credentialFile.toString());
                }

                return defaultJSch;
            }
        };

        CloneCommand cloneCommand = Git.cloneRepository();
        cloneCommand.setURI(srcURL.toString());

        if (srcURL.getScheme().toLowerCase().equals("ssh")) {
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
            throw new GitOperationsException(unableToCloneMsg);
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
    public String cloneRepoToFS(Path destination) throws IOException, GitOperationsException {
        String finishedCopyMsg = "Finished the copy to " + destination.toString() + "!";
        String finishedCloneingMsg = "Finished cloning to local";

        File tempD = GitMain.createTempDir("git");

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(destination.toUri(), conf);

        cloneRepo(tempD);

        // create a list of files and directories to upload
        File src = new File(tempD.getAbsolutePath());
        ArrayList<Path> srcs = new ArrayList<Path>(1000);
        for (File p:src.listFiles()) {
          srcs.add(new Path(p.toString()));
        }

        GitMain.outAndLog(finishedCloneingMsg);

        fs.mkdirs(destination);
        fs.copyFromLocalFile(false, true, srcs.toArray(new Path[0]), destination);

        GitMain.outAndLog(finishedCopyMsg);

        return destination.toString();
    }
 
}
