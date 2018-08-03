/**
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

import org.apache.commons.io.FileUtils;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.errors.RepositoryNotFoundException;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.transport.Daemon;
import org.eclipse.jgit.transport.DaemonClient;
import org.eclipse.jgit.transport.ServiceMayNotContinueException;
import org.eclipse.jgit.transport.resolver.RepositoryResolver;
import org.eclipse.jgit.transport.resolver.ServiceNotAuthorizedException;
import org.eclipse.jgit.transport.resolver.ServiceNotEnabledException;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class GitServer {
    /**
     * A simple git server serving anynymous git: protocol
     */
    private Map<String, Repository> repositories = new HashMap<>();
    private Daemon server;

    public GitServer() throws IOException {
        this.server = new Daemon(new InetSocketAddress(9418));
        this.server.getService("git-receive-pack").setEnabled(true);
        this.server.setRepositoryResolver(new EmptyRepositoryResolverImplementation());
        this.server.start();
    }

    public void stopAndCleanupReposServer() throws InterruptedException {
        cleanUpRepos();
        this.server.stop();
    }

    /**
     * A method to:
     * * remove all files on disk for all repositories
     * * clear the repositories listed for the GitServer
     */
    public void cleanUpRepos() {
        for (Repository repository : repositories.values()) {
            File workTree = repository.getWorkTree();
            try {
                FileUtils.deleteDirectory(workTree.getParentFile());
            } catch (IOException ignored) {

            }
        }
        repositories.clear();
    }

    /**
     * A simple class RepositoryResolver to provide an empty repository for non-existant repo requests
     */
    private final class EmptyRepositoryResolverImplementation implements
            RepositoryResolver<DaemonClient> {

        @Override
        public Repository open(DaemonClient client, String name)
                throws RepositoryNotFoundException,
                ServiceNotAuthorizedException, ServiceNotEnabledException,
                ServiceMayNotContinueException {
            Repository repo = repositories.get(name);
            if (repo == null) {
                try {
                    Path workDir = Files.createTempDirectory("GitTestSetup");
                    //git init
                    repo = FileRepositoryBuilder.create(new File(workDir.resolve(name).toFile(), ".git"));
                    repo.create();
                    // commit into the filesystem
                    Git git = new Git(repo);
                    // one needs an initial commit for a proper clone
                    addEmptyCommit(git);
                    git.close();
                    // serve the Git repo
                    repositories.put(name, repo);
                } catch (Exception e) {
                    throw new RuntimeException();
                }
            }
            return repo;
        }

        /**
         * Add an empty commit to a Git repository
         * @param git a Git repository to add an empty commit to 
         */
        public void addEmptyCommit(Git git) {
            try {
                git.commit().setMessage("Empty commit").call();
            } catch (GitAPIException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
