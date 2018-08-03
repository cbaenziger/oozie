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

     * Inspired from:
     * * https://stackoverflow.com/questions/14360909/jgit-how-to-use-inmemoryrepository-to-learn-about-dfs-implementations
     * * https://github.com/centic9/jgit-cookbook/blob/c0f0d591839382461119fc5bd7a73db109d795b0/httpserver/src/main/java/org/dstadler/jgit/server/Main.java#L79-L94 (latter example is not accepting writes and pulls netty making it slower)
     */
    private Map<String, Repository> repositories = new HashMap<>();
    private Daemon server;

    public GitServer() throws IOException {
        this.server = new Daemon(new InetSocketAddress(9418));
        this.server.getService("git-receive-pack").setEnabled(true);
        this.server.setRepositoryResolver(new RepositoryResolverImplementation());
        this.server.start();
    }

    public void stopAndCleanupReposServer() throws InterruptedException {
        cleanUpRepos();
        this.server.stop();
    }

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

    private final class RepositoryResolverImplementation implements
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
                    addInitialCommit(git);
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
         * When missing then master branch is missing and can't be checked out in clones.
         */
        public void addInitialCommit(Git git) {
            try {
                git.commit().setMessage("Initial empty repo setup").call();
            } catch (GitAPIException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
