package org.apache.oozie.action.hadoop;

import java.io.File;
import java.net.URI;
import java.util.Arrays;

import org.apache.oozie.action.hadoop.GitMain.GitMainException;
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

public class GitOperations {
	private URI srcURL;
	private String branch;
	private File repoDir;
	private String credentialData;

	public void GitOperations(URI gitSrc, String branch, File repoDir, credentialFile) {
		
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


}
