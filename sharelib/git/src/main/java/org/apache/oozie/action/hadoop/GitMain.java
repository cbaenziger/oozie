package org.apache.oozie.action.hadoop;

import java.io.IOException;
import java.io.StringReader;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.action.hadoop.LauncherMain;
import org.apache.oozie.action.hadoop.LauncherSecurityManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException.ErrorType;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.action.hadoop.GitActionExecutor;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.util.ELEvaluationException;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.PropertiesUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.apache.hadoop.conf.Configuration;
import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.TransportConfigCallback;
import org.eclipse.jgit.lib.TextProgressMonitor;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.eclipse.jgit.transport.*;
import org.eclipse.jgit.util.FS;



public class GitMain extends LauncherMain {

    private static final String HADOOP_USER = "user.name";
    private static final String HADOOP_JOB_TRACKER = "mapred.job.tracker";
    private static final String HADOOP_JOB_TRACKER_2 = "mapreduce.jobtracker.address";
    private static final String HADOOP_YARN_RM = "yarn.resourcemanager.address";
    private static final String HADOOP_JOB_NAME = "mapred.job.name";

    // Private configuration variables
    private String appName;
    private String workflowId;
    private String callbackUrl;
    private String jobTracker;
    private String nameNode;
    private String keyPath;
    private String destinationUri;
    private String gitUri;
    private String actionType;
    private String actionName;

    private static final Set<String> DISALLOWED_PROPERTIES = new HashSet<String>();

    static {
        DISALLOWED_PROPERTIES.add(HADOOP_USER);
        DISALLOWED_PROPERTIES.add(HADOOP_JOB_TRACKER);
        DISALLOWED_PROPERTIES.add(HADOOP_JOB_TRACKER_2);
        DISALLOWED_PROPERTIES.add(HADOOP_YARN_RM);
    }

    protected XLog LOG = XLog.getLog(getClass());

    public static void main(String[] args) throws Exception {
        run(GitMain.class, args);
    }

    @Override
    protected void run(String[] args) throws Exception {
        System.out.println();
        System.out.println("Oozie Git Action Configuration");
        LOG.debug("Oozie Git Action Configuration");
        System.out
                .println("=============================================");
        LOG.debug("=============================================");
        // loading action conf prepared by Oozie
        Configuration actionConf = new Configuration(false);

        String actionXml = System.getProperty("oozie.action.conf.xml");
        if (actionXml == null) {
            throw new RuntimeException(
                    "Missing Java System Property [oozie.action.conf.xml]");
        }
        if (!new File(actionXml).exists()) {
            throw new RuntimeException("Action Configuration XML file ["
                    + actionXml + "] does not exist");
        }

        actionConf.addResource(new Path("file:///", actionXml));

        try {
            parseActionConfiguration(actionConf);
        } catch (RuntimeException e) {
            //convertException(e);
            throw e;
        }

        // XXX Clay correct dulication of sshSessionFactory hack here
        SshSessionFactory sshSessionFactory = null;
        sshSessionFactory = new JschConfigSessionFactory() {

            @Override
            protected void configure(OpenSshConfig.Host host, Session session) {

            }

            @Override
            protected JSch createDefaultJSch(FS fs) throws JSchException {
                JSch.setConfig("StrictHostKeyChecking", "no");
                JSch defaultJSch = super.createDefaultJSch(fs);
                if (keyPath != null) {
                    try {
                        final String localKey = getSshKeys(new Path(keyPath));
                        defaultJSch.addIdentity(localKey);
                    } catch(IOException | URISyntaxException e) {
                        e.printStackTrace();
                        throw (JSchException) new JSchException().initCause(e);
                    }
                }
                return defaultJSch;
            }
        };

        try {
            cloneRepo(new Path(destinationUri), new URI(gitUri), sshSessionFactory);
        }catch(Exception e){
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * Gathers the sshkey from HDFS and copies it to a local
     * filesystem location
     *
     * @param location where the ssh key is located (an HDFS URI)
     * @return the location to where the key was saved
     */
    private String getSshKeys(Path location) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        FileSystem hfs = FileSystem.newInstance(new URI(nameNode), conf);
        File key = new File(Files.createTempDirectory(
            Paths.get("."),
            "keys_" + Long.toString(System.nanoTime()),
            PosixFilePermissions
                .asFileAttribute(PosixFilePermissions
                   .fromString("rwx------")))
            .toString());

        System.out.println("Local mkdir called creating temp. dir at: " +
            key.getAbsolutePath());
        LOG.debug("Local mkdir called creating temp. dir at: " +
            key.getAbsolutePath());

        hfs.copyToLocalFile(location, new Path("file:///" +
            key.getAbsolutePath() + "/privkey.pem"));
        System.out.println("Copied keys to local container!");
        LOG.debug("Copied keys to local container!");
        return(key.getAbsolutePath() + "/privkey.pem");
    }

    /**
     * Clones a Git repository 
     *
     * @param destination
     * @throws Exception
     */
    private String cloneRepo(Path destination, URI gitSrc,
                             final SshSessionFactory sshSessionFactory) throws Exception {
        File tempD = new File(Files.createTempDirectory(
            Paths.get("."),
            "git_" + Long.toString(System.nanoTime()),
            PosixFilePermissions
                .asFileAttribute(PosixFilePermissions
                   .fromString("rwx------")))
            .toString());

        System.out.println("Local mkdir called creating temp. dir at: " +
            tempD.getAbsolutePath());
        LOG.debug("Local mkdir called creating temp. dir at: " +
            tempD.getAbsolutePath());

        Configuration conf = new Configuration();
        FileSystem hfs = FileSystem.get(destination.toUri(), conf);

        CloneCommand cloneCommand = Git.cloneRepository();
        cloneCommand.setURI(gitSrc.toString());
        if (gitSrc.getScheme().toLowerCase() == "ssh") {
          cloneCommand.setTransportConfigCallback(new TransportConfigCallback() {
              @Override
              public void configure(Transport transport) {
                  SshTransport sshTransport = (SshTransport)transport;
                  sshTransport.setSshSessionFactory(sshSessionFactory);
              }
          });
        }
        cloneCommand.setDirectory(tempD);
        cloneCommand.call();

        // create a list of files and directories to upload
        File src = new File(tempD.getAbsolutePath());
        ArrayList<Path> srcs = new ArrayList(1000);
        for (File p:src.listFiles()) {
          srcs.add(new Path(p.toString()));
        }

        System.out.println("Finished cloning to local");
        LOG.debug("Finished cloning to local");

        hfs.mkdirs(destination);
        hfs.copyFromLocalFile(false, true, srcs.toArray(new Path[0]), destination);
        System.out.println("Finished the copy to " + destination.toString() + "!");
        LOG.debug("Finished the copy to " + destination.toString() + "!");
        return(destination.toString());
    }

    /**
     * Parse action configuration and set configuration variables
     *
     * @param Oozie action configuration
     * @throws RuntimeException upon any parse failure
     */
    private void parseActionConfiguration(Configuration actionConf) {
        // APP_NAME
        appName = actionConf.get(GitActionExecutor.APP_NAME);
        if (appName == null) {
            throw new RuntimeException("Action Configuration does not have "
                    + GitActionExecutor.APP_NAME + " property");
        }
        //WORKFLOW_ID
        workflowId = actionConf.get(GitActionExecutor.WORKFLOW_ID);
        if (workflowId == null) {
            throw new RuntimeException("Action Configuration does not have "
                    + GitActionExecutor.WORKFLOW_ID + " property");
        }
        // CALLBACK_URL
        callbackUrl = actionConf.get(GitActionExecutor.CALLBACK_URL);
        if (callbackUrl == null) {
            throw new RuntimeException("Action Configuration does not have "
                    + GitActionExecutor.CALLBACK_URL + " property");
        }
        // JOB_TRACKER
        jobTracker = actionConf.get(GitActionExecutor.JOB_TRACKER);
        if (jobTracker == null) {
            throw new RuntimeException("Action Configuration does not have "
                    + GitActionExecutor.JOB_TRACKER + " property");
        }
        //NAME_NODE
        nameNode = actionConf.get(GitActionExecutor.NAME_NODE);
        if (nameNode == null) {
            throw new RuntimeException("Action Configuration does not have "
                    + GitActionExecutor.NAME_NODE + " property");
        }
        // DESTINATION_URI
        destinationUri = actionConf.get(GitActionExecutor.DESTINATION_URI);
        if (destinationUri == null) {
            throw new RuntimeException("Action Configuration does not have "
                    + GitActionExecutor.DESTINATION_URI + " property");
        }
        try {
            FileSystem hfs = FileSystem.get(new URI(destinationUri), actionConf);
            destinationUri = hfs.makeQualified(new Path(destinationUri)).toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException("Action Configuration does not have "
                    + "a proper URI " + GitActionExecutor.DESTINATION_URI + "exception "
                    + e.toString());
        } catch (IOException e) {
            throw new RuntimeException("Action Configuration does not have "
                    + "a filesystem for URI " + GitActionExecutor.DESTINATION_URI + "exception "
                    + e.toString());
        }
        // GIT_URI
        gitUri = actionConf.get(GitActionExecutor.GIT_URI);
        if (gitUri == null) {
            throw new RuntimeException("Action Configuration does not have "
                    + "a scheme: " + GitActionExecutor.GIT_URI);
        }
        try {
            if (new URI(gitUri).getScheme() == null) {
              throw new RuntimeException("Action Configuration does not have "
                      + "a proper URI " + GitActionExecutor.GIT_URI);
             }
        } catch (URISyntaxException e) {
            throw new RuntimeException("Action Configuration does not have "
                    + "a proper URI " + GitActionExecutor.GIT_URI + "exception "
                    + e.toString());
        }
        // KEY_PATH
        keyPath = actionConf.get(GitActionExecutor.KEY_PATH);
        // ACTION_TYPE
        actionType = actionConf.get(GitActionExecutor.ACTION_TYPE);
        if (actionType == null) {
            throw new RuntimeException("Action Configuration does not have "
                    + GitActionExecutor.ACTION_TYPE + " property");
        }
        // ACTION_NAME
        actionName = actionConf.get(GitActionExecutor.ACTION_NAME);
        if (actionName == null) {
            throw new RuntimeException("Action Configuration does not have "
                    + GitActionExecutor.ACTION_NAME + " property");
        }
    }

}
