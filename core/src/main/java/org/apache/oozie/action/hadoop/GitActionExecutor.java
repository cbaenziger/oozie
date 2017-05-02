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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AccessControlException;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.action.hadoop.JavaActionExecutor;
import org.apache.oozie.action.hadoop.LauncherMain;
import org.apache.oozie.action.hadoop.LauncherMapper;
import org.apache.oozie.action.hadoop.MapReduceMain;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException.ErrorType;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;

public class GitActionExecutor extends JavaActionExecutor {

    private static final String GIT_MAIN_CLASS_NAME =
            "org.apache.oozie.action.hadoop.GitMain";
    static final String APP_NAME = "oozie.oozie.app.name";
    static final String WORKFLOW_ID = "oozie.oozie.workflow.id";
    static final String CALLBACK_URL = "oozie.oozie.callback.url";
    static final String JOB_TRACKER = "oozie.oozie.job.tracker";
    static final String NAME_NODE = "oozie.oozie.name.node";
    static final String GIT_URI = "oozie.git.git.uri";
    static final String GIT_BRANCH = "oozie.git.git.branch";
    static final String DESTINATION_URI = "oozie.git.destination.uri";
    static final String KEY_PATH = "oozie.git.key.path";
    static final String ACTION_TYPE = "oozie.oozie.action.type";
    static final String ACTION_NAME = "oozie.oozie.action.name";

    public GitActionExecutor() {
        super("git");
    }

    @SuppressWarnings("rawtypes")
    @Override
    public List<Class> getLauncherClasses() {
        List<Class> classes = super.getLauncherClasses();
        classes.add(LauncherMain.class);
        classes.add(MapReduceMain.class);
        try {
            classes.add(Class.forName(GIT_MAIN_CLASS_NAME));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Class not found", e);
        }
        return classes;
    }

    @Override
    protected String getLauncherMain(Configuration launcherConf, Element actionXml) {
        return launcherConf.get(LauncherMapper.CONF_OOZIE_ACTION_MAIN_CLASS,
                GIT_MAIN_CLASS_NAME);
    }

    @Override
    public void initActionType() {
        super.initActionType();
        registerError(UnknownHostException.class.getName(), ActionExecutorException.ErrorType.TRANSIENT, "HES001");
        registerError(AccessControlException.class.getName(), ActionExecutorException.ErrorType.NON_TRANSIENT,
                "JA002");
        registerError(DiskChecker.DiskOutOfSpaceException.class.getName(),
                ActionExecutorException.ErrorType.NON_TRANSIENT, "HES003");
        registerError(org.apache.hadoop.hdfs.protocol.QuotaExceededException.class.getName(),
                ActionExecutorException.ErrorType.NON_TRANSIENT, "HES004");
        registerError(org.apache.hadoop.hdfs.server.namenode.SafeModeException.class.getName(),
                ActionExecutorException.ErrorType.NON_TRANSIENT, "HES005");
        registerError(ConnectException.class.getName(), ActionExecutorException.ErrorType.TRANSIENT, "  HES006");
        registerError(JDOMException.class.getName(), ActionExecutorException.ErrorType.ERROR, "HES007");
        registerError(FileNotFoundException.class.getName(), ActionExecutorException.ErrorType.ERROR, "HES008");
        registerError(IOException.class.getName(), ActionExecutorException.ErrorType.TRANSIENT, "HES009");
    }

    @Override
    @SuppressWarnings("unchecked")
    Configuration setupActionConf(Configuration actionConf, Context context,
                                  Element actionXml, Path appPath) throws ActionExecutorException {
        super.setupActionConf(actionConf, context, actionXml, appPath);
        Namespace ns = actionXml.getNamespace();

        // APP_NAME
        String appName = context.getWorkflow().getAppName();
        if (appName == null) {
            throw new RuntimeException("Action Configuration does not have "
                    + GitActionExecutor.APP_NAME + " property");
        }
        //WORKFLOW_ID
        String workflowId = context.getWorkflow().getId();
        if (workflowId == null) {
            throw new RuntimeException("Action Configuration does not have "
                    + GitActionExecutor.WORKFLOW_ID + " property");
        }
        // CALLBACK_URL
        String callbackUrl = context.getCallbackUrl("$jobStatus");
        if (callbackUrl == null) {
            throw new RuntimeException("Action Configuration does not have "
                    + GitActionExecutor.CALLBACK_URL + " property");
        }
        actionConf.set(CALLBACK_URL, callbackUrl);

        // JOB_TRACKER
        Element jobTracker = actionXml.getChild("job-tracker", ns);
        if (jobTracker == null) {
            throw new RuntimeException("Action Configuration does not have "
                    + GitActionExecutor.JOB_TRACKER + " property");
        }
        actionConf.set(JOB_TRACKER, jobTracker.getTextTrim());

        //NAME_NODE
        Element nameNode = actionXml.getChild("name-node", ns);
        if (nameNode == null) {
            throw new RuntimeException("Action Configuration does not have "
                    + GitActionExecutor.NAME_NODE + " property");
        }
        actionConf.set(NAME_NODE, nameNode.getTextTrim());

        // DESTINATION_URI
        Element destinationUri = actionXml.getChild("destination-uri", ns);
        if (destinationUri == null) {
            throw new RuntimeException("Action Configuration does not have "
                    + GitActionExecutor.DESTINATION_URI + " property");
        }
        actionConf.set(DESTINATION_URI, destinationUri.getTextTrim());

        // GIT_URI
        Element gitUri = actionXml.getChild("git-uri", ns);
        if (gitUri == null) {
            throw new RuntimeException("Action Configuration does not have "
                    + GitActionExecutor.GIT_URI + " property");
        }
        actionConf.set(GIT_URI, gitUri.getTextTrim());

        Element keyPath = actionXml.getChild("key-path", ns);
        if (keyPath != null) {
          actionConf.set(KEY_PATH, keyPath.getTextTrim());
        }

        Element branch = actionXml.getChild("branch", ns);
        if (branch != null) {
          actionConf.set(GIT_BRANCH, branch.getTextTrim());
        }

        String actionType = getType();
        String actionName = "git";

        actionConf.set(APP_NAME, appName);
        actionConf.set(WORKFLOW_ID, workflowId);
        actionConf.set(ACTION_TYPE, actionType);
        actionConf.set(ACTION_NAME, actionName);
        return actionConf;
    }

    @Override
    protected String getDefaultShareLibName(Element actionXml) {
        return "git";
    }
}
