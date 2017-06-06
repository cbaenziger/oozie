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
import java.util.ArrayList;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.List;
import java.lang.Object;

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
    static final String GIT_URI = "oozie.git.source.uri";
    static final String GIT_BRANCH = "oozie.git.branch";
    static final String DESTINATION_URI = "oozie.git.destination.uri";
    static final String KEY_PATH = "oozie.git.key.path";
    static final String ACTION_TYPE = "oozie.oozie.action.type";
    static final String ACTION_NAME = "oozie.oozie.action.name";

    public GitActionExecutor() {
        super("git");
    }

    @SuppressWarnings("rawtypes")
    @Override
    public List<Class<?>> getLauncherClasses() {
       List<Class<?>> classes = new ArrayList<Class<?>>();
        try {
            classes.add(Class.forName(GIT_MAIN_CLASS_NAME));
        }
        catch (ClassNotFoundException e) {
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

    /**
     * Verifies a value is not null and if so, sets it into the actionConf
     * can optionally throw an ActionExecutorException if the value is mandatory
     *
     * @param  value       value to verify is non-null
     * @param  displayName the actionConf name to use (will be printed in the exception too)
     * @param  fatal       if an exception should be raised if the value is null
     * @param  getTextTrim if we should run the getTextTrim() method on the value verified
     * @return             true if value was non-null
     */
    public static boolean verifyPropertyNotNull(Object value, String displayName,
                               boolean fatal) throws RuntimeException {
        if (value == null) {
            if (fatal)
                throw new RuntimeException("Action Configuration does not have "
                          + displayName + " property");
            return(false);
        } else {
            return(true);
        }
    }
    
    private void verifyPropertyNotNullFatal(String value, String displayName, Configuration actionConf) {
        if (verifyPropertyNotNull(value, displayName, true)) {
            actionConf.set(displayName, value);
        }
    }
    
    private void verifyPropertyNotNullFatalTT(Element value, String displayName, Configuration actionConf) {
        if (verifyPropertyNotNull(value, displayName, true)) {
            actionConf.set(displayName, value.getTextTrim());
        }
    }
    
    private void verifyPropertyNotNullConfOnlyTT(Element value, String displayName, Configuration actionConf) {
        if (verifyPropertyNotNull(value, displayName, false)) {
            actionConf.set(displayName, value.getTextTrim());
        }
    }
    
    @Override
    @SuppressWarnings("unchecked")
    Configuration setupActionConf(Configuration actionConf, Context context,
                                  Element actionXml, Path appPath) throws ActionExecutorException {
        super.setupActionConf(actionConf, context, actionXml, appPath);
                
        Namespace ns = actionXml.getNamespace();

        // APP_NAME
        verifyPropertyNotNullFatal(context.getWorkflow().getAppName(), GitActionExecutor.APP_NAME, actionConf);

        //WORKFLOW_ID
        verifyPropertyNotNullFatal(context.getWorkflow().getId(), GitActionExecutor.WORKFLOW_ID, actionConf);
        
        // CALLBACK_URL
        verifyPropertyNotNullFatal(context.getCallbackUrl("$jobStatus"), GitActionExecutor.CALLBACK_URL, actionConf);       

        // JOB_TRACKER
        verifyPropertyNotNullFatalTT(actionXml.getChild("job-tracker", ns), GitActionExecutor.JOB_TRACKER, actionConf);

        //NAME_NODE
        verifyPropertyNotNullFatalTT(actionXml.getChild("name-node", ns), GitActionExecutor.NAME_NODE, actionConf);

        // DESTINATION_URI
        verifyPropertyNotNullFatalTT(actionXml.getChild("destination-uri", ns), GitActionExecutor.DESTINATION_URI, actionConf);

        // GIT_URI
        verifyPropertyNotNullFatalTT(actionXml.getChild("git-uri", ns), GitActionExecutor.GIT_URI, actionConf);

        // KEY_PATH
        verifyPropertyNotNullConfOnlyTT(actionXml.getChild("key-path", ns), KEY_PATH, actionConf);

        // GIT_BRANCH
        verifyPropertyNotNullConfOnlyTT(actionXml.getChild("branch", ns), GIT_BRANCH, actionConf);

        actionConf.set(ACTION_TYPE, getType());
        actionConf.set(ACTION_NAME, "git");
        
        return actionConf;
    }

    @Override
    protected String getDefaultShareLibName(Element actionXml) {
        return "git";
    }
}
