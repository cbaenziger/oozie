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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import org.apache.oozie.test.XFsTestCase;

public class TestGitMainGetKey extends XFsTestCase {

    private GitMain gitmain = null;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        gitmain = new GitMain();
        gitmain.nameNode = getFileSystem().getUri().toString();
    }

    public void testGitKeyFileIsCopiedToHDFS() throws Exception {
        final Path credentialFilePath = Path.mergePaths(getFsTestCaseDir(), new Path("/key_dir/my_key.dsa"));
        final String credentialFileData = "Key file data";
        Path.mergePaths(getFsTestCaseDir(), new Path("/destDir"));

        setupCredentialFile(credentialFilePath, credentialFileData);

        File localFile = gitmain.getKeyFromFS(credentialFilePath);

        try (FileReader reader = new FileReader(localFile)) {
            char[] testOutput = new char[credentialFileData.length()];
            assertEquals("credential file length mismatch", 13, credentialFileData.length());

            reader.read(testOutput, 0, credentialFileData.length());
            assertEquals("credential file data mismatch", credentialFileData, String.valueOf(testOutput));
        }

        FileUtils.deleteDirectory(new File(localFile.getParent()));
    }

    private void setupCredentialFile(Path credentialFilePath, String credentialFileData) throws IOException {
        try (FSDataOutputStream credentialFileOS = getFileSystem().create(credentialFilePath)) {
            credentialFileOS.write(credentialFileData.getBytes());
            credentialFileOS.flush();
        }
    }
}
