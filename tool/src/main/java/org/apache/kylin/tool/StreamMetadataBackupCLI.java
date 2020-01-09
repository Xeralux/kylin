/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kylin.tool;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.stream.coordinator.StreamMetadataStore;
import org.apache.kylin.stream.coordinator.StreamMetadataStoreFactory;
import org.apache.kylin.stream.core.model.CubeAssignment;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.util.List;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

/**
 * sh bin/kylin.sh org.apache.kylin.tool.MetadataBackupCLI backup_assignment
 */
public class StreamMetadataBackupCLI {
    private StreamMetadataStore streamMetadataStore;

    public void prepare() {
        streamMetadataStore = StreamMetadataStoreFactory.getStreamMetaDataStore();
    }

    public void backupAssignment(String backupPath) throws IOException {
        List<CubeAssignment> cubeAssignments = streamMetadataStore.getAllCubeAssignments();
        String assignmentPath = backupPath + "/cubeAssignment";
        System.out.println("Found " + cubeAssignments.size() + "assignments.");
        for (CubeAssignment assignment : cubeAssignments) {
            saveObject(assignment, assignmentPath, assignment.getCubeName());
        }
    }

    public void restoreAssignment(String path, String fileName) throws IOException {

        File cubeFile = new File(path + "/" + fileName + ".json");

        if (!cubeFile.exists()) {
            System.err.println(cubeFile + " not found.");
        }
        CubeAssignment assignment = JsonUtil.readValue(cubeFile, CubeAssignment.class);
        System.out.println("Found and save :" + assignment);
        streamMetadataStore.saveNewCubeAssignment(assignment);
    }

    public static void main(String[] args) throws Exception {
        LocalDateTime localDateTime = LocalDateTime.now(ZoneId.systemDefault());
        DateTimeFormatter myFormatter = new DateTimeFormatterBuilder()
                .appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
                .appendLiteral('-')
                .appendValue(MONTH_OF_YEAR, 2)
                .appendLiteral('-')
                .appendValue(DAY_OF_MONTH, 2)
                .appendLiteral('_')
                .appendValue(HOUR_OF_DAY, 2)
                .appendLiteral('-')
                .appendValue(MINUTE_OF_HOUR, 2)
                .optionalStart()
                .appendLiteral('-')
                .appendValue(SECOND_OF_MINUTE, 2)
                .optionalStart().toFormatter();
        String baseDir = "stream_metadata/" + localDateTime.format(myFormatter);
        args = StringUtil.filterSystemArgs(args);

        if (args.length == 0) {
            System.out.println("Usage: MetadataBackupCLI backup_assignment");
            System.out.println("Usage: MetadataBackupCLI restore_assignment /path/to/backup cube");
            return;
        }
        String option = args[0];
        StreamMetadataBackupCLI cli = new StreamMetadataBackupCLI();
        cli.prepare();

        switch (option) {
            case "backup_assignment": {
                cli.backupAssignment(baseDir);
                break;
            }

            case "restore_assignment": {
                String dataDir = args[1];
                String filename = args[2];
                cli.restoreAssignment(dataDir, filename);
                break;
            }

            default:
                System.err.println("Error: please use correct options.");
        }
        System.out.println("Completed " + option + " !");
    }

    private void saveObject(Object obj, String path, String name) throws IOException {
        String str = JsonUtil.writeValueAsIndentString(obj);
        File f = new File(path, name + ".json");
        f.getParentFile().mkdirs();
        System.out.println("Save " + str + " to " + f.getAbsolutePath());
        FileUtils.write(f, str);
        f.setLastModified(System.currentTimeMillis());
    }
}
