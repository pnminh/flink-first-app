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

package org.example.stream;

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

    public static final String DATA_RAW_AUDIT_TRAIL_DIR = "src/main/resources/data/raw_audit_trail";

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //read once then stop
        DataStream<String> auditTrailsReadOnce = env.readTextFile(DATA_RAW_AUDIT_TRAIL_DIR);
        //This prints before the job get executed
        printLineWithThreadInfo("Hello");
        auditTrailsReadOnce.map(
                audit ->
                {
                    AuditTrail auditTrail = new AuditTrail(audit);
					printLineWithThreadInfo(auditTrail.toString());
					return auditTrail;
                });
        //This prints before the job get executed
        printLineWithThreadInfo("World");
        //continue waiting for new data
        env.readFile(
                new TextInputFormat(new Path(DATA_RAW_AUDIT_TRAIL_DIR)),
                DATA_RAW_AUDIT_TRAIL_DIR,
                FileProcessingMode.PROCESS_CONTINUOUSLY,
                1000).map(value -> {
            printLineWithThreadInfo(value);
            return value;
        });
        env.execute("Starting Job, auditTrails is not printed yet");
    }

    private static void printLineWithThreadInfo(String printLineString) {
        System.out.println(Thread.currentThread().getName() + ":" + printLineString);
    }
}
