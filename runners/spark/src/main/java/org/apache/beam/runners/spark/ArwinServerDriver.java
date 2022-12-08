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
package org.apache.beam.runners.spark;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import org.apache.beam.runners.jobsubmission.JobServerDriver;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.fn.server.ServerFactory;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/** Driver program that starts a job server for the Spark runner. */
public class ArwinServerDriver extends JobServerDriver {

  private static final Logger LOG = LoggerFactory.getLogger(ArwinServerDriver.class);

  public static void main(String[] args) {
    try {
      String hostname = new BufferedReader(
        new InputStreamReader(Runtime.getRuntime().exec(new String[]{"hostname", "-I"}).getInputStream(), Charsets.UTF_8))
       .readLine();
      hostname = Iterables.get(Splitter.on(' ').split(hostname), 0);
      LOG.info("ARWIN'S HOSTNAME: {}", hostname);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    LOG.info("We are in Arwin's world!");

    PipelineOptions options = PipelineOptionsFactory.create();
    // Limiting gcs upload buffer to reduce memory usage while doing parallel artifact uploads.
    options.as(GcsOptions.class).setGcsUploadBufferSizeBytes(1024 * 1024);
    // Register standard file systems.
    FileSystems.setDefaultPipelineOptions(options);
    fromParams(args).run();
  }

  private static void printUsage(CmdLineParser parser) {
    System.err.println(
        String.format("Usage: java %s arguments...", ArwinServerDriver.class.getSimpleName()));
    parser.printUsage(System.err);
    System.err.println();
  }

  public static ArwinServerDriver fromParams(String[] args) {
    SparkJobServerDriver.SparkServerConfiguration configuration = new SparkJobServerDriver.SparkServerConfiguration();
    CmdLineParser parser = new CmdLineParser(configuration);
    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      LOG.error("Unable to parse command line arguments.", e);
      printUsage(parser);
      throw new IllegalArgumentException("Unable to parse command line arguments.", e);
    }

    return fromConfig(configuration);
  }

  public static ArwinServerDriver fromConfig(SparkJobServerDriver.SparkServerConfiguration configuration) {
    return create(
        configuration,
        createJobServerFactory(configuration),
        createArtifactServerFactory(configuration));
  }

  private static ArwinServerDriver create(
      SparkJobServerDriver.SparkServerConfiguration configuration,
      ServerFactory jobServerFactory,
      ServerFactory artifactServerFactory) {
    return new ArwinServerDriver(configuration, jobServerFactory, artifactServerFactory);
  }

  private ArwinServerDriver(
      SparkJobServerDriver.SparkServerConfiguration configuration,
      ServerFactory jobServerFactory,
      ServerFactory artifactServerFactory) {
    super(
        configuration,
        jobServerFactory,
        artifactServerFactory,
        () -> SparkJobInvoker.create(configuration));
  }
}
