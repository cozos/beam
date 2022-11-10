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
package org.apache.beam.runners.spark.translation;

import java.util.Arrays;
import scala.Tuple2;

import org.apache.beam.runners.spark.SparkContextOptions;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.coders.SparkRunnerKryoRegistrator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Spark context factory. */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public final class SparkContextFactory {
  private static final Logger LOG = LoggerFactory.getLogger(SparkContextFactory.class);

  /**
   * If the property {@code beam.spark.test.reuseSparkContext} is set to {@code true} then the Spark
   * context will be reused for beam pipelines. This property should only be enabled for tests.
   */
  public static final String TEST_REUSE_SPARK_CONTEXT = "beam.spark.test.reuseSparkContext";

  // Spark allows only one context for JVM so this can be static.
  private static JavaSparkContext sparkContext;
  private static String sparkMaster;
  private static boolean usesProvidedSparkContext;

  private SparkContextFactory() {}

  public static synchronized JavaSparkContext getSparkContext(SparkPipelineOptions options) {
    SparkContextOptions contextOptions = options.as(SparkContextOptions.class);
    usesProvidedSparkContext = contextOptions.getUsesProvidedSparkContext();
    // reuse should be ignored if the context is provided.
    if (Boolean.getBoolean(TEST_REUSE_SPARK_CONTEXT) && !usesProvidedSparkContext) {

      // if the context is null or stopped for some reason, re-create it.
      if (sparkContext == null || sparkContext.sc().isStopped()) {
        sparkContext = createSparkContext(contextOptions);
        sparkMaster = options.getSparkMaster();
      } else if (!options.getSparkMaster().equals(sparkMaster)) {
        throw new IllegalArgumentException(
            String.format(
                "Cannot reuse spark context "
                    + "with different spark master URL. Existing: %s, requested: %s.",
                sparkMaster, options.getSparkMaster()));
      }
      return sparkContext;
    } else {
      return createSparkContext(contextOptions);
    }
  }

  // See https://docs.databricks.com/workflows/jobs/jobs.html#use-the-shared-sparkcontext
  public static synchronized JavaSparkContext getDatabricksSparkContext(SparkPipelineOptions options) {
      LOG.info("Entering JavaSparkContext...");
      SparkContextOptions contextOptions = options.as(SparkContextOptions.class);

      // Honestly not sure what the synchronized stuff is about.
      if (sparkContext == null || sparkContext.sc().isStopped()) {
        // THIS IS THE IMPORTANT THING. Get the static SparkContext.
        SparkContext dbxSparkContext = SparkContext.getOrCreate();

        // Add files to stage.
        if (contextOptions.getFilesToStage() != null && !contextOptions.getFilesToStage().isEmpty()) {
          LOG.info("Staging {} files to Databricks SparkContext", contextOptions.getFilesToStage().size());
          for (String fileToStage : contextOptions.getFilesToStage()) {
            LOG.info("Staging {} to Databricks SparkContext...", fileToStage);
            dbxSparkContext.addJar(fileToStage);
          }
        } else {
          LOG.info("There are no files to stage to DatabricksSparkContext");
        }

        LOG.info("Printing Databricks Spark Context!");
        for (final Tuple2<String, String> tuple : dbxSparkContext.getConf().getAll()) {
          LOG.info(tuple._1() + ", " + tuple._2());
        }

        sparkContext = JavaSparkContext.fromSparkContext(dbxSparkContext);
        sparkMaster = options.getSparkMaster();
      }

      return sparkContext;
  }

  public static synchronized void stopSparkContext(JavaSparkContext context) {
    if (!Boolean.getBoolean(TEST_REUSE_SPARK_CONTEXT) && !usesProvidedSparkContext) {
      context.stop();
    }
  }

  private static JavaSparkContext createSparkContext(SparkContextOptions contextOptions) {
    if (usesProvidedSparkContext) {
      LOG.info("Using a provided Spark Context");
      JavaSparkContext jsc = contextOptions.getProvidedSparkContext();
      if (jsc == null || jsc.sc().isStopped()) {
        LOG.error("The provided Spark context " + jsc + " was not created or was stopped");
        throw new RuntimeException("The provided Spark context was not created or was stopped");
      }
      return jsc;
    } else {
      LOG.info("Creating a brand new Spark Context.");
      SparkConf conf = new SparkConf();
      if (!conf.contains("spark.master")) {
        // set master if not set.
        conf.setMaster(contextOptions.getSparkMaster());
      }

      if (contextOptions.getFilesToStage() != null && !contextOptions.getFilesToStage().isEmpty()) {
        conf.setJars(contextOptions.getFilesToStage().toArray(new String[0]));
      }

      conf.setAppName(contextOptions.getAppName());
      // register immutable collections serializers because the SDK uses them.
      conf.set("spark.kryo.registrator", SparkRunnerKryoRegistrator.class.getName());

      LOG.info("Printing vanilla Beam Spark Context!");
      for (final Tuple2<String, String> tuple : conf.getAll()) {
        LOG.info(tuple._1() + ", " + tuple._2());
      }
      return new JavaSparkContext(conf);
    }

  }
}
