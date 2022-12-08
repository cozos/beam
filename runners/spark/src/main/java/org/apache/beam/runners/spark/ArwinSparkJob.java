package org.apache.beam.runners.spark;

import com.google.common.base.Charsets;
import org.apache.spark.SparkContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ArwinSparkJob {
    public static void main() {
        try {
          String hostname = new BufferedReader(
            new InputStreamReader(Runtime.getRuntime().exec(new String[]{"hostname", "-I"}).getInputStream(), Charsets.UTF_8))
           .readLine();
          System.out.println(hostname);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
    }
}
