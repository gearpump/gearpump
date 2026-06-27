/*
 * Licensed under the Apache License, Version 2.0 (the
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
package io.gearpump.examples.beam.quickstart;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.beam.runners.gearpump.GearpumpPipelineOptions;
import org.apache.beam.runners.gearpump.GearpumpPipelineResult;
import org.apache.beam.runners.gearpump.GearpumpRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;

/** Quick start Beam pipeline for the Gearpump runner. */
public final class BeamQuickStart {

  private static final int EXPECTED_OUTPUT_LINES = 6;

  private BeamQuickStart() {}

  public interface QuickStartOptions extends GearpumpPipelineOptions {

    @Description("Local file written by the Beam workers")
    @Default.String("target/beam-quick-start-output.txt")
    String getOutput();

    void setOutput(String output);

    @Description("Milliseconds to wait for the bounded local output")
    @Default.Long(30000)
    Long getWaitMillis();

    void setWaitMillis(Long waitMillis);

    @Description("Keep the Gearpump application running after the bounded output is written")
    @Default.Boolean(false)
    Boolean getKeepRunning();

    void setKeepRunning(Boolean keepRunning);

    @Description("Milliseconds to keep running after output; 0 means until interrupted")
    @Default.Long(0)
    Long getKeepRunningMillis();

    void setKeepRunningMillis(Long keepRunningMillis);
  }

  public static void main(String[] args) throws Exception {
    QuickStartOptions options =
        PipelineOptionsFactory.fromArgs(args).as(QuickStartOptions.class);
    options.setRunner(GearpumpRunner.class);
    if (options.getApplicationName() == null) {
      options.setApplicationName("beamQuickStart");
    }

    Path output = Paths.get(options.getOutput()).toAbsolutePath();
    prepareOutput(output);

    Pipeline pipeline = Pipeline.create(options);
    if (Boolean.TRUE.equals(options.getKeepRunning())) {
      Path keepRunningOutput = Paths.get(options.getOutput() + ".keep-running").toAbsolutePath();
      prepareOutput(keepRunningOutput);
      pipeline
          .apply(
              "KeepRunningInput",
              Read.from(new KeepRunningSource(options.getKeepRunningMillis())))
          .apply(
              "WriteKeepRunningOutput",
              ParDo.of(new WriteOutputFn(keepRunningOutput.toString())));
    }
    pipeline
        .apply(
            "Input",
            Create.of("Apache Beam runs on Gearpump", "Gearpump runs Beam pipelines"))
        .apply("ExtractWords", ParDo.of(new ExtractWordsFn()))
        .apply("CountWords", Sum.integersPerKey())
        .apply("FormatCounts", ParDo.of(new FormatCountsFn()))
        .apply("WriteOutput", ParDo.of(new WriteOutputFn(output.toString())));

    GearpumpPipelineResult result = null;
    try {
      result = (GearpumpPipelineResult) pipeline.run();
      List<String> lines =
          waitForOutput(output, EXPECTED_OUTPUT_LINES, options.getWaitMillis());
      System.out.println("Beam quick start output:");
      for (String line : lines) {
        System.out.println(line);
      }
      if (Boolean.TRUE.equals(options.getKeepRunning())) {
        keepRunning(options.getKeepRunningMillis());
      }
    } finally {
      if (result != null) {
        try {
          result.cancel();
        } finally {
          result.getClientContext().close();
        }
      }
    }
  }

  private static void prepareOutput(Path output) throws IOException {
    Path parent = output.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    Files.deleteIfExists(output);
  }

  private static List<String> waitForOutput(Path output, int expectedLineCount, long waitMillis)
      throws IOException, InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(waitMillis);
    while (System.nanoTime() < deadline) {
      if (Files.exists(output)) {
        TreeSet<String> lines =
            new TreeSet<>(Files.readAllLines(output, StandardCharsets.UTF_8));
        if (lines.size() >= expectedLineCount) {
          return new ArrayList<>(lines);
        }
      }
      Thread.sleep(250);
    }
    throw new IllegalStateException(
        "Timed out waiting for Beam quick start output in " + output);
  }

  private static void keepRunning(long keepRunningMillis) throws InterruptedException {
    if (keepRunningMillis > 0) {
      System.out.println(
          "Keeping Beam quick start application running for " + keepRunningMillis + " ms.");
      Thread.sleep(keepRunningMillis);
    } else {
      System.out.println("Keeping Beam quick start application running until interrupted.");
      new CountDownLatch(1).await();
    }
  }

  private static final class KeepRunningSource
      extends UnboundedSource<String, KeepRunningCheckpoint> {
    private static final long serialVersionUID = 1L;
    private static final long INTERVAL_MILLIS = 1000L;

    private final long keepRunningMillis;

    private KeepRunningSource(long keepRunningMillis) {
      this.keepRunningMillis = keepRunningMillis;
    }

    @Override
    public List<? extends UnboundedSource<String, KeepRunningCheckpoint>> split(
        int desiredNumSplits, PipelineOptions options) {
      return Collections.singletonList(this);
    }

    @Override
    public UnboundedReader<String> createReader(
        PipelineOptions options, KeepRunningCheckpoint checkpointMark) {
      return new KeepRunningReader(this);
    }

    @Override
    public void validate() {}

    @Override
    public Coder<String> getOutputCoder() {
      return StringUtf8Coder.of();
    }

    @Override
    public Coder<KeepRunningCheckpoint> getCheckpointMarkCoder() {
      return SerializableCoder.of(KeepRunningCheckpoint.class);
    }
  }

  private static final class KeepRunningReader extends UnboundedSource.UnboundedReader<String> {
    private final KeepRunningSource source;
    private long startNanos;
    private long sequence;
    private String current;

    private KeepRunningReader(KeepRunningSource source) {
      this.source = source;
    }

    @Override
    public boolean start() {
      startNanos = System.nanoTime();
      current = keepRunningMessage();
      return true;
    }

    @Override
    public boolean advance() throws IOException {
      if (source.keepRunningMillis > 0
          && elapsedMillis() >= source.keepRunningMillis) {
        return false;
      }
      try {
        Thread.sleep(KeepRunningSource.INTERVAL_MILLIS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while emitting keep-running records", e);
      }
      sequence++;
      current = keepRunningMessage();
      return true;
    }

    @Override
    public String getCurrent() {
      return current;
    }

    @Override
    public org.joda.time.Instant getCurrentTimestamp() {
      return org.joda.time.Instant.now();
    }

    @Override
    public void close() {}

    @Override
    public UnboundedSource<String, ?> getCurrentSource() {
      return source;
    }

    @Override
    public org.joda.time.Instant getWatermark() {
      return org.joda.time.Instant.now();
    }

    @Override
    public UnboundedSource.CheckpointMark getCheckpointMark() {
      return new KeepRunningCheckpoint();
    }

    private long elapsedMillis() {
      return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }

    private String keepRunningMessage() {
      return "keep-running=" + sequence;
    }
  }

  private static final class KeepRunningCheckpoint
      implements UnboundedSource.CheckpointMark, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public void finalizeCheckpoint() {}
  }

  private static final class ExtractWordsFn extends DoFn<String, KV<String, Integer>> {
    private static final Pattern WORD_SEPARATOR = Pattern.compile("[^A-Za-z]+");

    @ProcessElement
    public void processElement(ProcessContext context) {
      for (String word :
          WORD_SEPARATOR.split(context.element().toLowerCase(Locale.ROOT))) {
        if (!word.isEmpty()) {
          context.output(KV.of(word, 1));
        }
      }
    }
  }

  private static final class FormatCountsFn extends DoFn<KV<String, Integer>, String> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      KV<String, Integer> element = context.element();
      context.output(element.getKey() + "=" + element.getValue());
    }
  }

  private static final class WriteOutputFn extends DoFn<String, String> {
    private final String output;

    private WriteOutputFn(String output) {
      this.output = output;
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
      String line = context.element();
      Files.write(
          Paths.get(output),
          Collections.singletonList(line),
          StandardCharsets.UTF_8,
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND);
      context.output(line);
    }
  }
}
