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

package org.apache.iceberg.data;

import java.io.IOException;
import java.util.UUID;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FanoutDataWriter;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;

/** Bridges Gearpump to Iceberg's generic record readers and writers. */
public final class GearpumpIcebergData {
  private GearpumpIcebergData() {}

  public static CloseableIterable<Record> read(
      TableScan scan, int taskIndex, int parallelism) {
    return new TaskPartitionedScan(scan, taskIndex, parallelism);
  }

  public static DataWriter<Record> newDataWriter(
      Table table, FileFormat format, EncryptedOutputFile outputFile) {
    GenericFileWriterFactory writerFactory =
        GenericFileWriterFactory.builderFor(table).dataFileFormat(format).build();
    return writerFactory.newDataWriter(outputFile, table.spec(), null);
  }

  /** Creates a rolling task writer that supports unpartitioned and partitioned tables. */
  public static RecordTaskWriter newTaskWriter(
      Table table,
      FileFormat format,
      int taskId,
      long attemptId,
      long targetFileSizeBytes) {
    GenericFileWriterFactory writerFactory =
        GenericFileWriterFactory.builderFor(table).dataFileFormat(format).build();
    OutputFileFactory outputFileFactory =
        OutputFileFactory.builderFor(table, taskId, attemptId)
            .operationId(UUID.randomUUID().toString())
            .format(format)
            .build();

    if (table.spec().isUnpartitioned()) {
      TaskWriter<Record> writer =
          new org.apache.iceberg.io.UnpartitionedWriter<>(
              table.spec(), format, writerFactory, outputFileFactory, table.io(), targetFileSizeBytes);
      return new UnpartitionedTaskWriter(writer);
    }

    return new PartitionedTaskWriter(
        writerFactory, outputFileFactory, table, targetFileSizeBytes);
  }

  /** Public bridge around Iceberg's record task writers. */
  public interface RecordTaskWriter {
    void write(Record record) throws IOException;

    DataFile[] complete() throws IOException;

    void abort() throws IOException;
  }

  private static final class UnpartitionedTaskWriter implements RecordTaskWriter {
    private final TaskWriter<Record> writer;

    private UnpartitionedTaskWriter(TaskWriter<Record> writer) {
      this.writer = writer;
    }

    @Override
    public void write(Record record) throws IOException {
      writer.write(record);
    }

    @Override
    public DataFile[] complete() throws IOException {
      return writer.complete().dataFiles();
    }

    @Override
    public void abort() throws IOException {
      writer.abort();
    }
  }

  private static final class PartitionedTaskWriter implements RecordTaskWriter {
    private final FanoutDataWriter<Record> writer;
    private final PartitionSpec spec;
    private final PartitionKey partitionKey;
    private final Table table;
    private boolean closed = false;

    private PartitionedTaskWriter(
        FileWriterFactory<Record> writerFactory,
        OutputFileFactory outputFileFactory,
        Table table,
        long targetFileSizeBytes) {
      this.writer =
          new FanoutDataWriter<>(writerFactory, outputFileFactory, table.io(), targetFileSizeBytes);
      this.spec = table.spec();
      this.partitionKey = new PartitionKey(spec, table.schema());
      this.table = table;
    }

    @Override
    public void write(Record record) {
      partitionKey.partition(record);
      writer.write(record, spec, partitionKey);
    }

    @Override
    public DataFile[] complete() throws IOException {
      closeWriter();
      return writer.result().dataFiles().toArray(new DataFile[0]);
    }

    @Override
    public void abort() throws IOException {
      closeWriter();
      for (DataFile file : writer.result().dataFiles()) {
        table.io().deleteFile(file.location().toString());
      }
    }

    private void closeWriter() throws IOException {
      if (!closed) {
        writer.close();
        closed = true;
      }
    }
  }

  /** Reads one deterministic partition of the file tasks planned by an Iceberg table scan. */
  private static final class TaskPartitionedScan extends CloseableGroup
      implements CloseableIterable<Record> {
    private final GenericReader reader;
    private final CloseableIterable<CombinedScanTask> plannedTasks;
    private final Iterable<FileScanTask> assignedFiles;
    private boolean iterated = false;

    private TaskPartitionedScan(TableScan scan, int taskIndex, int parallelism) {
      if (parallelism <= 0) {
        throw new IllegalArgumentException("Parallelism must be greater than zero");
      }

      if (taskIndex < 0 || taskIndex >= parallelism) {
        throw new IllegalArgumentException(
            String.format("Task index %s must be between 0 and %s", taskIndex, parallelism - 1));
      }

      this.reader = new GenericReader(scan, false);
      this.plannedTasks = scan.planTasks();
      Iterable<FileScanTask> files =
          Iterables.concat(Iterables.transform(plannedTasks, CombinedScanTask::files));
      this.assignedFiles =
          Iterables.filter(files, file -> assignedTask(file, parallelism) == taskIndex);
    }

    @Override
    public synchronized CloseableIterator<Record> iterator() {
      if (iterated) {
        throw new IllegalStateException("TaskPartitionedScan may only be iterated once");
      }

      iterated = true;
      Iterable<CloseableIterable<Record>> readers =
          Iterables.transform(assignedFiles, reader::open);
      CloseableIterator<Record> iterator = CloseableIterable.concat(readers).iterator();
      addCloseable(iterator);
      return iterator;
    }

    private static int assignedTask(FileScanTask file, int parallelism) {
      int hash = file.file().location().hashCode();
      hash = 31 * hash + Long.hashCode(file.start());
      hash = 31 * hash + Long.hashCode(file.length());
      return Math.floorMod(hash, parallelism);
    }

    @Override
    public void close() throws IOException {
      try {
        plannedTasks.close();
      } finally {
        super.close();
      }
    }
  }
}
