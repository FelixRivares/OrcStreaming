package ru.orc.streaming.demo.generator;

import java.sql.Timestamp;
import java.util.Random;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.orc.streaming.demo.Utils;

@Component
@Slf4j
public class OrcGenerator {

  private final FileSystem fileSystem;

  @Autowired
  public OrcGenerator(FileSystem fileSystem) {
    this.fileSystem = fileSystem;
  }

  @SneakyThrows
  public void generateWithAllSupportedTypes(String path, long rowCount) {
    TypeDescription schema = TypeDescription.fromString(
        "struct<longValue:int,doubleValue:double,stringValue:binary,timestampValue:timestamp>");
    Writer writer = OrcFile.createWriter(new Path(path), OrcFile.writerOptions(fileSystem.getConf()).setSchema(schema));
    VectorizedRowBatch batch = schema.createRowBatch();
    LongColumnVector longColumnVector = (LongColumnVector) batch.cols[0];
    DoubleColumnVector doubleColumnVector = (DoubleColumnVector) batch.cols[1];
    BytesColumnVector bytesColumnVector = (BytesColumnVector) batch.cols[2];
    TimestampColumnVector timestampColumnVector = (TimestampColumnVector) batch.cols[3];
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++, batch.size++) {
      long randomLongValue = new Random().nextLong();
      double randomDoubleValue = new Random().nextDouble();
      String randomStringValue = Utils.generateRandomString(10);
      Timestamp randomTimestampValue = Timestamp.valueOf(Utils.generateRandomLocalDateTime());
      log.info(
          "Fill rowBatch with the following values:\n"
              + " longValue: [{}],\n"
              + " doubleValue: [{}],\n"
              + " stringValue: [{}],\n"
              + " timestampValue: [{}]",
          randomLongValue,
          randomDoubleValue,
          randomStringValue,
          randomTimestampValue);
      longColumnVector.vector[rowIndex] = randomLongValue;
      doubleColumnVector.vector[rowIndex] = randomDoubleValue;
      bytesColumnVector.setVal(rowIndex, randomStringValue.getBytes());
      timestampColumnVector.time[rowIndex] = randomTimestampValue.getTime();
      timestampColumnVector.nanos[rowIndex] = randomTimestampValue.getNanos();
      if (rowIndex == batch.size - 1) {
        writer.addRowBatch(batch);
        batch.reset();
      }
    }
    if (batch.size != 0) {
      writer.addRowBatch(batch);
      batch.reset();
    }
    writer.close();
  }

  public void generateWithAllSuportedTypesAndNulls(String path, long rowCount) {

  }


}
