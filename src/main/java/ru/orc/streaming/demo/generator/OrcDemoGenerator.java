package ru.orc.streaming.demo.generator;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.List;
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
public class OrcDemoGenerator {

//  private static final TypeDescription ORC_SCHEMA = TypeDescription.fromString(
//      "struct<longValue:int,doubleValue:double,stringValue:binary,timestampValue:timestamp>");


  private static final TypeDescription ORC_LOG_SCHEMA = TypeDescription.createStruct()
                                                                       .addField("longMetric", TypeDescription.createLong())
                                                                       .addField("serviceName", TypeDescription.createString())
                                                                       .addField("message", TypeDescription.createString())
                                                                       .addField("timestamp", TypeDescription.createTimestamp());

//  private static final TypeDescription ORC_LOG_SCHEMA = TypeDescription.fromString(
//      "struct<longMetric:int,doubleMetric:double,serviceName:binary,message:binary,timestamp:timestamp>");

//  private static final TypeDescription ORC_LOG_SCHEMA = TypeDescription.fromString(
//      "struct<longMetric:int,serviceName:binary,message:binary,timestamp:timestamp>");

  private static final List<String> SERVICE_NAMES = List.of("loggerService", "payloadService", "notificationService");

  private final FileSystem fileSystem;

  @Autowired
  public OrcDemoGenerator(FileSystem fileSystem) {
    this.fileSystem = fileSystem;
  }

  @SneakyThrows
  public void generateServiceLogsImitation(String path, long rowCount, LocalDate startDate, LocalDate endDate) {
    log.info("Start generating ORC with all service log imitation...");
//    try (Writer writer = OrcFile.createWriter(
//        new Path(path),
//        OrcFile.writerOptions(fileSystem.getConf()).setSchema(ORC_LOG_SCHEMA))) {
    Writer writer = OrcFile.createWriter(
        new Path(path),
        OrcFile.writerOptions(fileSystem.getConf()).setSchema(ORC_LOG_SCHEMA));

    VectorizedRowBatch batch = ORC_LOG_SCHEMA.createRowBatch();
    LongColumnVector countMetricVector = (LongColumnVector) batch.cols[0];
    // DoubleColumnVector averageValueMetricVector = (DoubleColumnVector) batch.cols[1];
    BytesColumnVector serviceNameVector = (BytesColumnVector) batch.cols[1];
    BytesColumnVector logMessageVector = (BytesColumnVector) batch.cols[2];
    TimestampColumnVector timestampVector = (TimestampColumnVector) batch.cols[3];
    for (int rowIndex = 0, iteration = 0; iteration < rowCount; iteration++, rowIndex = batch.size++) {
      log.info("Iteration: [{}]", iteration);
      long randomCountMetric = new Random().nextLong();
      double randomAverageValueMetric = new Random().nextDouble();
      String randomLogMessage = Utils.generateRandomString(10);
      String randomServiceName = Utils.getRandomStringFromList(SERVICE_NAMES);
      Timestamp randomTimestampValue = Timestamp.valueOf(Utils.generateRandomLocalDateTimeBetween(startDate, endDate));
      log.debug(
          "Fill rowBatch with the following values:\n"
              + " randomCountMetric: [{}],\n"
              + " randomAverageValueMetric: [{}],\n"
              + " randomLogMessage: [{}],\n"
              + " randomServiceName: [{}],\n"
              + " randomTimestampValue: [{}]",
          randomCountMetric,
          randomAverageValueMetric,
          randomLogMessage,
          randomServiceName,
          randomTimestampValue);
      countMetricVector.vector[rowIndex] = randomCountMetric;
      // averageValueMetricVector.vector[rowIndex] = randomAverageValueMetric;
      logMessageVector.setVal(rowIndex, randomLogMessage.getBytes());
      serviceNameVector.setVal(rowIndex, randomServiceName.getBytes());
      timestampVector.time[rowIndex] = randomTimestampValue.getTime();
      timestampVector.nanos[rowIndex] = randomTimestampValue.getNanos();
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

}

//  @SneakyThrows
//  public void generateWithAllSupportedTypes(String path, long rowCount) {
//    log.info("Start generating ORC with all supported types...");
//    try (Writer writer = OrcFile.createWriter(
//        new Path(path),
//        OrcFile.writerOptions(fileSystem.getConf()).setSchema(ORC_SCHEMA))) {
//      VectorizedRowBatch batch = ORC_SCHEMA.createRowBatch();
//      LongColumnVector longColumnVector = (LongColumnVector) batch.cols[0];
//      DoubleColumnVector doubleColumnVector = (DoubleColumnVector) batch.cols[1];
//      BytesColumnVector bytesColumnVector = (BytesColumnVector) batch.cols[2];
//      TimestampColumnVector timestampColumnVector = (TimestampColumnVector) batch.cols[3];
//      for (int rowIndex = 0, iteration = 0; iteration < rowCount; iteration++, rowIndex = batch.size++) {
//
//        long randomLongValue = new Random().nextLong();
//        double randomDoubleValue = new Random().nextDouble();
//        String randomStringValue = Utils.generateRandomString(10);
//        Timestamp randomTimestampValue = Timestamp.valueOf(Utils.generateRandomLocalDateTime());
//        log.info(
//            "Fill rowBatch with the following values:\n"
//                + " longValue: [{}],\n"
//                + " doubleValue: [{}],\n"
//                + " stringValue: [{}],\n"
//                + " timestampValue: [{}]",
//            randomLongValue,
//            randomDoubleValue,
//            randomStringValue,
//            randomTimestampValue);
//        longColumnVector.vector[rowIndex] = randomLongValue;
//        doubleColumnVector.vector[rowIndex] = randomDoubleValue;
//        bytesColumnVector.setVal(rowIndex, randomStringValue.getBytes());
//        timestampColumnVector.time[rowIndex] = randomTimestampValue.getTime();
//        timestampColumnVector.nanos[rowIndex] = randomTimestampValue.getNanos();
//        if (rowIndex == batch.size - 1) {
//
//          //   if (batch.size == batch.getMaxSize()) {
//          writer.addRowBatch(batch);
//          batch.reset();
//        }
//      }
//      if (batch.size != 0) {
//        writer.addRowBatch(batch);
//        batch.reset();
//      }
//    }
//  }
//
//  @SneakyThrows
//  public void generateWithAllSupportedTypesAndNulls(String path, long rowCount) {
//    log.info("Start generation ORC with all supported types and nulls...");
//    try (Writer writer = OrcFile.createWriter(
//        new Path(path),
//        OrcFile.writerOptions(fileSystem.getConf()).setSchema(ORC_SCHEMA))) {
//
//      VectorizedRowBatch batch = ORC_SCHEMA.createRowBatch();
//      LongColumnVector longColumnVector = (LongColumnVector) batch.cols[0];
//      longColumnVector.noNulls = false;
//      DoubleColumnVector doubleColumnVector = (DoubleColumnVector) batch.cols[1];
//      doubleColumnVector.noNulls = false;
//      BytesColumnVector bytesColumnVector = (BytesColumnVector) batch.cols[2];
//      bytesColumnVector.noNulls = false;
//      TimestampColumnVector timestampColumnVector = (TimestampColumnVector) batch.cols[3];
//      timestampColumnVector.noNulls = false;
//      for (int rowIndex = 0, iteration = 0; iteration < rowCount; iteration++, rowIndex = batch.size++) {
//
//        long randomLongValue = new Random().nextLong();
//        double randomDoubleValue = new Random().nextDouble();
//        String randomStringValue = Utils.generateRandomString(10);
//        Timestamp randomTimestampValue = Timestamp.valueOf(Utils.generateRandomLocalDateTime());
//        // each even for long and double is not null and otherwise for string and timestamp
//        if (rowIndex % 2 == 0) {
//          log.info(
//              "Fill rowBatch with the following values:\n"
//                  + " longValue: [{}],\n"
//                  + " doubleValue: [{}],\n"
//                  + " stringValue: [{}],\n"
//                  + " timestampValue: [{}]",
//              randomLongValue,
//              randomDoubleValue,
//              null,
//              null);
//          longColumnVector.vector[rowIndex] = randomLongValue;
//          doubleColumnVector.vector[rowIndex] = randomDoubleValue;
//          bytesColumnVector.isNull[rowIndex] = true;
//          timestampColumnVector.isNull[rowIndex] = true;
//        } else {
//          log.info(
//              "Fill rowBatch with the following values:\n"
//                  + " longValue: [{}],\n"
//                  + " doubleValue: [{}],\n"
//                  + " stringValue: [{}],\n"
//                  + " timestampValue: [{}]",
//              null,
//              null,
//              randomStringValue,
//              randomTimestampValue);
//          longColumnVector.isNull[rowIndex] = true;
//          doubleColumnVector.isNull[rowIndex] = true;
//          bytesColumnVector.setVal(rowIndex, randomStringValue.getBytes());
//          timestampColumnVector.time[rowIndex] = randomTimestampValue.getTime();
//          timestampColumnVector.nanos[rowIndex] = randomTimestampValue.getNanos();
//        }
//        if (rowIndex == batch.size - 1) {
//          writer.addRowBatch(batch);
//          batch.reset();
//        }
//      }
//      if (batch.size != 0) {
//        writer.addRowBatch(batch);
//        batch.reset();
//      }
//    }
//  }
