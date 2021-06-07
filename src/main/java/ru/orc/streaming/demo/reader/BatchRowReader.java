//package ru.orc.streaming.demo.reader;
//
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.function.Function;
//import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
//import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
//import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
//import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
//import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
//import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
//import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
//import org.springframework.stereotype.Component;
//
//@Component
//public class BatchRowReader {
//
//  public Map<String, Object> readRow(VectorizedRowBatch batch, List<String> fieldNames, int rowIndex) {
//    Map<String, Object> message = new HashMap<>();
//    for (int columnIndex = 0; columnIndex < batch.numCols; columnIndex++) {
//      Type type = batch.cols[columnIndex].type;
//      switch (type) {
//        case LONG:
//          LongColumnVector longColumnVector = (LongColumnVector) batch.cols[columnIndex];
//          putValueInMessageIfNotNull(
//              message,
//              longColumnVector,
//              columnIndex,
//              rowIndex,
//              fieldNames,
//              longValue -> longColumnVector.vector[rowIndex]);
//          break;
//        case DOUBLE:
//          DoubleColumnVector doubleColumnVector = (DoubleColumnVector) batch.cols[columnIndex];
//          putValueInMessageIfNotNull(
//              message,
//              doubleColumnVector,
//              columnIndex,
//              rowIndex,
//              fieldNames,
//              doubleValue -> doubleColumnVector.vector[rowIndex]);
//          break;
//        case BYTES:
//          BytesColumnVector bytesColumnVector = (BytesColumnVector) batch.cols[columnIndex];
//          putValueInMessageIfNotNull(
//              message,
//              bytesColumnVector,
//              columnIndex,
//              rowIndex,
//              fieldNames,
//              stringValue -> bytesColumnVector.toString(rowIndex));
//          break;
//        case TIMESTAMP:
//          TimestampColumnVector timestampColumnVector = (TimestampColumnVector) batch.cols[columnIndex];
//          putValueInMessageIfNotNull(
//              message,
//              timestampColumnVector,
//              columnIndex,
//              rowIndex,
//              fieldNames,
//              timestampValue -> timestampColumnVector.asScratchTimestamp(rowIndex).toLocalDateTime().toString());
//          break;
//        case NONE:
//        case DECIMAL:
//        case DECIMAL_64:
//        case INTERVAL_DAY_TIME:
//        case STRUCT:
//        case LIST:
//        case MAP:
//        case UNION:
//        case VOID:
//          throw new IllegalArgumentException(String.format("Type [%s] is unsupported", type));
//      }
//    }
//    return message;
//  }
//
//  private <T> void putValueInMessageIfNotNull(
//      Map<String, Object> message,
//      ColumnVector columnVector,
//      int columnIndex,
//      int rowIndex,
//      List<String> fieldNames,
//      Function<Integer, T> getValueFunction) {
//
//    if (!columnVector.isNull[rowIndex]) {
//      message.put(fieldNames.get(columnIndex), getValueFunction.apply(columnIndex));
//    }
//  }
//
//}
