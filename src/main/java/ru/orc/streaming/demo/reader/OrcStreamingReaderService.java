package ru.orc.streaming.demo.reader;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.orc.streaming.demo.hdfs.HdfsService;

@Service
@Slf4j
public class OrcStreamingReaderService {

  private final FileSystem fileSystem;
  private final HdfsService hdfsService;
  private final BatchRowReader batchRowReader;
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Autowired
  public OrcStreamingReaderService(
      FileSystem fileSystem,
      HdfsService hdfsService,
      BatchRowReader batchRowReader) {

    this.fileSystem = fileSystem;
    this.hdfsService = hdfsService;
    this.batchRowReader = batchRowReader;
  }

  @SneakyThrows
  public void readIntoStream(String path, OutputStream outputStream) {
    List<Path> resultOrcFilesPaths = hdfsService.listOrcFiles(path);
    for (Path orcFilesPath : resultOrcFilesPaths) {
      Reader reader = OrcFile.createReader(orcFilesPath, OrcFile.readerOptions(fileSystem.getConf()));
      long size = reader.getNumberOfRows();
      log.info("numberOfRows [{}]", size);
      try (RecordReader rows = reader.rows()) {
        rows.seekToRow(0);
        //  log.info("Schema [{}]", reader.getSchema().toJson());
        log.info("Schema [{}]", reader.getSchema().toString());
        // List<TypeDescription> children = reader.getSchema().getChildren();
        List<String> fieldNames = reader.getSchema().getFieldNames();
//        List<Category> collect = fieldNames.stream()
//                                           .map(fieldName -> reader.getSchema().findSubtype(fieldName))
//                                           .map(TypeDescription::getCategory)
//                                           .collect(Collectors.toList());
//        for (Category child : collect) {
//          log.info(child.getName());
//        }
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
        int i = 0;
        while (rows.nextBatch(batch)) {
          log.info("progress - [{}]", rows.getProgress());
          log.info("index - [{}]", rows.getRowNumber());
          for (int rowIndex = 0; rowIndex < batch.size; rowIndex++, i++) {
            Map<String, Object> message = batchRowReader.readRow(
                batch,
                reader.getSchema().getFieldNames(),
                rowIndex);
            outputStream.write(objectMapper.writeValueAsBytes(message));
          }
          if (i > 10) {
            break;
          }
        }
        log.info("i = [{}]", i);
      }
    }
  }

}
