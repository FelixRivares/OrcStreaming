package ru.orc.streaming.demo.merge;

import java.util.List;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.orc.streaming.demo.hdfs.HdfsService;

@Service
@Slf4j
public class OrcFileMerger {

  private final FileSystem fileSystem;
  private final HdfsService hdfsService;

  @Autowired
  public OrcFileMerger(
      FileSystem fileSystem,
      HdfsService hdfsService) {

    this.fileSystem = fileSystem;
    this.hdfsService = hdfsService;
  }

  @SneakyThrows
  public void merge(String outputPathString, String sourcePath) {
    List<Path> sourcePaths = hdfsService.listOrcFiles(sourcePath);
    log.info("Start merging files [{}] to file [{}]", sourcePaths, outputPathString);
    List<Path> merged = OrcFile.mergeFiles(
        new Path(outputPathString),
        OrcFile.writerOptions(fileSystem.getConf()),
        sourcePaths);
    log.info("Successfully merged files [{}]", merged);
  }

}
