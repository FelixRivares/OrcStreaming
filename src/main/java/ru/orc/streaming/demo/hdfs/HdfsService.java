package ru.orc.streaming.demo.hdfs;

import java.util.ArrayList;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class HdfsService {

  private final FileSystem fileSystem;

  @Autowired
  public HdfsService(FileSystem fileSystem) {
    this.fileSystem = fileSystem;
  }

  @SneakyThrows
  public List<Path> listOrcFiles(String path) {
    List<Path> filePaths = new ArrayList<>();
    for (FileStatus fileStatus : fileSystem.listStatus(new Path(path))) {
      Path filePath = fileStatus.getPath();
      if (FilenameUtils.getExtension(filePath.toString()).equals("orc")) {
        filePaths.add(filePath);
      }
    }
    return filePaths;
  }

  @SneakyThrows
  public void deleteDirectory(String path, boolean recursive) {
    fileSystem.delete(new Path(path), recursive);
  }

  @SneakyThrows
  public long getModificationTime(String path) {
    return fileSystem.getFileStatus(new Path(path)).getModificationTime();
  }

}
