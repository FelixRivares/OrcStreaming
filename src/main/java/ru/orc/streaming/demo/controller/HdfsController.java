package ru.orc.streaming.demo.controller;

import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.orc.streaming.demo.hdfs.HdfsService;

@RestController
public class HdfsController {

  private final HdfsService hdfsService;

  @Autowired
  public HdfsController(HdfsService hdfsService) {
    this.hdfsService = hdfsService;
  }

  @SneakyThrows
  @DeleteMapping("/delete")
  public void deleteFromPath(
      @RequestParam("path") String path,
      @RequestParam("recursive") boolean recursive) {

    hdfsService.deleteDirectory(path, recursive);
  }

  @SneakyThrows
  @GetMapping("/modification")
  public long deleteFromPath(
      @RequestParam("path") String path) {

    return hdfsService.getModificationTime(path);
  }


}
