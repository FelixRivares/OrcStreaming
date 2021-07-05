package ru.orc.streaming.demo.controller;

import javax.servlet.http.HttpServletResponse;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.orc.streaming.demo.generator.OrcDemoGenerator;
import ru.orc.streaming.demo.merge.OrcFileMerger;
import ru.orc.streaming.demo.reader.OrcStreamingReaderService;

@RestController
public class OrcStreamingController {

  private final OrcStreamingReaderService readerService;
  private final OrcFileMerger orcFileMerger;
  private final OrcDemoGenerator orcDemoGenerator;

  @Autowired
  public OrcStreamingController(
      OrcStreamingReaderService readerService,
      OrcFileMerger orcFileMerger,
      OrcDemoGenerator orcDemoGenerator) {

    this.readerService = readerService;
    this.orcFileMerger = orcFileMerger;
    this.orcDemoGenerator = orcDemoGenerator;
  }

  @SneakyThrows
  @GetMapping("/read")
  public void readFromPath(
      @RequestParam("path") String path,
      HttpServletResponse httpServletResponse) {

    readerService.readIntoStream(path, httpServletResponse.getOutputStream());
  }


  @SneakyThrows
  @PostMapping("/write")
  public void writeToPath(
      @RequestParam("path") String path,
      @RequestParam("rowCount") long rowCount,
      @RequestParam("withNulls") boolean withNulls) {

    if (withNulls) {
      orcDemoGenerator.generateWithAllSupportedTypesAndNulls(path, rowCount);
    } else {
      orcDemoGenerator.generateWithAllSupportedTypes(path, rowCount);
    }
  }

  @SneakyThrows
  @PostMapping("/merge")
  public void mergeFiles(
      @RequestParam("toFilePath") String toFilePath,
      @RequestParam("sourcePath") String sourcePath) {

    orcFileMerger.merge(toFilePath, sourcePath);
  }

}
