package ru.orc.streaming.demo.controller;

import java.io.File;
import java.io.FileOutputStream;
import javax.servlet.http.HttpServletResponse;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.orc.streaming.demo.generator.OrcGenerator;
import ru.orc.streaming.demo.reader.OrcStreamingReaderService;

@RestController
public class OrcStreamingController {

  private final OrcStreamingReaderService readerService;
  private final OrcGenerator orcGenerator;

  @Autowired
  public OrcStreamingController(
      OrcStreamingReaderService readerService,
      OrcGenerator orcGenerator) {

    this.readerService = readerService;
    this.orcGenerator = orcGenerator;
  }

  @SneakyThrows
  @GetMapping("/read")
  public void readFromPath(
      @RequestParam("path") String path,
      HttpServletResponse httpServletResponse) {

    readerService.readIntoStream(path, httpServletResponse.getOutputStream());

    //readerService.readIntoStream(path, new FileOutputStream(new File("pes.json")));
  }


  @SneakyThrows
  @PostMapping("/write")
  public void writeToPath(
      @RequestParam("path") String path,
      @RequestParam("rowCount") long rowCount) {

    orcGenerator.generateWithAllSupportedTypes(path, rowCount);
  }

}
