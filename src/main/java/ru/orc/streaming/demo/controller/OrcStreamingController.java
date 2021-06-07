package ru.orc.streaming.demo.controller;

import java.time.LocalDate;
import javax.servlet.http.HttpServletResponse;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.orc.streaming.demo.generator.OrcDemoGenerator;
//import ru.orc.streaming.demo.reader.OrcStreamingReaderService;

@RestController
public class OrcStreamingController {

//  private final OrcStreamingReaderService readerService;
  private final OrcDemoGenerator orcDemoGenerator;

  @Autowired
  public OrcStreamingController(
    //  OrcStreamingReaderService readerService,
      OrcDemoGenerator orcDemoGenerator) {

   // this.readerService = readerService;
    this.orcDemoGenerator = orcDemoGenerator;
  }
//
//  @SneakyThrows
//  @GetMapping("/read")
//  public void readFromPath(
//      @RequestParam("path") String path,
//      HttpServletResponse httpServletResponse) {
//
//    readerService.readIntoStream(path, httpServletResponse.getOutputStream());
//  }


//  @SneakyThrows
//  @PostMapping("/write")
//  public void writeToPath(
//      @RequestParam("path") String path,
//      @RequestParam("rowCount") long rowCount,
//      @RequestParam("withNulls") boolean withNulls) {
//
//    if (withNulls) {
//      orcDemoGenerator.generateWithAllSupportedTypesAndNulls(path, rowCount);
//    } else {
//      orcDemoGenerator.generateWithAllSupportedTypes(path, rowCount);
//    }
//  }

  @SneakyThrows
  @PostMapping("/write/logs")
  public void writeLogsToPath(
      @RequestParam("path") String path,
      @RequestParam("rowCount") long rowCount) {

    orcDemoGenerator.generateServiceLogsImitation(
        path,
        rowCount,
        LocalDate.of(2021, 5, 10),
        LocalDate.of(2021, 5, 16));
  }

}
