package ru.orc.streaming.demo;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class OrcStreamingReader {

  public static void main(String[] args) {
    new SpringApplicationBuilder().sources(OrcStreamingReader.class).run(args);
  }


}
