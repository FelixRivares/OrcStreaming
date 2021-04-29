package ru.orc.streaming.demo;

import lombok.SneakyThrows;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OrcStreamingReaderConfiguration {

  @Bean
  @SneakyThrows
  @ConditionalOnProperty(name = "fs.type", havingValue = "hdfs")
  public FileSystem fileSystem(
      @Value("${core.site.path}") String coreSitePath,
      @Value("${hdfs.site.path}") String hdfsSitePath) {

    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    conf.addResource(new Path(coreSitePath));
    conf.addResource(new Path(hdfsSitePath));
    return FileSystem.get(conf);
  }

  @Bean
  @SneakyThrows
  @ConditionalOnProperty(name = "fs.type", havingValue = "local")
  public FileSystem localFileSystem() {
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    return FileSystem.get(conf);
  }

}
