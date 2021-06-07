package ru.orc.streaming.demo;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import lombok.experimental.UtilityClass;

@UtilityClass
public class Utils {

  public String generateRandomString(int size) {
    int leftLimit = 97; // letter 'a'
    int rightLimit = 122; // letter 'z'
    int targetStringLength = 10;
    Random random = new Random();
    return random.ints(leftLimit, rightLimit + 1)
                 .limit(targetStringLength)
                 .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                 .toString();
  }

  public String getRandomStringFromList(List<String> stringList) {
    return stringList.get(ThreadLocalRandom.current().nextInt(stringList.size()) % stringList.size());
  }

  public LocalDateTime generateRandomLocalDateTime() {
    int hundredYears = 100 * 365;
    LocalDate localDate = LocalDate.ofEpochDay(ThreadLocalRandom.current().nextInt(-hundredYears, hundredYears));
    LocalTime localTime = between(LocalTime.MIN, LocalTime.MAX);
    return LocalDateTime.of(localDate, localTime);
  }

  public LocalDateTime generateRandomLocalDateTimeBetween(LocalDate startDate, LocalDate endDate) {
    long randomDay = ThreadLocalRandom.current().nextLong(startDate.toEpochDay(), endDate.toEpochDay());
    LocalDate localDate = LocalDate.ofEpochDay(randomDay);
    LocalTime localTime = between(LocalTime.MIN, LocalTime.MAX);
    return LocalDateTime.of(localDate, localTime);
  }

  private LocalTime between(LocalTime startTime, LocalTime endTime) {
    int startSeconds = startTime.toSecondOfDay();
    int endSeconds = endTime.toSecondOfDay();
    int randomTime = ThreadLocalRandom.current().nextInt(startSeconds, endSeconds);
    return LocalTime.ofSecondOfDay(randomTime);
  }

}
