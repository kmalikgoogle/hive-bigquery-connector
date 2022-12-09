/*
 * Copyright 2022 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hive.bigquery.connector.utils;

import java.time.*;
import org.apache.hadoop.hive.common.type.Timestamp;

public class DateTimeUtils {

  public static LocalDateTime convertToUTC(Timestamp ts) {
    LocalDateTime localDateTime =
        LocalDateTime.of(
            ts.getYear(),
            ts.getMonth(),
            ts.getDay(),
            ts.getHours(),
            ts.getMinutes(),
            ts.getSeconds(),
            ts.getNanos());
    return localDateTime
        .atZone(ZoneId.systemDefault())
        .withZoneSameInstant(ZoneId.of("UTC"))
        .toLocalDateTime();
  }

  public static Timestamp convertToHiveTimestamp(long utc) {
    long seconds = utc / 1_000_000;
    int nanos = (int) (utc % 1_000_000) * 1_000;
    ZonedDateTime utcDateTime = Instant.ofEpochSecond(seconds, nanos).atZone(ZoneId.of("UTC"));
    LocalDateTime localDateTime =
        utcDateTime.withZoneSameInstant(ZoneId.systemDefault()).toLocalDateTime();
    return Timestamp.ofEpochSecond(
        localDateTime.toEpochSecond(ZoneOffset.UTC), localDateTime.getNano());
  }

  public static Timestamp convertToSystemTimeZone(LocalDateTime utcLocalDateTime) {
    LocalDateTime localDateTime =
        utcLocalDateTime
            .atZone(ZoneId.of("UTC"))
            .withZoneSameInstant(ZoneId.systemDefault())
            .toLocalDateTime();
    return Timestamp.ofEpochSecond(
        localDateTime.toEpochSecond(ZoneOffset.UTC), localDateTime.getNano());
  }

}
