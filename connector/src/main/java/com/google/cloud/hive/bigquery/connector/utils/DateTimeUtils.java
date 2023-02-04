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

import com.google.cloud.bigquery.storage.v1beta2.CivilTimeEncoder;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import java.time.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;

public class DateTimeUtils {

  public static final String TIMEZONED_TIMESTAMP_ERROR =
      String.format(
          "Using timezoned values forbidden for Hive TIMESTAMP. Consider setting the `%s`"
              + " property.",
          HiveBigQueryConfig.HIVE_TIMESTAMP_TIMEZONE);

  public static long getEpochMicrosFromHiveTimestampTZ(TimestampTZ timestampTZ) {
    return timestampTZ.getZonedDateTime().toEpochSecond() * 1_000_000
        + timestampTZ.getZonedDateTime().getNano() / 1_000;
  }

  /**
   * Converts a Hive TIMESTAMP value to a BigQuery TIMESTAMP value. This is non-standard behavior
   * that must be explicitly enabled via configuration.
   */
  public static long getToUTCEpochMicrosFromHiveTimestamp(Configuration conf, Timestamp timestamp) {
    String timezone = conf.get(HiveBigQueryConfig.HIVE_TIMESTAMP_TIMEZONE);
    if (timezone == null) {
      throw new RuntimeException(TIMEZONED_TIMESTAMP_ERROR);
    }
    LocalDateTime localDateTime =
        LocalDateTime.of(
            timestamp.getYear(),
            timestamp.getMonth(),
            timestamp.getDay(),
            timestamp.getHours(),
            timestamp.getMinutes(),
            timestamp.getSeconds(),
            timestamp.getNanos());
    ZonedDateTime zonedDateTime =
        localDateTime.atZone(ZoneId.of(timezone)).withZoneSameInstant(ZoneId.of("UTC"));
    return zonedDateTime.toEpochSecond() * 1_000_000 + zonedDateTime.getNano() / 1_000;
  }

  public static long getEpochMicrosFromHiveTimestamp(Timestamp timestamp) {
    return timestamp.toEpochSecond() * 1_000_000 + timestamp.getNanos() / 1_000;
  }

  public static long getEncodedProtoLongFromHiveTimestamp(Timestamp timestamp) {
    return CivilTimeEncoder.encodePacked64DatetimeMicros(
        org.threeten.bp.LocalDateTime.of(
            timestamp.getYear(),
            timestamp.getMonth(),
            timestamp.getDay(),
            timestamp.getHours(),
            timestamp.getMinutes(),
            timestamp.getSeconds(),
            timestamp.getNanos()));
  }

  /**
   * Converts a BigQuery TIMESTAMP value to a Hive TIMESTAMP value. This is non-standard behavior
   * that must be explicitly enabled via configuration.
   */
  public static Timestamp getHiveTimestampFromUTC(Configuration conf, long utc) {
    String timezone = conf.get(HiveBigQueryConfig.HIVE_TIMESTAMP_TIMEZONE);
    if (timezone == null) {
      throw new RuntimeException(TIMEZONED_TIMESTAMP_ERROR);
    }
    long seconds = utc / 1_000_000;
    int nanos = (int) (utc % 1_000_000) * 1_000;
    ZonedDateTime utcDateTime = Instant.ofEpochSecond(seconds, nanos).atZone(ZoneId.of("UTC"));
    LocalDateTime localDateTime =
        utcDateTime.withZoneSameInstant(ZoneId.of(timezone)).toLocalDateTime();
    return Timestamp.ofEpochSecond(
        localDateTime.toEpochSecond(ZoneOffset.UTC), localDateTime.getNano());
  }

  public static TimestampTZ getHiveTimestampTZFromUTC(long utc) {
    long seconds = utc / 1_000_000;
    int nanos = (int) (utc % 1_000_000) * 1_000;
    ZonedDateTime zonedDateTime = Instant.ofEpochSecond(seconds, nanos).atZone(ZoneId.of("UTC"));
    return new TimestampTZ(zonedDateTime);
  }

  public static Timestamp getHiveTimestampFromLocalDatetime(LocalDateTime localDateTime) {
    return Timestamp.ofEpochSecond(
        localDateTime.toEpochSecond(ZoneOffset.UTC), localDateTime.getNano());
  }
}
