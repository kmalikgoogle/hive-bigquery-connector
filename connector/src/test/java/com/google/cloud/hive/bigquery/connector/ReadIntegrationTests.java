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
package com.google.cloud.hive.bigquery.connector;

import static com.google.cloud.hive.bigquery.connector.TestUtils.*;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.utils.DateTimeUtils;
import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import repackaged.by.hivebqconnector.com.google.common.collect.Streams;

public class ReadIntegrationTests extends IntegrationTestsBase {

  /**
   * Check that attempting to read a table that doesn't exist fails gracefully with a useful error
   * message
   */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_READ_FORMAT)
  public void testReadNonExistingTable(String engine, String readDataFormat) {
    initHive(engine, readDataFormat);
    // Make sure the table doesn't exist in BigQuery
    dropBqTableIfExists(dataset, TEST_TABLE_NAME);
    assertFalse(bQTableExists(dataset, TEST_TABLE_NAME));
    // Create a Hive table without creating its corresponding table in BigQuery
    createExternalTable(TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL);
    // Attempt to read the table
    Throwable exception =
        assertThrows(
            RuntimeException.class,
            () -> runHiveStatement(String.format("SELECT * FROM %s", TEST_TABLE_NAME)));
    assertTrue(
        exception
            .getMessage()
            .contains(
                String.format(
                    "Table '%s.%s.%s' not found", getProject(), dataset, TEST_TABLE_NAME)));
  }

  // -----------------------------------------------------------------------------------------------

  /** Check that reading an empty BQ table actually returns 0 results. */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_READ_FORMAT)
  public void testReadEmptyTable(String engine, String readDataFormat) {
    initHive(engine, readDataFormat);
    createExternalTable(TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL, BIGQUERY_TEST_TABLE_DDL);
    List<Object[]> rows = runHiveStatement(String.format("SELECT * FROM %s", TEST_TABLE_NAME));
    assertThat(rows).isEmpty();
  }

  // ---------------------------------------------------------------------------------------------------

  /** Test the WHERE clause */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_READ_FORMAT)
  public void testWhereClause(String engine, String readDataFormat) {
    initHive(engine, readDataFormat);
    createExternalTable(TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL, BIGQUERY_TEST_TABLE_DDL);
    // Insert data into BQ using the BQ SDK
    runBqQuery(
        String.format(
            "INSERT `${dataset}.%s` VALUES (123, 'hello'), (999, 'abcd')", TEST_TABLE_NAME));
    // Make sure the initial data is there
    TableResult result =
        runBqQuery(String.format("SELECT * FROM `${dataset}.%s`", TEST_TABLE_NAME));
    assertEquals(2, result.getTotalRows());
    // Read filtered data using Hive
    List<Object[]> rows =
        runHiveStatement(String.format("SELECT * FROM %s WHERE number = 999", TEST_TABLE_NAME));
    // Verify we get the expected rows
    assertArrayEquals(
        new Object[] {
          new Object[] {999L, "abcd"},
        },
        rows.toArray());
    // TODO: Confirm that the predicate was in fact pushed down to BigQuery
  }

  // ---------------------------------------------------------------------------------------------------

  /** Test the `SELECT` statement with explicit columns (i.e. not `SELECT *`) */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_READ_FORMAT)
  public void testSelectExplicitColumns(String engine, String readDataFormat) {
    initHive(engine, readDataFormat);
    createExternalTable(TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL, BIGQUERY_TEST_TABLE_DDL);
    // Insert data into BQ using the BQ SDK
    runBqQuery(
        String.format(
            "INSERT `${dataset}.%s` VALUES (123, 'hello'), (999, 'abcd')", TEST_TABLE_NAME));
    TableResult result =
        runBqQuery(String.format("SELECT * FROM `${dataset}.%s`", TEST_TABLE_NAME));
    // Make sure the initial data is there
    assertEquals(2, result.getTotalRows());
    // Read filtered data using Hive
    // Try with both columns in order
    List<Object[]> rows =
        runHiveStatement(
            String.format("SELECT number, text FROM %s ORDER BY number", TEST_TABLE_NAME));
    assertArrayEquals(
        new Object[] {
          new Object[] {123L, "hello"},
          new Object[] {999L, "abcd"}
        },
        rows.toArray());
    // Try in different order
    rows =
        runHiveStatement(
            String.format("SELECT text, number FROM %s ORDER BY number", TEST_TABLE_NAME));
    assertArrayEquals(
        new Object[] {
          new Object[] {"hello", 123L},
          new Object[] {"abcd", 999L}
        },
        rows.toArray());
    // Try a single column
    rows =
        runHiveStatement(String.format("SELECT number FROM %s ORDER BY number", TEST_TABLE_NAME));
    assertArrayEquals(new Object[] {new Object[] {123L}, new Object[] {999L}}, rows.toArray());
    // Try another single column
    rows = runHiveStatement(String.format("SELECT text FROM %s ORDER BY text", TEST_TABLE_NAME));
    assertArrayEquals(new Object[] {new Object[] {"abcd"}, new Object[] {"hello"}}, rows.toArray());
  }

  // ---------------------------------------------------------------------------------------------------

  /** Smoke test to make sure BigQuery accepts all different types of pushed predicates */
  @Test
  public void testWhereClauseAllTypes() {
    initHive();
    createExternalTable(
        ALL_TYPES_TABLE_NAME, HIVE_ALL_TYPES_TABLE_DDL, BIGQUERY_ALL_TYPES_TABLE_DDL);
    runHiveScript(
        String.join(
            "\n",
            "SELECT * FROM " + ALL_TYPES_TABLE_NAME + " WHERE",
            "((int_val > 10 AND bl = TRUE)",
            "OR (str = 'hello' OR day >= to_date('2000-01-01')))",
            "AND (ts BETWEEN TIMESTAMP'2018-09-05 00:10:04.19' AND"
                + " TIMESTAMP'2019-06-11 03:55:10.00')",
            "AND (fl <= 4.2)"));
    // TODO: Confirm that the predicates were in fact pushed down to BigQuery
  }

  // ---------------------------------------------------------------------------------------------------

  /** Test the "SELECT COUNT(*)" statement. */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_READ_FORMAT)
  public void testCount(String engine, String readDataFormat) {
    initHive(engine, readDataFormat);
    createExternalTable(TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL, BIGQUERY_TEST_TABLE_DDL);
    // Create some initial data in BQ
    runBqQuery(
        String.format(
            "INSERT `${dataset}.%s` VALUES (123, 'hello'), (999, 'abcd')", TEST_TABLE_NAME));
    TableResult result =
        runBqQuery(String.format("SELECT * FROM `${dataset}.%s`", TEST_TABLE_NAME));
    // Make sure the initial data is there
    assertEquals(2, result.getTotalRows());
    // Run COUNT query in Hive
    List<Object[]> rows = runHiveStatement("SELECT COUNT(*) FROM " + TEST_TABLE_NAME);
    assertEquals(1, rows.size());
    assertEquals(2L, rows.get(0)[0]);
  }

  // ---------------------------------------------------------------------------------------------------

  /** Check that we can read a Hive Map type from BigQuery. */
  @ParameterizedTest
  @MethodSource(READ_FORMAT)
  public void testMapOfInts(String readDataFormat) throws IOException {
    initHive("tez", readDataFormat);
    createExternalTable(
        "mapOfInts",
        "id INT, map_col MAP<STRING, INT>",
        "id INT64, map_col ARRAY<STRUCT<key STRING, value INT64>>");
    // Insert data into the BQ table using the BQ SDK
    runBqQuery(
        String.join(
            "\n",
            "INSERT `${dataset}.mapOfInts` VALUES ",
            "(1, [STRUCT('str11', 11)]),",
            "(2, [STRUCT('txt1', 200), STRUCT('txt2', 400)]),",
            "(3, [STRUCT('new12', 44), STRUCT('new14', 99), STRUCT('new16', 55)])"));
    // Read data using Hive
    List<Object[]> rows = runHiveStatement("SELECT * FROM mapOfInts ORDER BY id");
    ObjectMapper mapper = new ObjectMapper();
    TypeReference<HashMap<String, Integer>> mapOfIntsTypeRef =
        new TypeReference<HashMap<String, Integer>>() {};
    HashMap<String, Integer>[] expected =
        new HashMap[] {
          new HashMap<String, Integer>() {
            {
              put("str11", 11);
            }
          },
          new HashMap<String, Integer>() {
            {
              put("txt1", 200);
              put("txt2", 400);
            }
          },
          new HashMap<String, Integer>() {
            {
              put("new12", 44);
              put("new14", 99);
              put("new16", 55);
            }
          }
        };
    assertEquals(expected.length, rows.size());
    for (int i = 0; i < expected.length; i++) {
      HashMap<String, Integer> map = mapper.readValue(rows.get(i)[1].toString(), mapOfIntsTypeRef);
      assertEquals(expected[i], map);
    }
  }

  // ---------------------------------------------------------------------------------------------------

  /** Check that we can read a Hive Map of structs from BigQuery. */
  @ParameterizedTest
  @MethodSource(READ_FORMAT)
  public void testMapOfStructs(String readDataFormat) throws IOException {
    initHive("tez", readDataFormat);
    createExternalTable(
        "mapOfStructs",
        "id INT, map_col MAP<STRING, STRUCT<COLOR: STRING>>",
        "id INT64, map_col ARRAY<STRUCT<key STRING, value STRUCT<color STRING>>>");
    // Insert data into the BQ table using the BQ SDK
    runBqQuery(
        String.join(
            "\n",
            "INSERT `${dataset}.mapOfStructs` VALUES ",
            "(1, [STRUCT('hi', STRUCT('green')), STRUCT('hello', STRUCT('pink'))])"));
    // Read data using Hive
    List<Object[]> rows = runHiveStatement("SELECT * FROM mapOfStructs ORDER BY id");
    ObjectMapper mapper = new ObjectMapper();
    TypeReference<HashMap<String, HashMap<String, String>>> mapOfStructsTypeRef =
        new TypeReference<HashMap<String, HashMap<String, String>>>() {};
    HashMap<String, Integer>[] expected =
        new HashMap[] {
          new HashMap<String, HashMap<String, String>>() {
            {
              put(
                  "hi",
                  new HashMap<String, String>() {
                    {
                      put("color", "green");
                    }
                  });
              put(
                  "hello",
                  new HashMap<String, String>() {
                    {
                      put("color", "pink");
                    }
                  });
            }
          }
        };
    assertEquals(expected.length, rows.size());
    for (int i = 0; i < expected.length; i++) {
      HashMap<String, Integer> map =
          mapper.readValue(rows.get(i)[1].toString(), mapOfStructsTypeRef);
      assertEquals(expected[i], map);
    }
  }

  // ---------------------------------------------------------------------------------------------------

  /** Check that we can read a Hive Map of arrays from BigQuery. */
  @ParameterizedTest
  @MethodSource(READ_FORMAT)
  public void testMapOfArrays(String readDataFormat) throws IOException {
    initHive("tez", readDataFormat);
    createExternalTable(
        "mapOfArrays",
        "id INT, map_col MAP<STRING, ARRAY<INT>>",
        "id INT64, map_col ARRAY<STRUCT<key STRING, value ARRAY<INT64>>>");
    // Insert data into the BQ table using the BQ SDK
    runBqQuery(
        String.join(
            "\n",
            "INSERT `${dataset}.mapOfArrays` VALUES ",
            "(1, [STRUCT('hi', [1, 2, 3]), STRUCT('hello', [98, 99])])"));
    // Read data using Hive
    List<Object[]> rows = runHiveStatement("SELECT * FROM mapOfArrays ORDER BY id");
    ObjectMapper mapper = new ObjectMapper();
    TypeReference<HashMap<String, List<Integer>>> mapOfArraysTypeRef =
        new TypeReference<HashMap<String, List<Integer>>>() {};
    HashMap<String, Integer>[] expected =
        new HashMap[] {
          new HashMap<String, List<Integer>>() {
            {
              put("hi", Arrays.asList(1, 2, 3));
              put("hello", Arrays.asList(98, 99));
            }
          }
        };
    assertEquals(expected.length, rows.size());
    for (int i = 0; i < expected.length; i++) {
      HashMap<String, List<Integer>> map =
          mapper.readValue(rows.get(i)[1].toString(), mapOfArraysTypeRef);
      assertEquals(expected[i], map);
    }
  }

  // ---------------------------------------------------------------------------------------------------

  /**
   * Check that we can read both the BigQuery TIMESTAMP and DATETIME types from a Hive TIMESTAMP.
   */
  public void readTimestampAndDatetime(String readDataFormat) throws IOException {
    initHive("tez", readDataFormat);
    // Create the tables
    createExternalTable(
        TIMESTAMP_TABLE_NAME, HIVE_TIMESTAMP_TABLE_DDL, BIGQUERY_TIMESTAMP_TABLE_DDL);
    // Insert data using the BQ SDK
    String bqTimestampValue = "2022-12-27T10:00:00.123456";
    String UTC = "Z";
    runBqQuery(
        String.join(
            "\n",
            "INSERT `${dataset}." + TIMESTAMP_TABLE_NAME + "` VALUES (",
            "cast('" + bqTimestampValue + UTC + "' as TIMESTAMP),",
            "cast('" + bqTimestampValue + "' as DATETIME)",
            ")"));
    // Read the data using Hive
    List<Object[]> hiveRows = runHiveStatement("SELECT * FROM " + TIMESTAMP_TABLE_NAME);
    assertEquals(1, hiveRows.size());
    Object[] hiveRow = hiveRows.get(0);
    assertEquals(2, hiveRow.length);
    assertEquals("2022-12-27 15:30:00.123456", hiveRow[0]); // Assuming: (Asia/Kolkata, +5:30)
    assertEquals("2022-12-27 10:00:00.123456", hiveRow[1]); // Unchanged
    // Read the data using the BQ SDK
    TableResult result =
        runBqQuery(String.format("SELECT * FROM `${dataset}.%s`", TIMESTAMP_TABLE_NAME));
    assertEquals(1, result.getTotalRows());
    List<FieldValueList> bqRows = Streams.stream(result.iterateAll()).collect(Collectors.toList());
    FieldValueList bqRow = bqRows.get(0);
    assertEquals(2, bqRow.size());
    assertEquals(
        "2022-12-27T10:00:00.123456Z", bqRow.get(0).getTimestampInstant().toString()); // 'Z' == UTC
    assertEquals("2022-12-27T10:00:00.123456", bqRow.get(1).getStringValue()); // Unchanged
  }

  @ParameterizedTest
  @MethodSource(READ_FORMAT)
  public void testReadTimestampAndDatetimeWithCorrectSetting(String readDataFormat)
      throws IOException {
    hive.setHiveConfValue(HiveBigQueryConfig.HIVE_TIMESTAMP_TIMEZONE, "Asia/Kolkata");
    readTimestampAndDatetime(readDataFormat);
  }

  @ParameterizedTest
  @MethodSource(READ_FORMAT)
  public void testReadTimestampAndDatetimeWithoutCorrectSetting(String readDataFormat)
      throws IOException {
    Throwable exception =
        assertThrows(RuntimeException.class, () -> readTimestampAndDatetime(readDataFormat));
    assertTrue(exception.getMessage().contains(DateTimeUtils.TIMEZONED_TIMESTAMP_ERROR));
  }

  // ---------------------------------------------------------------------------------------------------

  /** Check that we can read all types of data from BigQuery. */
  @ParameterizedTest
  @MethodSource(READ_FORMAT)
  public void testReadAllTypes(String readDataFormat) throws IOException {
    initHive("tez", readDataFormat);
    createExternalTable(
        ALL_TYPES_TABLE_NAME, HIVE_ALL_TYPES_TABLE_DDL, BIGQUERY_ALL_TYPES_TABLE_DDL);
    // Insert data into the BQ table using the BQ SDK
    runBqQuery(
        String.join(
            "\n",
            String.format("INSERT `${dataset}.%s` VALUES (", ALL_TYPES_TABLE_NAME),
            "11,",
            "22,",
            "33,",
            "44,",
            "true,",
            "\"fixed char\",",
            "\"var char\",",
            "\"string\",",
            "cast(\"2019-03-18\" as date),",
            // Wall clock (no timezone)
            "cast(\"2000-01-01T00:23:45.123456\" as datetime),",
            // (Pacific/Honolulu, -10:00)
            "cast(\"2000-01-01T00:23:45.123456-10\" as timestamp),",
            "cast(\"bytes\" as bytes),",
            "2.0,",
            "4.2,",
            "struct(",
            "  cast(\"-99999999999999999999999999999.999999999\" as numeric),",
            "  cast(\"99999999999999999999999999999.999999999\" as numeric),",
            "  cast(3.14 as numeric),",
            "  cast(\"31415926535897932384626433832.795028841\" as numeric)",
            "),",
            "[1, 2, 3],",
            "[(select as struct 111), (select as struct 222), (select as struct 333)],",
            "struct(4.2, cast(\"2019-03-18 11:23:45.678901\" as datetime)),",
            "[struct('a_key', [struct('a_subkey', 888)]), struct('b_key', [struct('b_subkey',"
                + " 999)])]",
            ")"));
    // Read the data using Hive
    List<Object[]> rows = runHiveStatement("SELECT * FROM " + ALL_TYPES_TABLE_NAME);
    assertEquals(1, rows.size());
    Object[] row = rows.get(0);
    assertEquals(19, row.length); // Number of columns
    assertEquals((byte) 11, row[0]);
    assertEquals((short) 22, row[1]);
    assertEquals((int) 33, row[2]);
    assertEquals((long) 44, row[3]);
    assertEquals(true, row[4]);
    assertEquals("fixed char", row[5]);
    assertEquals("var char", row[6]);
    assertEquals("string", row[7]);
    assertEquals("2019-03-18", row[8]);
    assertEquals("2000-01-01 00:23:45.123456", row[9]);
    assertEquals(
        "2000-01-01T10:23:45.123456Z", // 'Z' == UTC
        Instant.from(
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS VV")
                    .parse(row[10].toString()))
            .toString());
    assertArrayEquals("bytes".getBytes(), (byte[]) row[11]);
    assertEquals(2.0, row[12]);
    assertEquals(4.2, row[13]);
    assertEquals(
        "{\"min\":-99999999999999999999999999999.999999999,\"max\":99999999999999999999999999999.999999999,\"pi\":3.14,\"big_pi\":31415926535897932384626433832.795028841}",
        row[14]);
    assertEquals("[1,2,3]", row[15]);
    assertEquals("[{\"i\":111},{\"i\":222},{\"i\":333}]", row[16]);
    assertEquals("{\"float_field\":4.2,\"ts_field\":\"2019-03-18 11:23:45.678901\"}", row[17]);
    // Map type
    ObjectMapper mapper = new ObjectMapper();
    TypeReference<HashMap<String, HashMap<String, Integer>>> typeRef =
        new TypeReference<HashMap<String, HashMap<String, Integer>>>() {};
    HashMap<String, HashMap<String, Integer>> map = mapper.readValue(row[18].toString(), typeRef);
    assertEquals(2, map.size());
    assertEquals(
        new HashMap() {
          {
            put("a_subkey", 888);
          }
        },
        map.get("a_key"));
    assertEquals(
        new HashMap() {
          {
            put("b_subkey", 999);
          }
        },
        map.get("b_key"));
  }

  // ---------------------------------------------------------------------------------------------------

  /**
   * Runs a series of smoke tests to make sure that Hive UDFs used in WHERE clauses are properly
   * translated to BigQuery's flavor of SQL. To check the actual translations, see unit tests in the
   * "input.udfs" package.
   */
  @Test
  public void testUDFWhereClauseSmoke() {
    initHive("tez", HiveBigQueryConfig.ARROW);
    createExternalTable(
        ALL_TYPES_TABLE_NAME, HIVE_ALL_TYPES_TABLE_DDL, BIGQUERY_ALL_TYPES_TABLE_DDL);
    String[] queries = {
      "select * from "
          + ALL_TYPES_TABLE_NAME
          + " where date(day + interval(5) DAY) > date('2001-09-05')",
      "select * from " + ALL_TYPES_TABLE_NAME + " where datediff('2022-09-07', day) > 0",
      "select * from " + ALL_TYPES_TABLE_NAME + " where date_sub(day, 2) > date('2001-01-01')",
      "select * from " + ALL_TYPES_TABLE_NAME + " where date_add(day, 2) > date('2001-01-01')",
      "select * from " + ALL_TYPES_TABLE_NAME + " where str RLIKE '^([0-9]|[a-z]|[A-Z])'",
      "select * from " + ALL_TYPES_TABLE_NAME + " where ((big_int_val / 2.0) = 1.0)",
      "select * from " + ALL_TYPES_TABLE_NAME + " where ((big_int_val % 2) = 1)"
    };
    for (String query : queries) {
      runHiveStatement(query);
    }
  }

  // ---------------------------------------------------------------------------------------------------

  /** Test the "RLIKE" expression */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_READ_FORMAT)
  public void testSelectWithWhereRlikeFilter(String engine, String readDataFormat) {
    initHive(engine, readDataFormat);
    createExternalTable(TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL, BIGQUERY_TEST_TABLE_DDL);
    // Create some initial data in BQ
    runBqQuery(
        String.format(
            "INSERT `${dataset}.%s` VALUES (123, 'email@gmail.com'), (999, 'notanemail')",
            TEST_TABLE_NAME));
    TableResult result =
        runBqQuery(String.format("SELECT * FROM `${dataset}.%s`", TEST_TABLE_NAME));
    // Make sure the initial data is there
    assertEquals(2, result.getTotalRows());
    // Run RLIKE query in Hive
    List<Object[]> rows =
        runHiveStatement(
            "SELECT * FROM "
                + TEST_TABLE_NAME
                + " where text RLIKE  '^[a-zA-Z0-9+_.-]+@[a-zA-Z0-9.-]+$'");
    assertEquals(1, rows.size());
    assertEquals("email@gmail.com", rows.get(0)[1]);
  }

  // ---------------------------------------------------------------------------------------------------

  /** Join two tables */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_READ_FORMAT)
  public void testInnerJoin(String engine, String readDataFormat) {
    initHive(engine, readDataFormat);
    createExternalTable(TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL, BIGQUERY_TEST_TABLE_DDL);
    createExternalTable(
        ANOTHER_TEST_TABLE_NAME, HIVE_ANOTHER_TEST_TABLE_DDL, BIGQUERY_ANOTHER_TEST_TABLE_DDL);
    // Insert data into the BQ tables using the BQ SDK
    runBqQuery(
        String.join(
            "\n",
            String.format("INSERT `${dataset}.%s` VALUES", TEST_TABLE_NAME),
            "(1, 'hello'), (2, 'bonjour'), (1, 'hola')"));
    runBqQuery(
        String.join(
            "\n",
            String.format("INSERT `${dataset}.%s` VALUES", ANOTHER_TEST_TABLE_NAME),
            "(1, 'red'), (2, 'blue'), (3, 'green')"));
    // Do an inner join of the two tables using Hive
    List<Object[]> rows =
        runHiveStatement(
            String.join(
                "\n",
                "SELECT",
                "t2.number,",
                "t1.str_val,",
                "t2.text",
                "FROM " + ANOTHER_TEST_TABLE_NAME + " t1",
                "JOIN " + TEST_TABLE_NAME + " t2",
                "ON (t1.num = t2.number)",
                "ORDER BY t2.number, t1.str_val, t2.text"));
    assertArrayEquals(
        new Object[] {
          new Object[] {1L, "red", "hello"},
          new Object[] {1L, "red", "hola"},
          new Object[] {2L, "blue", "bonjour"},
        },
        rows.toArray());
  }

  // ---------------------------------------------------------------------------------------------------

  /** Read from multiple tables in the same query. */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_READ_FORMAT)
  public void testMultiRead(String engine, String readDataFormat) {
    initHive(engine, readDataFormat);
    createExternalTable(TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL, BIGQUERY_TEST_TABLE_DDL);
    createExternalTable(
        ANOTHER_TEST_TABLE_NAME, HIVE_ANOTHER_TEST_TABLE_DDL, BIGQUERY_ANOTHER_TEST_TABLE_DDL);
    // Insert data into the BQ tables using the BQ SDK
    runBqQuery(
        String.join(
            "\n",
            String.format("INSERT `${dataset}.%s` VALUES", TEST_TABLE_NAME),
            "(1, 'hello1'), (2, 'hello2'), (3, 'hello3')"));
    runBqQuery(
        String.join(
            "\n",
            String.format("INSERT `${dataset}.%s` VALUES", ANOTHER_TEST_TABLE_NAME),
            "(123, 'hi123'), (42, 'hi42'), (999, 'hi999')"));
    // Read from multiple table in same Hive query
    List<Object[]> rows =
        runHiveStatement(
            String.join(
                "\n",
                "SELECT",
                "*",
                "FROM (",
                "SELECT",
                "t1.num as number,",
                "t1.str_val as text",
                "FROM " + ANOTHER_TEST_TABLE_NAME + " t1",
                "UNION ALL",
                "SELECT *",
                "FROM " + TEST_TABLE_NAME + " t2",
                ") unioned_table ORDER BY number"));
    assertArrayEquals(
        new Object[] {
          new Object[] {1L, "hello1"},
          new Object[] {2L, "hello2"},
          new Object[] {3L, "hello3"},
          new Object[] {42L, "hi42"},
          new Object[] {123L, "hi123"},
          new Object[] {999L, "hi999"},
        },
        rows.toArray());
  }

  /** Check that we can explicitly cast selected columns */
  @ParameterizedTest
  @MethodSource(READ_FORMAT)
  public void testExplicitCasts(String readDataFormat) throws IOException {
    initHive("tez", readDataFormat);
    createExternalTable(
        ALL_TYPES_TABLE_NAME, HIVE_ALL_TYPES_TABLE_DDL, BIGQUERY_ALL_TYPES_TABLE_DDL);
    // Insert data into the BQ table using the BQ SDK
    runBqQuery(
        String.join(
            "\n",
            String.format("INSERT `${dataset}.%s` VALUES (", ALL_TYPES_TABLE_NAME),
            "2,",
            "2,",
            "2,",
            "2,",
            "true,",
            "\"2\",",
            "\"2\",",
            "\"2\",",
            "NULL,",
            "NULL,",
            "NULL,",
            "NULL,",
            "2.0,",
            "4.2,",
            "struct(",
            "  cast(\"-99999999999999999999999999999.999999999\" as numeric),",
            "  NULL,",
            "  NULL,",
            "  NULL",
            "),",
            "NULL,",
            "NULL,",
            "NULL,",
            "NULL",
            ")"));
    // Read the data using Hive
    Map<String, String[]> castings = new HashMap<>();
    castings.put(
        "tiny_int_val",
        new String[] {
          "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL", "STRING", "VARCHAR(20)"
        });
    castings.put(
        "small_int_val",
        new String[] {"INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL", "STRING", "VARCHAR(20)"});
    castings.put(
        "int_val", new String[] {"BIGINT", "FLOAT", "DOUBLE", "DECIMAL", "STRING", "VARCHAR(20)"});
    castings.put(
        "big_int_val", new String[] {"FLOAT", "DOUBLE", "DECIMAL", "STRING", "VARCHAR(20)"});
    castings.put("fl", new String[] {"DOUBLE", "DECIMAL", "STRING", "VARCHAR(20)"});
    castings.put("dbl", new String[] {"DECIMAL", "STRING", "VARCHAR(20)"});
    castings.put("nums.min", new String[] {"FLOAT", "DOUBLE", "STRING", "VARCHAR(20)"});
    castings.put(
        "fixed_char",
        new String[] {
          "TINYINT",
          "SMALLINT",
          "INT",
          "BIGINT",
          "FLOAT",
          "DOUBLE",
          "DECIMAL",
          "STRING",
          "VARCHAR(20)"
        });
    castings.put(
        "var_char",
        new String[] {
          "TINYINT",
          "SMALLINT",
          "INT",
          "BIGINT",
          "FLOAT",
          "DOUBLE",
          "DECIMAL",
          "STRING",
          "VARCHAR(20)"
        });
    castings.put(
        "str",
        new String[] {
          "TINYINT",
          "SMALLINT",
          "INT",
          "BIGINT",
          "FLOAT",
          "DOUBLE",
          "DECIMAL",
          "STRING",
          "VARCHAR(20)"
        });
    List<String> casts = new ArrayList<>();
    for (Map.Entry<String, String[]> entry : castings.entrySet()) {
      for (String type : entry.getValue()) {
        casts.add(String.format("CAST(%s AS %s)", entry.getKey(), type));
      }
    }
    runHiveScript(
        String.format("SELECT %s FROM %s", String.join(", ", casts), ALL_TYPES_TABLE_NAME));
  }
}
