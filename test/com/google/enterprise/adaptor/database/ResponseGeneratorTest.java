// Copyright 2014 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.enterprise.adaptor.database;

import static com.google.enterprise.adaptor.database.JdbcFixture.DATABASE;
import static com.google.enterprise.adaptor.database.JdbcFixture.Database.H2;
import static com.google.enterprise.adaptor.database.JdbcFixture.Database.MYSQL;
import static com.google.enterprise.adaptor.database.JdbcFixture.Database.ORACLE;
import static com.google.enterprise.adaptor.database.JdbcFixture.Database.SQLSERVER;
import static com.google.enterprise.adaptor.database.JdbcFixture.executeQueryAndNext;
import static com.google.enterprise.adaptor.database.JdbcFixture.executeUpdate;
import static com.google.enterprise.adaptor.database.JdbcFixture.getConnection;
import static com.google.enterprise.adaptor.database.JdbcFixture.is;
import static com.google.enterprise.adaptor.database.JdbcFixture.prepareStatement;
import static com.google.enterprise.adaptor.database.JdbcFixture.setObject;
import static com.google.enterprise.adaptor.database.Logging.captureLogMessages;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.US;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

import com.google.enterprise.adaptor.InvalidConfigurationException;
import com.google.enterprise.adaptor.testing.RecordingResponse;
import com.google.enterprise.adaptor.testing.RecordingResponse.State;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Test cases for {@link ResponseGenerator}. */
public class ResponseGeneratorTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @After
  public void dropAllObjects() throws SQLException {
    JdbcFixture.dropAllObjects();
  }

  /** Writes the string to a temp file as UTF-8 and returns the path. */
  private File writeTempFile(String content) throws IOException {
    File file = tempFolder.newFile("db.rg.test.txt");
    Files.write(file.toPath(), content.getBytes(UTF_8));
    return file;
  }

  @Test
  public void testNullConfig() {
    thrown.expect(NullPointerException.class);
    ResponseGenerator.rowToText(null);
  }

  @Test
  public void testUrlColumn_missingColumnName() {
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("property columnName is required");
    ResponseGenerator.urlColumn(Collections.<String, String>emptyMap());
  }

  @Test
  public void testFilepathColumn_missingColumnName() {
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("property columnName is required");
    ResponseGenerator.filepathColumn(Collections.<String, String>emptyMap());
  }

  @Test
  public void testContentColumn_missingColumnName() {
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("property columnName is required");
    ResponseGenerator.contentColumn(Collections.<String, String>emptyMap());
  }

  @Test
  public void testUrlColumn_emptyColumnName() {
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("property columnName is required");
    ResponseGenerator.urlColumn(
        Collections.<String, String>singletonMap("columnName", ""));
  }

  @Test
  public void tesFilepathColumn_emptyColumnName() {
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("property columnName is required");
    ResponseGenerator.filepathColumn(
        Collections.<String, String>singletonMap("columnName", ""));
  }

  @Test
  public void testContentColumn_emptyColumnName() {
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("property columnName is required");
    ResponseGenerator.contentColumn(
        Collections.<String, String>singletonMap("columnName", ""));
  }

  @Test
  public void testRowToText_array() throws Exception {
    if (is(H2)) {
      executeUpdate("create table data (id int, arraycol array)");
      String sql = "insert into data(id, arraycol) values (1, ?)";
      PreparedStatement ps = prepareStatement(sql);
      ps.setObject(1, new String[] { "hello", "world" });
      assertEquals(1, ps.executeUpdate());
    } else if (is(ORACLE)) {
      executeUpdate(
          "create or replace type vcarray as varray(2) of varchar2(20)");
      executeUpdate("create table data(id int, arraycol vcarray)");
      executeUpdate("insert into data(id, arraycol) "
          + "values (1, vcarray('hello', 'world'))");
    } else {
      assumeTrue("ARRAY type not supported", false);
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RecordingResponse response = new RecordingResponse(baos);
    Map<String, String> cfg = new TreeMap<String, String>();
    ResponseGenerator resgen = ResponseGenerator.rowToText(cfg);

    ResultSet rs = executeQueryAndNext("select * from data");
    List<String> messages = new ArrayList<String>();
    captureLogMessages(ResponseGenerator.class,
        "Column type not supported for text", messages);
    resgen.generateResponse(rs, response);
    String golden = (is(ORACLE) ? "" : "DATA") + "\nID\n1\n";
    assertEquals(golden, baos.toString(UTF_8.name()));
    assertEquals(messages.toString(), 1, messages.size());
  }

  @Test
  public void testRowToText_boolean() throws Exception {
    assumeFalse("BOOLEAN type not supported", is(ORACLE));
    executeUpdate("create table data ("
        + "booleancol " + JdbcFixture.BOOLEAN + ")");
    String sql = "insert into data values (?)";
    PreparedStatement ps = prepareStatement(sql);
    ps.setBoolean(1, true);
    assertEquals(1, ps.executeUpdate());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RecordingResponse response = new RecordingResponse(baos);
    Map<String, String> cfg = new TreeMap<String, String>();
    ResponseGenerator resgen = ResponseGenerator.rowToText(cfg);

    ResultSet rs = executeQueryAndNext("select * from data");
    List<String> messages = new ArrayList<String>();
    captureLogMessages(ResponseGenerator.class,
        "Column type not supported for text", messages);
    resgen.generateResponse(rs, response);
    String tables = is(SQLSERVER) ? "" : "data";
    String golden = tables + "\n"
        + "booleancol\n"
        + "true\n";
    assertEquals(golden, baos.toString(UTF_8.name()).toLowerCase(US));
    assertEquals(messages.toString(), 0, messages.size());
  }

  @Test
  public void testRowToText_multipleTypes() throws Exception {
    executeUpdate("create table data ("
        + "intcol integer, charcol varchar(20), longcharcol longvarchar,"
        + " datecol date, timecol time, timestampcol timestamp(3),"
        + " clobcol clob, blobcol blob)");
    String sql = "insert into data values (1, ?, ?,"
        + "{d '2007-08-09'}, {t '12:34:56'}, {ts '2007-08-09 12:34:56.7'},"
        + "?, ?)";
    PreparedStatement ps = prepareStatement(sql);
    ps.setString(1, "hello, world");
    ps.setString(2, "lovely world");
    ps.setString(3, "it's a big world");
    ps.setBytes(4, "a big, bad world".getBytes(UTF_8));
    assertEquals(1, ps.executeUpdate());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RecordingResponse response = new RecordingResponse(baos);
    Map<String, String> cfg = new TreeMap<String, String>();
    ResponseGenerator resgen = ResponseGenerator.rowToText(cfg);

    ResultSet rs = executeQueryAndNext("select * from data");
    List<String> messages = new ArrayList<String>();
    captureLogMessages(ResponseGenerator.class,
        "Column type not supported for text", messages);
    resgen.generateResponse(rs, response);
    String tables;
    switch (DATABASE) {
      case ORACLE:
        tables = ",,,,,,";
        break;
      case SQLSERVER:
        tables = ",,data,,,,";
        break;
      default:
        tables = "data,data,data,data,data,data,data";
        break;
    }
    String golden = tables + "\n"
        + "intcol,charcol,longcharcol,datecol,timecol,timestampcol,clobcol\n"
        + "1,\"hello, world\",lovely world," + JdbcFixture.d("2007-08-09") + ","
        + JdbcFixture.t("12:34:56")
        + ",2007-08-09 12:34:56.7,it's a big world\n";
    assertEquals(golden, baos.toString(UTF_8.name()).toLowerCase(US));
    assertEquals(messages.toString(), 1, messages.size());
  }

  @Test
  public void testRowToText_national() throws Exception {
    assumeFalse("NATIONAL types not supported", is(MYSQL));
    executeUpdate("create table data ("
        + "intcol int, nvarcharcol nvarchar(20), nclobcol nclob)");
    String sql = "insert into data values (1, ?, ?)";
    PreparedStatement ps = prepareStatement(sql);
    ps.setString(1, "hello, world");
    ps.setString(2, "lovely world");
    assertEquals(1, ps.executeUpdate());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RecordingResponse response = new RecordingResponse(baos);
    Map<String, String> cfg = new TreeMap<String, String>();
    ResponseGenerator resgen = ResponseGenerator.rowToText(cfg);

    ResultSet rs = executeQueryAndNext("select * from data");
    List<String> messages = new ArrayList<String>();
    captureLogMessages(ResponseGenerator.class,
        "Column type not supported for text", messages);
    resgen.generateResponse(rs, response);
    String tables = (is(SQLSERVER) || is(ORACLE)) ? ",," : "data,data,data";
    String golden = tables + "\n"
        + "intcol,nvarcharcol,nclobcol\n"
        + "1,\"hello, world\",lovely world\n";
    assertEquals(golden, baos.toString(UTF_8.name()).toLowerCase(US));
    assertEquals(messages.toString(), 0, messages.size());
  }

  @Test
  public void testRowToText_null() throws Exception {
    executeUpdate(
        "create table data (id integer, this varchar(20), that clob)");
    executeUpdate("insert into data (id) values (1)");

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RecordingResponse response = new RecordingResponse(baos);
    Map<String, String> cfg = new TreeMap<String, String>();
    ResponseGenerator resgen = ResponseGenerator.rowToText(cfg);

    ResultSet rs = executeQueryAndNext("select * from data");
    resgen.generateResponse(rs, response);
    String table = (is(SQLSERVER) || is(ORACLE)) ? ",," : "DATA,DATA,DATA";
    String golden = table + "\nID,THIS,THAT\n1,,\n";
    assertEquals(golden, baos.toString(UTF_8.name()).toUpperCase(US));
  }

  @Test
  public void testRowToText_specialCharacters() throws Exception {
    executeUpdate(
        "create table data (ID integer, NAME varchar(20), QUOTE varchar(200))");
    String sql = "insert into data (id, name, quote) values (1, ?, ?)";
    PreparedStatement ps = prepareStatement(sql);
    ps.setString(1, "Rhett Butler");
    ps.setString(2, "\"Frankly Scarlett, I don't give a damn!\"");
    assertEquals(1, ps.executeUpdate());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RecordingResponse response = new RecordingResponse(baos);
    Map<String, String> cfg = new TreeMap<String, String>();
    ResponseGenerator resgen = ResponseGenerator.rowToText(cfg);

    ResultSet rs = executeQueryAndNext("select * from data");
    resgen.generateResponse(rs, response);
    String golden = "\nID,NAME,QUOTE\n"
        + "1,Rhett Butler,\"\"\"Frankly Scarlett, I don't give a damn!\"\"\"\n";
    assertThat(baos.toString(UTF_8.name()), endsWith(golden));
  }

  /** @see testContentColumn(int, String, Object, String) */
  private void testContentColumn(int sqlType, Object input, String output)
      throws Exception {
    testContentColumn(sqlType,
        DatabaseAdaptor.getColumnTypeName(sqlType, null, 0).toLowerCase(US),
        input, output);
  }

  /**
   * Parameterized test for contentColumn using different SQL data types.
   *
   * @param sqlType the SQL data type
   * @param sqlTypeDecl the SQL data type declaration
   * @param input the SQL value inserted into the database
   * @param output the expected content stream value
   */
  private void testContentColumn(int sqlType, String sqlTypeDecl, Object input,
      String output) throws SQLException, IOException {
    executeUpdate("create table data(id int, content " + sqlTypeDecl + ")");
    String sql = "insert into data(id, content) values (1, ?)";
    PreparedStatement ps = prepareStatement(sql);
    setObject(ps, 1, input, sqlType);
    assertEquals(1, ps.executeUpdate());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RecordingResponse response = new RecordingResponse(baos);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "content");
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);

    ResultSet rs = executeQueryAndNext("select * from data");
    resgen.generateResponse(rs, response);
    assertEquals(output, baos.toString(UTF_8.name()));
  }

  @Test
  public void testContentColumn_varchar() throws Exception {
    String content = "hello world";
    testContentColumn(Types.VARCHAR, "varchar(20)", content, content);
  }

  @Test
  public void testContentColumn_nullVarchar() throws Exception {
    testContentColumn(Types.VARCHAR, "varchar(20)", null, "");
  }

  @Test
  public void testContentColumn_incorrectColumnName() throws Exception {
    String content = "hello world";
    executeUpdate("create table data(id int, content varchar(20))");
    String sql = "insert into data(id, content) values (1, ?)";
    PreparedStatement ps = prepareStatement(sql);
    ps.setString(1, content);
    assertEquals(1, ps.executeUpdate());

    RecordingResponse response = new RecordingResponse();
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "wrongcolumn");
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);

    ResultSet rs = executeQueryAndNext("select * from data");
    thrown.expect(java.sql.SQLException.class);
    resgen.generateResponse(rs, response);
  }

  @Test
  public void testContentColumn_longvarchar() throws Exception {
    String content = "hello world";
    testContentColumn(Types.LONGVARCHAR, content, content);
  }

  @Test
  public void testContentColumn_nullLongvarchar() throws Exception {
    testContentColumn(Types.LONGVARCHAR, null, "");
  }

  @Test
  public void testContentColumn_varbinary() throws Exception {
    String content = "hello world";
    testContentColumn(Types.VARBINARY, "varbinary(20)", content.getBytes(UTF_8),
        content);
  }

  @Test
  public void testContentColumn_nullVarbinary() throws Exception {
    testContentColumn(Types.VARBINARY, "varbinary(20)", null, "");
  }

  @Test
  public void testContentColumn_longvarbinary() throws Exception {
    String content = "hello world";
    testContentColumn(Types.LONGVARBINARY, content.getBytes(UTF_8), content);
  }

  @Test
  public void testContentColumn_nullLongvarbinary() throws Exception {
    testContentColumn(Types.LONGVARBINARY, null, "");
  }

  @Test
  public void testContentColumn_clob() throws Exception {
    String content = "hello world";
    testContentColumn(Types.CLOB, content, content);
  }

  @Test
  public void testContentColumn_nullClob() throws Exception {
    testContentColumn(Types.CLOB, null, "");
  }

  @Test
  public void testContentColumn_nclob() throws Exception {
    assumeFalse("NCLOB type not supported", is(MYSQL));
    String content = "hello world";
    testContentColumn(Types.NCLOB, content, content);
  }

  @Test
  public void testContentColumn_nullNclob() throws Exception {
    assumeFalse("NCLOB type not supported", is(MYSQL));
    testContentColumn(Types.NCLOB, null, "");
  }

  @Test
  public void testContentColumn_blob() throws Exception {
    String content = "hello world";
    testContentColumn(Types.BLOB, content.getBytes(UTF_8), content);
  }

  @Test
  public void testContentColumn_nullBlob() throws Exception {
    testContentColumn(Types.BLOB, null, "");
  }

  @Test
  public void testContentColumn_sqlxml() throws Exception {
    assumeTrue("SQLXML type not supported", is(ORACLE) || is(SQLSERVER));
    String content = "<motd>hello world</motd>\n";
    executeUpdate("create table data(id int, content xml)");
    String sql = "insert into data(id, content) values (1, ?)";
    try (Connection conn = getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      SQLXML sqlxml = conn.createSQLXML();
      sqlxml.setString(content);
      ps.setSQLXML(1, sqlxml);
      assertEquals(1, ps.executeUpdate());
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RecordingResponse response = new RecordingResponse(baos);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "content");
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);

    ResultSet rs = executeQueryAndNext("select * from data");
    resgen.generateResponse(rs, response);
    assertEquals(is(SQLSERVER) ? content.trim() : content,
        baos.toString(UTF_8.name()));
  }

  @Test
  public void testContentColumn_nullSqlxml() throws Exception {
    assumeTrue("SQLXML type not supported", is(ORACLE) || is(SQLSERVER));
    testContentColumn(Types.SQLXML, "xml", null, "");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testBlobColumnInvokesContentColumn() {
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "my-blob-col");
    ResponseGenerator resgen = ResponseGenerator.blobColumn(cfg);
    String expected =
        "com.google.enterprise.adaptor.database."
            + "ResponseGenerator$ContentColumn";
    assertEquals(expected, resgen.getClass().getName());
  }

  @Test
  public void testContentColumn_invalidType() throws Exception {
    executeUpdate("create table data(id int, content timestamp)");
    String sql = "insert into data(id, content) values (1, ?)";
    PreparedStatement ps = prepareStatement(sql);
    ps.setTimestamp(1, new Timestamp(0L));
    assertEquals(1, ps.executeUpdate());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RecordingResponse response = new RecordingResponse(baos);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "content");
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);

    ResultSet rs = executeQueryAndNext("select * from data");
    List<String> messages = new ArrayList<String>();
    captureLogMessages(ResponseGenerator.class,
        "Content column type not supported", messages);
    resgen.generateResponse(rs, response);
    assertEquals("", baos.toString(UTF_8.name()));
    assertEquals(messages.toString(), 1, messages.size());
  }

  @Test
  public void testFilepathColumn() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RecordingResponse response = new RecordingResponse(baos);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "filepath");
    String content = "we live inside a file\nwe do\nyes";
    File testFile = writeTempFile(content);

    executeUpdate("create table data(filepath varchar(200))");
    executeUpdate("insert into data(filepath) values ('"
        + testFile.getAbsolutePath() + "')");

    ResponseGenerator resgen = ResponseGenerator.filepathColumn(cfg);
    ResultSet rs = executeQueryAndNext("select * from data");
    resgen.generateResponse(rs, response);
    assertEquals(content, baos.toString(UTF_8.name()));
  }

  @Test
  public void testFilepathColumnModeIncorrectColumnName() throws Exception {
    RecordingResponse response = new RecordingResponse();
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "wrongcolumn");
    String content = "we live inside a file\nwe do\nyes";
    File testFile = writeTempFile(content);

    executeUpdate("create table data(filepath varchar(200))");
    executeUpdate("insert into data(filepath) values ('"
        + testFile.getAbsolutePath() + "')");

    ResponseGenerator resgen = ResponseGenerator.filepathColumn(cfg);
    ResultSet rs = executeQueryAndNext("select * from data");
    thrown.expect(java.sql.SQLException.class);
    resgen.generateResponse(rs, response);
  }

  @Test
  public void testUrlColumn() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RecordingResponse response = new RecordingResponse(baos);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "url");
    String content = "from a yellow url connection comes monty python";
    File testFile = writeTempFile(content);
    // File.toURI() produces file:/path/to/temp/file, which is invalid.
    URI testUri = testFile.toURI();
    testUri = new URI(testUri.getScheme(), "localhost", testUri.getPath(),
                      null);

    executeUpdate("create table data(url varchar(200))");
    executeUpdate("insert into data(url) values ('" + testUri + "')");

    ResponseGenerator resgen = ResponseGenerator.urlColumn(cfg);
    ResultSet rs = executeQueryAndNext("select * from data");
    resgen.generateResponse(rs, response);
    assertEquals(content, baos.toString(UTF_8.name()));
    assertEquals("text/plain", response.getContentType());
    assertEquals(testUri, response.getDisplayUrl());
  }

  @Test
  public void testUrlColumnModeIncorrectColumnName() throws Exception {
    RecordingResponse response = new RecordingResponse();
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "wrongcolumn");

    executeUpdate("create table data(url varchar(20))");
    executeUpdate("insert into data(url) values ('some URL')");

    ResponseGenerator resgen = ResponseGenerator.urlColumn(cfg);
    ResultSet rs = executeQueryAndNext("select * from data");
    thrown.expect(java.sql.SQLException.class);
    resgen.generateResponse(rs, response);
  }

  @Test
  public void testUrlAndMetadataLister_servesNoContent() throws Exception {
    String content = "from a yellow url connection comes monty python";
    File testFile = writeTempFile(content);
    // File.toURI() produces file:/path/to/temp/file, which is invalid.
    URI testUri = testFile.toURI();
    testUri = new URI(testUri.getScheme(), "localhost", testUri.getPath(),
                      null);

    executeUpdate("create table data(url varchar(200), col1 varchar(20))");
    String sql = "insert into data(url, col1) values (?, ?)";
    PreparedStatement ps = prepareStatement(sql);
    ps.setString(1, testUri.toString());
    ps.setString(2, "some metadata");
    assertEquals(1, ps.executeUpdate());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RecordingResponse response = new RecordingResponse(baos);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("db.metadataColumns", "col1");
    ResponseGenerator resgen = ResponseGenerator.urlAndMetadataLister(cfg);
    ResultSet rs = executeQueryAndNext("select * from data");
    resgen.generateResponse(rs, response);
    assertEquals(State.NOT_FOUND, response.getState());
    assertEquals("", baos.toString(UTF_8.name()));
    assertNull(response.getContentType());
    assertNull(response.getDisplayUrl());
  }

  @Test
  public void testUrlAndMetadataLister_ignoresColumnName() throws Exception {
    executeUpdate("create table data(url varchar(200))");
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "anycolumn");
    List<String> messages = new ArrayList<String>();
    captureLogMessages(ResponseGenerator.class,
        "urlAndMetadataLister mode ignores columnName", messages);
    ResponseGenerator resgen = ResponseGenerator.urlAndMetadataLister(cfg);
    assertEquals(messages.toString(), 1, messages.size());
  }

  @Test
  public void testUrlAndMetadataLister_nullUrl() throws Exception {
    executeUpdate("create table data(url varchar(200))");
    executeUpdate("insert into data(url) values (null)");

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RecordingResponse response = new RecordingResponse(baos);
    Map<String, String> cfg = new TreeMap<String, String>();
    ResponseGenerator resgen = ResponseGenerator.urlAndMetadataLister(cfg);
    ResultSet rs = executeQueryAndNext("select * from data");
    resgen.generateResponse(rs, response);
    assertEquals(State.NOT_FOUND, response.getState());
    assertEquals("", baos.toString(UTF_8.name()));
    assertNull(response.getContentType());
    assertNull(response.getDisplayUrl());
  }

  @Test
  public void testContentColumn_contentTypeOverrideAndContentTypeCol()
      throws Exception {
    RecordingResponse response = new RecordingResponse();
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "my-blob-col");
    cfg.put("contentTypeOverride", "dev/rubish");
    cfg.put("contentTypeCol", "get-ct-here");
    thrown.expect(InvalidConfigurationException.class);
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);
  }

  @Test
  public void testContentColumn_contentTypeOverride() throws Exception {
    String content = "hello world";
    executeUpdate("create table data(id int, content blob)");
    String sql = "insert into data(id, content) values (1, ?)";
    PreparedStatement ps = prepareStatement(sql);
    ps.setBytes(1, content.getBytes(UTF_8));
    assertEquals(1, ps.executeUpdate());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RecordingResponse response = new RecordingResponse(baos);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "content");
    cfg.put("contentTypeOverride", "dev/rubish");
    cfg.put("contentTypeCol", ""); // Empty should be ignored.
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);

    ResultSet rs = executeQueryAndNext("select * from data");
    resgen.generateResponse(rs, response);
    assertEquals(content, baos.toString(UTF_8.name()));
    assertEquals("dev/rubish", response.getContentType());
  }

  @Test
  public void testContentColumn_contentTypeCol() throws Exception {
    String content = "hello world";
    executeUpdate(
        "create table data(id int, content blob, contentType varchar(20))");
    String sql =
        "insert into data(id, content, contentType) values (1, ?, 'text/rtf')";
    PreparedStatement ps = prepareStatement(sql);
    ps.setBytes(1, content.getBytes(UTF_8));
    assertEquals(1, ps.executeUpdate());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RecordingResponse response = new RecordingResponse(baos);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "content");
    cfg.put("contentTypeOverride", ""); // Empty should be ignored.
    cfg.put("contentTypeCol", "contentType");
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);

    ResultSet rs = executeQueryAndNext("select * from data");
    resgen.generateResponse(rs, response);
    assertEquals(content, baos.toString(UTF_8.name()));
    assertEquals("text/rtf", response.getContentType());
  }

  @Test
  public void testContentColumn_contentTypeCol_nullValue() throws Exception {
    String content = "hello world";
    executeUpdate(
        "create table data(id int, content blob, contentType varchar(20))");
    String sql = "insert into data(id, content) values (1, ?)";
    PreparedStatement ps = prepareStatement(sql);
    ps.setBytes(1, content.getBytes(UTF_8));
    assertEquals(1, ps.executeUpdate());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RecordingResponse response = new RecordingResponse(baos);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "content");
    cfg.put("contentTypeOverride", ""); // Empty should be ignored.
    cfg.put("contentTypeCol", "contentType");
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);

    ResultSet rs = executeQueryAndNext("select * from data");
    List<String> messages = new ArrayList<String>();
    captureLogMessages(ResponseGenerator.class,
        "content type at col {0} is null", messages);
    resgen.generateResponse(rs, response);
    assertEquals(content, baos.toString(UTF_8.name()));
    assertNull(response.getContentType());
    assertEquals(messages.toString(), 1, messages.size());
  }

  @Test
  public void testContentColumn_displayUrlCol() throws Exception {
    String content = "hello world";
    String url = "http://host/hard-coded-blob-display-url";
    executeUpdate("create table data(id int, content blob, url varchar(200))");
    String sql = "insert into data(id, content, url) values (1, ?, ?)";
    PreparedStatement ps = prepareStatement(sql);
    ps.setBytes(1, content.getBytes(UTF_8));
    ps.setString(2, url);
    assertEquals(1, ps.executeUpdate());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RecordingResponse response = new RecordingResponse(baos);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "content");
    cfg.put("displayUrlCol", "url");
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);

    ResultSet rs = executeQueryAndNext("select * from data");
    resgen.generateResponse(rs, response);
    assertEquals(content, baos.toString(UTF_8.name()));
    assertEquals(new URI(url), response.getDisplayUrl());
  }

  @Test
  public void testContentColumn_displayUrlCol_nullValue() throws Exception {
    String content = "hello world";
    executeUpdate("create table data(id int, content blob, url varchar(200))");
    String sql = "insert into data(id, content) values (1, ?)";
    PreparedStatement ps = prepareStatement(sql);
    ps.setBytes(1, content.getBytes(UTF_8));
    assertEquals(1, ps.executeUpdate());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RecordingResponse response = new RecordingResponse(baos);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "content");
    cfg.put("displayUrlCol", "url");
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);

    ResultSet rs = executeQueryAndNext("select * from data");
    List<String> messages = new ArrayList<String>();
    captureLogMessages(ResponseGenerator.class,
        "display url at col {0} is null", messages);
    resgen.generateResponse(rs, response);
    assertEquals(content, baos.toString(UTF_8.name()));
    assertNull(response.getDisplayUrl());
    assertEquals(messages.toString(), 1, messages.size());
  }

  @Test
  public void testContentColumn_displayUrlCol_invalidUrl() throws Exception {
    String content = "hello world";
    String url = "invalid-display-url";
    executeUpdate("create table data(id int, content blob, url varchar(200))");
    String sql = "insert into data(id, content, url) values (1, ?, ?)";
    PreparedStatement ps = prepareStatement(sql);
    ps.setBytes(1, content.getBytes(UTF_8));
    ps.setString(2, url);
    assertEquals(1, ps.executeUpdate());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RecordingResponse response = new RecordingResponse(baos);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "content");
    cfg.put("displayUrlCol", "url");
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);

    ResultSet rs = executeQueryAndNext("select * from data");
    List<String> messages = new ArrayList<String>();
    captureLogMessages(ResponseGenerator.class,
         "override display url invalid:", messages);
    resgen.generateResponse(rs, response);
    assertEquals(content, baos.toString(UTF_8.name()));
    assertNull(response.getDisplayUrl());
    assertEquals(messages.toString(), 1, messages.size());
  }

  @Test
  public void testUrlColumn_invalidUrl() throws Exception {
    String url = "invalid-url";
    executeUpdate("create table data(id int, url varchar(200))");
    String sql = "insert into data(id, url) values (1, ?)";
    PreparedStatement ps = prepareStatement(sql);
    ps.setString(1, url);
    assertEquals(1, ps.executeUpdate());

    RecordingResponse response = new RecordingResponse();
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "url");
    ResponseGenerator resgen = ResponseGenerator.urlColumn(cfg);

    ResultSet rs = executeQueryAndNext("select * from data");
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("is not a valid URI");
    resgen.generateResponse(rs, response);
  }

  @Test
  public void testUrlColumn_displayUrlCol() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RecordingResponse response = new RecordingResponse(baos);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "fetchUrl");
    cfg.put("displayUrlCol", "url");
    String content = "from a yellow url connection comes monty python";
    File testFile = writeTempFile(content);
    URL testUrl = testFile.toURI().toURL();

    String url = "http://host/hard-coded-disp-url";
    executeUpdate("create table data(fetchUrl varchar(200), url varchar(200))");
    executeUpdate("insert into data(fetchUrl, url) values ('" + testUrl
        + "', '" + url + "')");

    ResponseGenerator resgen = ResponseGenerator.urlColumn(cfg);
    ResultSet rs = executeQueryAndNext("select * from data");
    resgen.generateResponse(rs, response);
    assertEquals(content, baos.toString(UTF_8.name()));
    assertEquals("text/plain", response.getContentType());
    assertEquals(new URI(url), response.getDisplayUrl());
  }

  @Test
  public void testUrlColumn_contentTypeCol() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RecordingResponse response = new RecordingResponse(baos);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "url");
    cfg.put("contentTypeCol", "contentType");

    String content = "from a yellow url connection comes monty python";
    File testFile = writeTempFile(content);
    URI testUri = testFile.toURI();
    testUri = new URI(testUri.getScheme(), "localhost", testUri.getPath(),
        null);

    executeUpdate(
        "create table data(url varchar(200), contentType varchar(200))");
    executeUpdate("insert into data(url, contentType) values ('" + testUri
        + "', 'text/rtf')");

    ResponseGenerator resgen = ResponseGenerator.urlColumn(cfg);
    ResultSet rs = executeQueryAndNext("select * from data");
    resgen.generateResponse(rs, response);
    assertEquals(content, baos.toString(UTF_8.name()));
    assertEquals("text/rtf", response.getContentType());
  }

  @Test
  public void testRowToHtml() throws Exception {
    executeUpdate("create table data(id int, XYGGY_COL varchar(20))");
    executeUpdate(
        "insert into data(id, xyggy_col) values (1, 'xyggy value')");

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RecordingResponse response = new RecordingResponse(baos);
    ResponseGenerator resgen =
        ResponseGenerator.rowToHtml(Collections.<String, String>emptyMap());

    ResultSet rs = executeQueryAndNext("select * from data");
    resgen.generateResponse(rs, response);
    // Assert that the column of interest appears as the content of some
    // element in the HTML.
    String content = baos.toString(UTF_8.name());
    assertThat(content, containsString(">XYGGY_COL<"));
    assertThat(content, containsString(">xyggy value<"));
  }

  /** Tests column names with spaces. */
  @Test
  public void testRowToHtml_spacesInColumnNames() throws Exception {
    executeUpdate("create table data(id int, \"xyggy col\" varchar(20))");
    executeUpdate(
        "insert into data(id, \"xyggy col\") values (1, 'xyggy value')");

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RecordingResponse response = new RecordingResponse(baos);
    ResponseGenerator resgen =
        ResponseGenerator.rowToHtml(Collections.<String, String>emptyMap());

    ResultSet rs = executeQueryAndNext("select * from data");
    resgen.generateResponse(rs, response);
    // Assert that the column of interest appears as the content of some
    // element in the HTML.
    String content = baos.toString(UTF_8.name());
    assertThat(content, containsString(">xyggy col<"));
    assertThat(content, containsString(">xyggy value<"));
  }

  @Test
  public void testRowToHtml_stylesheet() throws Exception {
    executeUpdate("create table data(id int, XYGGY_COL varchar(20))");
    executeUpdate(
        "insert into data(id, xyggy_col) values (1, 'xyggy value')");

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RecordingResponse response = new RecordingResponse(baos);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("stylesheet",
        ResponseGenerator.class
        .getResource("resources/dbdefault.xsl")
        .getPath());
    ResponseGenerator resgen = ResponseGenerator.rowToHtml(cfg);

    ResultSet rs = executeQueryAndNext("select * from data");
    resgen.generateResponse(rs, response);
    // Assert that the column of interest appears as the content of some
    // element in the HTML.
    String content = baos.toString(UTF_8.name());
    assertThat(content, containsString(">XYGGY_COL<"));
    assertThat(content, containsString(">xyggy value<"));
  }

  @Test
  public void testRowToHtml_stylesheetNotFound() throws Exception {
    RecordingResponse response = new RecordingResponse();
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("stylesheet", "not/a/valid/path.xsl");
    thrown.expect(FileNotFoundException.class);
    ResponseGenerator resgen = ResponseGenerator.rowToHtml(cfg);
  }
}
