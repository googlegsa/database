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

import static com.google.enterprise.adaptor.database.JdbcFixture.executeQueryAndNext;
import static com.google.enterprise.adaptor.database.JdbcFixture.executeUpdate;
import static com.google.enterprise.adaptor.database.JdbcFixture.getConnection;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

import com.google.enterprise.adaptor.InvalidConfigurationException;
import com.google.enterprise.adaptor.Response;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
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
  private String writeTempFile(String content) throws IOException {
    Path file = tempFolder.newFile().toPath();
    Files.write(file, content.getBytes(UTF_8));
    return file.toString();
  }

  @Test
  public void testMissingColumnNameForUrlMode() {
    thrown.expect(NullPointerException.class);
    ResponseGenerator.urlColumn(Collections.<String, String>emptyMap());
  }

  @Test
  public void testMissingColumnNameForUrlAndMetadataListerMode() {
    thrown.expect(NullPointerException.class);
    ResponseGenerator.urlAndMetadataLister(
        Collections.<String, String>emptyMap());
  }

  @Test
  public void testMissingColumnNameForFilepathMode() {
    thrown.expect(NullPointerException.class);
    ResponseGenerator.filepathColumn(Collections.<String, String>emptyMap());
  }

  @Test
  public void testMissingColumnNameForBlobMode() {
    thrown.expect(NullPointerException.class);
    ResponseGenerator.contentColumn(Collections.<String, String>emptyMap());
  }

  private static class MockResponse implements InvocationHandler {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    String contentType = null;
    URI displayUrl = null;

    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {
      String methodName = method.getName();
      if ("getOutputStream".equals(methodName)) {
        return baos;
      } else if ("setContentType".equals(methodName)) {
        contentType = "" + args[0];
        return null;
      } else if ("setDisplayUrl".equals(methodName)) {
        displayUrl = (URI) args[0];
        return null;
      }
      throw new AssertionError("misused response proxy");
    }
  }

  @Test
  public void testBlobColumnModeServesResultBlob() throws Exception {
    String content = "hello world";
    executeUpdate("create table data(id int, content blob)");
    String sql = "insert into data(id, content) values (1, ?)";
    try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
      ps.setBytes(1, content.getBytes(UTF_8));
      assertEquals(1, ps.executeUpdate());
    }

    MockResponse bar = new MockResponse();
    Response response = (Response) Proxy.newProxyInstance(
        Response.class.getClassLoader(),
        new Class[] { Response.class }, bar);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "content");
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);

    ResultSet rs = executeQueryAndNext("select * from data");
    resgen.generateResponse(rs, response);
    String responseMsg = bar.baos.toString(UTF_8.name());
    assertEquals(content, responseMsg);
  }

  @Test
  public void testBlobColumnModeIncorrectColumnName() throws Exception {
    String content = "hello world";
    executeUpdate("create table data(id int, content blob)");
    String sql = "insert into data(id, content) values (1, ?)";
    try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
      ps.setBytes(1, content.getBytes(UTF_8));
      assertEquals(1, ps.executeUpdate());
    }

    MockResponse bar = new MockResponse();
    Response response = (Response) Proxy.newProxyInstance(
        Response.class.getClassLoader(),
        new Class[] { Response.class }, bar);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "wrongcolumn");
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);

    ResultSet rs = executeQueryAndNext("select * from data");
    thrown.expect(java.sql.SQLException.class);
    resgen.generateResponse(rs, response);
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
  public void testClobColumnModeServesResultClob() throws Exception {
    String content = "hello world";
    executeUpdate("create table data(id int, content clob)");
    String sql = "insert into data(id, content) values (1, ?)";
    try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
      ps.setString(1, content);
      assertEquals(1, ps.executeUpdate());
    }

    MockResponse bar = new MockResponse();
    Response response = (Response) Proxy.newProxyInstance(
        Response.class.getClassLoader(),
        new Class[] { Response.class }, bar);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "content");
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);

    ResultSet rs = executeQueryAndNext("select * from data");
    resgen.generateResponse(rs, response);
    String responseMsg = bar.baos.toString(UTF_8.name());
    assertEquals(content, responseMsg);
  }

  @Test
  public void testClobColumnModeIncorrectColumnName() throws Exception {
    String content = "hello world";
    executeUpdate("create table data(id int, content clob)");
    String sql = "insert into data(id, content) values (1, ?)";
    try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
      ps.setString(1, content);
      assertEquals(1, ps.executeUpdate());
    }

    MockResponse bar = new MockResponse();
    Response response = (Response) Proxy.newProxyInstance(
        Response.class.getClassLoader(),
        new Class[] { Response.class }, bar);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "wrongcolumn");
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);

    ResultSet rs = executeQueryAndNext("select * from data");
    thrown.expect(java.sql.SQLException.class);
    resgen.generateResponse(rs, response);
  }

  private static void writeDataToFile(File f, String content)
      throws IOException {
    FileOutputStream fw = null;
    try {
      fw = new FileOutputStream(f);
      fw.write(content.getBytes(UTF_8));
      fw.flush();
    } finally {
      if (null != fw) {
        fw.close();
      }
    }
  }
 
  @Test
  public void testFilepathColumnModeServesResult() throws Exception {
    MockResponse far = new MockResponse();
    Response response = (Response) Proxy.newProxyInstance(
        Response.class.getClassLoader(),
        new Class[] { Response.class }, far);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "filepath");
    File testFile = null;
    try {
      testFile = File.createTempFile("db.rg.test", ".txt");
      String content = "we live inside a file\nwe do\nyes";
      writeDataToFile(testFile, content);

      executeUpdate("create table data(filepath varchar)");
      executeUpdate("insert into data(filepath) values ('"
          + testFile.getAbsolutePath() + "')");

      ResponseGenerator resgen = ResponseGenerator.filepathColumn(cfg);
      ResultSet rs = executeQueryAndNext("select * from data");
      resgen.generateResponse(rs, response);
      String responseMsg = far.baos.toString(UTF_8.name());
      assertEquals(content, responseMsg);
    } finally {
      if (null != testFile) {
        testFile.delete();
      }
    }
  }

  @Test
  public void testFilepathColumnModeIncorrectColumnName() throws Exception {
    MockResponse far = new MockResponse();
    Response response = (Response) Proxy.newProxyInstance(
        Response.class.getClassLoader(),
        new Class[] { Response.class }, far);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "wrongcolumn");
    File testFile = null;
    try {
      testFile = File.createTempFile("db.rg.test", ".txt");
      String content = "we live inside a file\nwe do\nyes";
      writeDataToFile(testFile, content);

      executeUpdate("create table data(filepath varchar)");
      executeUpdate("insert into data(filepath) values ('"
          + testFile.getAbsolutePath() + "')");

      ResponseGenerator resgen = ResponseGenerator.filepathColumn(cfg);
      ResultSet rs = executeQueryAndNext("select * from data");
      thrown.expect(java.sql.SQLException.class);
      resgen.generateResponse(rs, response);
    } finally {
      if (null != testFile) {
        testFile.delete();
      }
    }
  }
 
  @Test
  public void testUrlColumnModeServesResult() throws Exception {
    MockResponse uar = new MockResponse();
    Response response = (Response) Proxy.newProxyInstance(
        Response.class.getClassLoader(),
        new Class[] { Response.class }, uar);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "url");
    File testFile = null;
    try {
      testFile = File.createTempFile("db.rg.test", ".txt");
      String content = "from a yellow url connection comes monty python";
      writeDataToFile(testFile, content);
      // File.toURI() produces file:/path/to/temp/file, which is invalid.
      URI testUri = testFile.toURI();
      testUri = new URI(testUri.getScheme(), "localhost", testUri.getPath(),
                        null);

      executeUpdate("create table data(url varchar)");
      executeUpdate("insert into data(url) values ('" + testUri + "')");

      ResponseGenerator resgen = ResponseGenerator.urlColumn(cfg);
      ResultSet rs = executeQueryAndNext("select * from data");
      resgen.generateResponse(rs, response);
      String responseMsg = uar.baos.toString(UTF_8.name());
      assertEquals(content, responseMsg);
      assertEquals("text/plain", uar.contentType);
      assertEquals(testUri, uar.displayUrl);
    } finally {
      if (null != testFile) {
        testFile.delete();
      }
    }
  }

  @Test
  public void testUrlColumnModeIncorrectColumnName() throws Exception {
    MockResponse far = new MockResponse();
    Response response = (Response) Proxy.newProxyInstance(
        Response.class.getClassLoader(),
        new Class[] { Response.class }, far);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "wrongcolumn");
    File testFile = null;
    try {
      testFile = File.createTempFile("db.rg.test", ".txt");
      String content = "from a yellow url connection comes monty python";
      writeDataToFile(testFile, content);

      executeUpdate("create table data(url varchar)");
      executeUpdate("insert into data(url) values ('some URL')");

      ResponseGenerator resgen = ResponseGenerator.urlColumn(cfg);
      ResultSet rs = executeQueryAndNext("select * from data");
      thrown.expect(java.sql.SQLException.class);
      resgen.generateResponse(rs, response);
    } finally {
      if (null != testFile) {
        testFile.delete();
      }
    }
  }

  @Test
  public void testBlobColumnModeContentTypeOverrideAndContentTypeCol()
      throws Exception {
    MockResponse bar = new MockResponse();
    Response response = (Response) Proxy.newProxyInstance(
        Response.class.getClassLoader(),
        new Class[] { Response.class }, bar);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "my-blob-col");
    cfg.put("contentTypeOverride", "dev/rubish");
    cfg.put("contentTypeCol", "get-ct-here");
    thrown.expect(InvalidConfigurationException.class);
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);
  }

  @Test
  public void testBlobColumnModeContentTypeOverride() throws Exception {
    String content = "hello world";
    executeUpdate("create table data(id int, content blob)");
    String sql = "insert into data(id, content) values (1, ?)";
    try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
      ps.setBytes(1, content.getBytes(UTF_8));
      assertEquals(1, ps.executeUpdate());
    }

    MockResponse bar = new MockResponse();
    Response response = (Response) Proxy.newProxyInstance(
        Response.class.getClassLoader(),
        new Class[] { Response.class }, bar);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "content");
    cfg.put("contentTypeOverride", "dev/rubish");
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);

    ResultSet rs = executeQueryAndNext("select * from data");
    resgen.generateResponse(rs, response);
    String responseMsg = bar.baos.toString(UTF_8.name());
    assertEquals(content, responseMsg);
    assertEquals("dev/rubish", bar.contentType);
  }

  @Test
  public void testBlobColumnModeContentTypeCol() throws Exception {
    String content = "hello world";
    executeUpdate(
        "create table data(id int, content blob, contentType varchar)");
    String sql =
        "insert into data(id, content, contentType) values (1, ?, 'text/rtf')";
    try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
      ps.setBytes(1, content.getBytes(UTF_8));
      assertEquals(1, ps.executeUpdate());
    }

    MockResponse bar = new MockResponse();
    Response response = (Response) Proxy.newProxyInstance(
        Response.class.getClassLoader(),
        new Class[] { Response.class }, bar);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "content");
    cfg.put("contentTypeCol", "contentType");
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);

    ResultSet rs = executeQueryAndNext("select * from data");
    resgen.generateResponse(rs, response);
    String responseMsg = bar.baos.toString(UTF_8.name());
    assertEquals(content, responseMsg);
    assertEquals("text/rtf", bar.contentType);
  }

  @Test
  public void testBlobColumnModeDisplayUrlCol() throws Exception {
    String content = "hello world";
    String url = "http://host/hard-coded-blob-display-url";
    executeUpdate(
        "create table data(id int, content blob, url varchar)");
    String sql = "insert into data(id, content, url) values (1, ?, ?)";
    try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
      ps.setBytes(1, content.getBytes(UTF_8));
      ps.setString(2, url);
      assertEquals(1, ps.executeUpdate());
    }

    MockResponse bar = new MockResponse();
    Response response = (Response) Proxy.newProxyInstance(
        Response.class.getClassLoader(),
        new Class[] { Response.class }, bar);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "content");
    cfg.put("displayUrlCol", "url");
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);

    ResultSet rs = executeQueryAndNext("select * from data");
    resgen.generateResponse(rs, response);
    String responseMsg = bar.baos.toString(UTF_8.name());
    assertEquals(content, responseMsg);
    assertEquals(new URI(url), bar.displayUrl);
  }

  @Test
  public void testClobColumnModeContentTypeOverrideAndContentTypeCol()
      throws Exception {
    MockResponse bar = new MockResponse();
    Response response = (Response) Proxy.newProxyInstance(
        Response.class.getClassLoader(),
        new Class[] { Response.class }, bar);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "my-clob-col");
    cfg.put("contentTypeOverride", "dev/rubish");
    cfg.put("contentTypeCol", "get-ct-here");
    thrown.expect(InvalidConfigurationException.class);
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);
  }

  @Test
  public void testClobColumnModeContentTypeOverride() throws Exception {
    String content = "hello world";
    executeUpdate("create table data(id int, content clob)");
    String sql = "insert into data(id, content) values (1, ?)";
    try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
      ps.setString(1, content);
      assertEquals(1, ps.executeUpdate());
    }

    MockResponse bar = new MockResponse();
    Response response = (Response) Proxy.newProxyInstance(
        Response.class.getClassLoader(),
        new Class[] { Response.class }, bar);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "content");
    cfg.put("contentTypeOverride", "dev/rubish");
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);

    ResultSet rs = executeQueryAndNext("select * from data");
    resgen.generateResponse(rs, response);
    String responseMsg = bar.baos.toString(UTF_8.name());
    assertEquals(content, responseMsg);
    assertEquals("dev/rubish", bar.contentType);
  }

  @Test
  public void testClobColumnModeContentTypeCol() throws Exception {
    String content = "hello world";
    executeUpdate(
        "create table data(id int, content clob, contentType varchar)");
    String sql =
        "insert into data(id, content, contentType) values (1, ?, 'text/rtf')";
    try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
      ps.setString(1, content);
      assertEquals(1, ps.executeUpdate());
    }

    MockResponse bar = new MockResponse();
    Response response = (Response) Proxy.newProxyInstance(
        Response.class.getClassLoader(),
        new Class[] { Response.class }, bar);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "content");
    cfg.put("contentTypeCol", "contentType");
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);

    ResultSet rs = executeQueryAndNext("select * from data");
    resgen.generateResponse(rs, response);
    String responseMsg = bar.baos.toString(UTF_8.name());
    assertEquals(content, responseMsg);
    assertEquals("text/rtf", bar.contentType);
  }

  @Test
  public void testClobColumnModeDisplayUrlCol() throws Exception {
    String content = "hello world";
    String url = "http://host/hard-coded-clob-display-url";
    executeUpdate("create table data(id int, content clob, url varchar)");
    String sql = "insert into data(id, content, url) values (1, ?, ?)";
    try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
      ps.setString(1, content);
      ps.setString(2, url);
      assertEquals(1, ps.executeUpdate());
    }

    MockResponse bar = new MockResponse();
    Response response = (Response) Proxy.newProxyInstance(
        Response.class.getClassLoader(),
        new Class[] { Response.class }, bar);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "content");
    cfg.put("displayUrlCol", "url");
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);

    ResultSet rs = executeQueryAndNext("select * from data");
    resgen.generateResponse(rs, response);
    String responseMsg = bar.baos.toString(UTF_8.name());
    assertEquals(content, responseMsg);
    assertEquals(new URI(url), bar.displayUrl);
  }

  @Test
  public void testUrlColumnModeDisplayUrlCol() throws Exception {
    MockResponse uar = new MockResponse();
    Response response = (Response) Proxy.newProxyInstance(
        Response.class.getClassLoader(),
        new Class[] { Response.class }, uar);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "fetchUrl");
    cfg.put("displayUrlCol", "url");
    File testFile = null;
    try {
      testFile = File.createTempFile("db.rg.test", ".txt");
      String content = "from a yellow url connection comes monty python";
      writeDataToFile(testFile, content);
      URL testUrl = testFile.toURI().toURL();

      String url = "http://host/hard-coded-disp-url";
      executeUpdate("create table data(fetchUrl varchar, url varchar)");
      executeUpdate("insert into data(fetchUrl, url) values ('" + testUrl
          + "', '" + url + "')");

      ResponseGenerator resgen = ResponseGenerator.urlColumn(cfg);
      ResultSet rs = executeQueryAndNext("select * from data");
      resgen.generateResponse(rs, response);
      String responseMsg = uar.baos.toString(UTF_8.name());
      assertEquals(content, responseMsg);
      assertEquals("text/plain", uar.contentType);
      assertEquals(new URI(url), uar.displayUrl);
    } finally {
      if (null != testFile) {
        testFile.delete();
      }
    }
  }
}
