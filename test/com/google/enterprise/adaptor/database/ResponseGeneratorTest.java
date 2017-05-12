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

import static java.util.Arrays.asList;

import com.google.enterprise.adaptor.InvalidConfigurationException;
import com.google.enterprise.adaptor.Response;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.sql.rowset.serial.SerialClob;

/** Test cases for {@link ResponseGenerator}. */
public class ResponseGeneratorTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

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

  private ResultSet makeMockBlobResultSet(byte b[]) {
    return makeMockBlobResultSet(b, new ArrayList<String>(), new ArrayList<Integer>());
  }

  /* Proxy based mock of ResultSet, because it has lots of methods to mock. */
  private ResultSet makeMockBlobResultSet(byte b[], List<String> names,
      List<Integer> types) {
    ResultSet rs = (ResultSet) Proxy.newProxyInstance(
        ResultSet.class.getClassLoader(), new Class[] { ResultSet.class },
        new InvocationHandler() {
          public Object invoke(Object proxy, Method method, Object[] args)
              throws Throwable {
            if ("getBlob".equals(method.getName())) {
              if ("my-blob-col".equals(args[0])) {
                return new javax.sql.rowset.serial.SerialBlob(b);
              } else {
                throw new java.sql.SQLException("no column named: " + args[0]);
              }
            }
            if ("getString".equals(method.getName())) {
              if ("this-blob-col-has-CT".equals(args[0])) {
                return "hard-coded-blob-content-type";
              } else if ("this-blob-col-has-disp-url".equals(args[0])) {
                return "hard-coded-blob-display-url";
              } else {
                throw new java.sql.SQLException("no column named: " + args[0]);
              }
            }
            if ("getMetaData".equals(method.getName())) {
              return makeMockResultSetMetaData(names, types);
            }
            if ("findColumn".equals(method.getName())) {
              for (int i = 0; i < names.size(); i++) {
                if (args[0].equals(names.get(i))) {
                  return i + 1;
                }
              }
            }
            throw new AssertionError("invalid method: " + method.getName());
          }
        }
    );
    return rs;
  }

  private ResultSet makeMockClobResultSet(String s) {
    return makeMockClobResultSet(s, new ArrayList<String>(), new ArrayList<Integer>());
  }

  /* Proxy based mock of ResultSet, because it has lots of methods to mock. */
  private ResultSet makeMockClobResultSet(String s, List<String> names,
      List<Integer> types) {
    ResultSet rs = (ResultSet) Proxy.newProxyInstance(
        ResultSet.class.getClassLoader(), new Class[] { ResultSet.class },
        new InvocationHandler() {
          public Object invoke(Object proxy, Method method, Object[] args)
              throws Throwable {
            if ("getClob".equals(method.getName())) {
              if ("my-clob-col".equals(args[0])) {
                return new SerialClob(s.toCharArray());
              } else {
                throw new java.sql.SQLException("no column named: " + args[0]);
              }
            }
            if ("getString".equals(method.getName())) {
              if ("this-clob-col-has-CT".equals(args[0])) {
                return "hard-coded-clob-content-type";
              } else if ("this-clob-col-has-disp-url".equals(args[0])) {
                return "hard-coded-clob-display-url";
              } else {
                throw new java.sql.SQLException("no column named: " + args[0]);
              }
            }
            if ("getMetaData".equals(method.getName())) {
              return makeMockResultSetMetaData(names, types);
            }
            if ("findColumn".equals(method.getName())) {
              for (int i = 0; i < names.size(); i++) {
                if (args[0].equals(names.get(i))) {
                  return i + 1;
                }
              }
            }
            throw new AssertionError("invalid method: " + method.getName());
          }
        }
    );
    return rs;
  }

  private ResultSetMetaData makeMockResultSetMetaData(List name, List type) {
    ResultSetMetaData rs = (ResultSetMetaData) Proxy.newProxyInstance(
        ResultSetMetaData.class.getClassLoader(), new Class[] { ResultSetMetaData.class },
        new InvocationHandler() {
          public Object invoke(Object proxy, Method method, Object[] args)
              throws Throwable {
            if ("getColumnCount".equals(method.getName())) {
              return name.size();
            }
            if ("getColumnLabel".equals(method.getName())) {
              return name.get((int) args[0] - 1);
            }
            if ("getColumnType".equals(method.getName())) {
              return type.get((int) args[0] - 1);
            }
            throw new AssertionError("invalid method: " + method.getName());
          }
        }
    );
    return rs;
  }

  /* Proxy based mock of ResultSet, because it has lots of methods to mock. */
  private ResultSet makeMockFilepathResultSet(final File f) {
    ResultSet rs = (ResultSet) Proxy.newProxyInstance(
        ResultSet.class.getClassLoader(), new Class[] { ResultSet.class },
        new InvocationHandler() {
          public Object invoke(Object proxy, Method method, Object[] args)
              throws Throwable {
            Assert.assertEquals("getString", method.getName());
            if ("my-filepath-col".equals(args[0])) {
              return f.getAbsolutePath();
            } else {
              throw new java.sql.SQLException("no column named: " + args[0]);
            }
          }
        }
    );
    return rs;
  }

  /* Proxy based mock of ResultSet, because it has lots of methods to mock. */
  private ResultSet makeMockUrlResultSet(final URL url) {
    ResultSet rs = (ResultSet) Proxy.newProxyInstance(
        ResultSet.class.getClassLoader(), new Class[] { ResultSet.class },
        new InvocationHandler() {
          public Object invoke(Object proxy, Method method, Object[] args)
              throws Throwable {
            Assert.assertEquals("getString", method.getName());
            if ("my-url-is-in-col".equals(args[0])) {
              return url.toString();
            } else if ("my-disp-url-is-in-col-2".equals(args[0])) {
              return "hard-coded-disp-url";
            } else {
              throw new java.sql.SQLException("no column named: " + args[0]);
            }
          }
        }
    );
    return rs;
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
    MockResponse bar = new MockResponse();
    Response response = (Response) Proxy.newProxyInstance(
        Response.class.getClassLoader(),
        new Class[] { Response.class }, bar);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "my-blob-col");
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);
    String content = "hello world";
    byte b[] = content.getBytes("US-ASCII");
    resgen.generateResponse(
        makeMockBlobResultSet(b, asList("my-blob-col"), asList(Types.BLOB)),
        response);
    String responseMsg = new String(bar.baos.toByteArray(), "US-ASCII");
    Assert.assertEquals(content, responseMsg);
  }

  @Test
  public void testBlobColumnModeIncorrectColumnName() throws Exception {
    MockResponse bar = new MockResponse();
    Response response = (Response) Proxy.newProxyInstance(
        Response.class.getClassLoader(),
        new Class[] { Response.class }, bar);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "my-col-name-is-wrong");
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);
    String content = "hello world";
    byte b[] = content.getBytes("US-ASCII");
    thrown.expect(java.sql.SQLException.class);
    resgen.generateResponse(
        makeMockBlobResultSet(b, asList("my-col-name-is-wrong"),
            asList(Types.BLOB)), response);
  }

  @Test
  public void testClobColumnModeServesResultClob() throws Exception {
    MockResponse bar = new MockResponse();
    Response response = (Response) Proxy.newProxyInstance(
        Response.class.getClassLoader(),
        new Class[] { Response.class }, bar);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "my-clob-col");
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);
    String content = "hello world";
    resgen.generateResponse(
        makeMockClobResultSet(content, asList("my-clob-col"),
            asList(Types.CLOB)), response);
    String responseMsg = new String(bar.baos.toByteArray(), "US-ASCII");
    Assert.assertEquals(content, responseMsg);
  }

  @Test
  public void testClobColumnModeIncorrectColumnName() throws Exception {
    MockResponse bar = new MockResponse();
    Response response = (Response) Proxy.newProxyInstance(
        Response.class.getClassLoader(),
        new Class[] { Response.class }, bar);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "my-col-name-is-wrong");
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);
    String content = "hello world";
    thrown.expect(java.sql.SQLException.class);
    resgen.generateResponse(
        makeMockClobResultSet(content, asList("my-col-name-is-wrong"),
            asList(Types.CLOB)), response);
  }

  private static void writeDataToFile(File f, String content)
      throws IOException {
    FileOutputStream fw = null;
    try {
      fw = new FileOutputStream(f);
      fw.write(content.getBytes("UTF-8"));
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
    cfg.put("columnName", "my-filepath-col");
    File testFile = null;
    try {
      testFile = File.createTempFile("db.rg.test", ".txt");
      String content = "we live inside a file\nwe do\nyes";
      writeDataToFile(testFile, content);
      ResponseGenerator resgen = ResponseGenerator.filepathColumn(cfg);
      resgen.generateResponse(makeMockFilepathResultSet(testFile), response);
      String responseMsg = new String(far.baos.toByteArray(), "UTF-8");
      Assert.assertEquals(content, responseMsg);
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
    cfg.put("columnName", "my-filepath-is-not-in-this-col");
    File testFile = null;
    try {
      testFile = File.createTempFile("db.rg.test", ".txt");
      String content = "we live inside a file\nwe do\nyes";
      writeDataToFile(testFile, content);
      ResponseGenerator resgen = ResponseGenerator.filepathColumn(cfg);
      thrown.expect(java.sql.SQLException.class);
      resgen.generateResponse(makeMockFilepathResultSet(testFile), response);
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
    cfg.put("columnName", "my-url-is-in-col");
    File testFile = null;
    try {
      testFile = File.createTempFile("db.rg.test", ".txt");
      String content = "from a yellow url connection comes monty python";
      writeDataToFile(testFile, content);
      ResponseGenerator resgen = ResponseGenerator.urlColumn(cfg);
      URL testUrl = testFile.toURI().toURL();
      ResultSet rs = makeMockUrlResultSet(testUrl);
      resgen.generateResponse(rs, response);
      String responseMsg = new String(uar.baos.toByteArray(), "UTF-8");
      Assert.assertEquals(content, responseMsg);
      Assert.assertEquals("text/plain", uar.contentType);
      Assert.assertEquals(testUrl.toURI(), uar.displayUrl);
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
    cfg.put("columnName", "my-url-went-that-way");
    File testFile = null;
    try {
      testFile = File.createTempFile("db.rg.test", ".txt");
      String content = "from a yellow url connection comes monty python";
      writeDataToFile(testFile, content);
      ResponseGenerator resgen = ResponseGenerator.urlColumn(cfg);
      URL testUrl = testFile.toURI().toURL();
      ResultSet rs = makeMockUrlResultSet(testUrl);
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
    MockResponse bar = new MockResponse();
    Response response = (Response) Proxy.newProxyInstance(
        Response.class.getClassLoader(),
        new Class[] { Response.class }, bar);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "my-blob-col");
    cfg.put("contentTypeOverride", "dev/rubish");
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);
    String content = "hello world";
    byte b[] = content.getBytes("US-ASCII");
    resgen.generateResponse(
        makeMockBlobResultSet(b, asList("my-blob-col"), asList(Types.BLOB)),
        response);
    String responseMsg = new String(bar.baos.toByteArray(), "US-ASCII");
    Assert.assertEquals(content, responseMsg);
    Assert.assertEquals("dev/rubish", bar.contentType);
  }

  @Test
  public void testBlobColumnModeContentTypeCol() throws Exception {
    MockResponse bar = new MockResponse();
    Response response = (Response) Proxy.newProxyInstance(
        Response.class.getClassLoader(),
        new Class[] { Response.class }, bar);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "my-blob-col");
    cfg.put("contentTypeCol", "this-blob-col-has-CT");
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);
    String content = "hello world";
    byte b[] = content.getBytes("US-ASCII");
    resgen.generateResponse(
        makeMockBlobResultSet(b, asList("my-blob-col"), asList(Types.BLOB)),
        response);
    String responseMsg = new String(bar.baos.toByteArray(), "US-ASCII");
    Assert.assertEquals(content, responseMsg);
    Assert.assertEquals("hard-coded-blob-content-type", bar.contentType);
  }

  @Test
  public void testBlobColumnModeDisplayUrlCol() throws Exception {
    MockResponse bar = new MockResponse();
    Response response = (Response) Proxy.newProxyInstance(
        Response.class.getClassLoader(),
        new Class[] { Response.class }, bar);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "my-blob-col");
    cfg.put("displayUrlCol", "this-blob-col-has-disp-url");
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);
    String content = "hello world";
    byte b[] = content.getBytes("US-ASCII");
    resgen.generateResponse(
        makeMockBlobResultSet(b, asList("my-blob-col"), asList(Types.BLOB)),
        response);
    String responseMsg = new String(bar.baos.toByteArray(), "US-ASCII");
    Assert.assertEquals(content, responseMsg);
    Assert.assertEquals(new URI("hard-coded-blob-display-url"), bar.displayUrl);
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
    MockResponse bar = new MockResponse();
    Response response = (Response) Proxy.newProxyInstance(
        Response.class.getClassLoader(),
        new Class[] { Response.class }, bar);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "my-clob-col");
    cfg.put("contentTypeOverride", "dev/rubish");
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);
    String content = "hello world";
    resgen.generateResponse(
        makeMockClobResultSet(content, asList("my-clob-col"),
            asList(Types.CLOB)), response);
    String responseMsg = new String(bar.baos.toByteArray(), "US-ASCII");
    Assert.assertEquals(content, responseMsg);
    Assert.assertEquals("dev/rubish", bar.contentType);
  }

  @Test
  public void testClobColumnModeContentTypeCol() throws Exception {
    MockResponse bar = new MockResponse();
    Response response = (Response) Proxy.newProxyInstance(
        Response.class.getClassLoader(),
        new Class[] { Response.class }, bar);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "my-clob-col");
    cfg.put("contentTypeCol", "this-clob-col-has-CT");
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);
    String content = "hello world";
    resgen.generateResponse(
        makeMockClobResultSet(content, asList("my-clob-col"),
            asList(Types.CLOB)), response);
    String responseMsg = new String(bar.baos.toByteArray(), "US-ASCII");
    Assert.assertEquals(content, responseMsg);
    Assert.assertEquals("hard-coded-clob-content-type", bar.contentType);
  }

  @Test
  public void testClobColumnModeDisplayUrlCol() throws Exception {
    MockResponse bar = new MockResponse();
    Response response = (Response) Proxy.newProxyInstance(
        Response.class.getClassLoader(),
        new Class[] { Response.class }, bar);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "my-clob-col");
    cfg.put("displayUrlCol", "this-clob-col-has-disp-url");
    ResponseGenerator resgen = ResponseGenerator.contentColumn(cfg);
    String content = "hello world";
    resgen.generateResponse(
        makeMockClobResultSet(content, asList("my-clob-col"),
            asList(Types.CLOB)), response);
    String responseMsg = new String(bar.baos.toByteArray(), "US-ASCII");
    Assert.assertEquals(content, responseMsg);
    Assert.assertEquals(new URI("hard-coded-clob-display-url"), bar.displayUrl);
  }

  @Test
  public void testUrlColumnModeDisplayUrlCol() throws Exception {
    MockResponse uar = new MockResponse();
    Response response = (Response) Proxy.newProxyInstance(
        Response.class.getClassLoader(),
        new Class[] { Response.class }, uar);
    Map<String, String> cfg = new TreeMap<String, String>();
    cfg.put("columnName", "my-url-is-in-col");
    cfg.put("displayUrlCol", "my-disp-url-is-in-col-2");
    File testFile = null;
    try {
      testFile = File.createTempFile("db.rg.test", ".txt");
      String content = "from a yellow url connection comes monty python";
      writeDataToFile(testFile, content);
      ResponseGenerator resgen = ResponseGenerator.urlColumn(cfg);
      URL testUrl = testFile.toURI().toURL();
      ResultSet rs = makeMockUrlResultSet(testUrl);
      resgen.generateResponse(rs, response);
      String responseMsg = new String(uar.baos.toByteArray(), "UTF-8");
      Assert.assertEquals(content, responseMsg);
      Assert.assertEquals("text/plain", uar.contentType);
      Assert.assertEquals(new URI("hard-coded-disp-url"), uar.displayUrl);
    } finally {
      if (null != testFile) {
        testFile.delete();
      }
    }
  }
}
