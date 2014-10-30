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

import static org.junit.Assert.*;

import com.google.enterprise.adaptor.InvalidConfigurationException;

import org.junit.*;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/** Test cases for {@link UniqueKey}. */
public class UniqueKeyTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testNullKeys() {
    thrown.expect(NullPointerException.class);
    UniqueKey pk = new UniqueKey(null, "", "");
  }

  @Test
  public void testNullContentCols() {
    thrown.expect(NullPointerException.class);
    UniqueKey pk = new UniqueKey("num:int", null, "");
  }

  @Test
  public void testNullAclCols() {
    thrown.expect(NullPointerException.class);
    UniqueKey pk = new UniqueKey("num:int", "", null);
  }

  @Test
  public void testSingleInt() {
    UniqueKey pk = new UniqueKey("numnum:int");
    assertEquals(1, pk.numElementsForTest());
    assertEquals("numnum", pk.nameForTest(0));
    assertEquals(UniqueKey.ColumnType.INT, pk.typeForTest(0));
  }

  @Test
  public void testEmptyThrows() {
    thrown.expect(InvalidConfigurationException.class);
    UniqueKey pk = new UniqueKey(" ");
  }

  @Test
  public void testTwoInt() {
    UniqueKey pk = new UniqueKey("numnum:int,other:int");
    assertEquals(2, pk.numElementsForTest());
    assertEquals("numnum", pk.nameForTest(0));
    assertEquals("other", pk.nameForTest(1));
    assertEquals(UniqueKey.ColumnType.INT, pk.typeForTest(0));
    assertEquals(UniqueKey.ColumnType.INT, pk.typeForTest(1));
  }

  @Test
  public void testIntString() {
    UniqueKey pk = new UniqueKey("numnum:int,strstr:string");
    assertEquals(2, pk.numElementsForTest());
    assertEquals("numnum", pk.nameForTest(0));
    assertEquals("strstr", pk.nameForTest(1));
    assertEquals(UniqueKey.ColumnType.INT, pk.typeForTest(0));
    assertEquals(UniqueKey.ColumnType.STRING, pk.typeForTest(1));
  }

  @Test
  public void testNameRepeatsNotAllowed() {
    thrown.expect(InvalidConfigurationException.class);
    UniqueKey pk = new UniqueKey("num:int,num:string");
  }

  @Test
  public void testBadDef() {
    thrown.expect(InvalidConfigurationException.class);
    UniqueKey pk = new UniqueKey("numnum:int,strstr/string");
  }

  @Test
  public void testUnknownContentCol() {
    thrown.expect(InvalidConfigurationException.class);
    UniqueKey pk = new UniqueKey("numnum:int,strstr:string",
        "numnum,IsStranger,strstr", "");
  }

  @Test
  public void testUnknownAclCol() {
    thrown.expect(InvalidConfigurationException.class);
    UniqueKey pk = new UniqueKey("numnum:int,strstr:string", "",
        "numnum,IsStranger,strstr");
  }

  /* Proxy based mock of ResultSet, because it has lots of methods to mock. */
  private ResultSet makeMockResultSet(final int n, final String s) {
    ResultSet rs = (ResultSet) Proxy.newProxyInstance(
        ResultSet.class.getClassLoader(), new Class[] { ResultSet.class }, 
        new InvocationHandler() {
          public Object invoke(Object proxy, Method method, Object[] args)
              throws Throwable {
            switch (args[0].toString()) {
              case "numnum": return n;
              case "strstr": return s;
              default: throw new IllegalStateException();
            }
          }
        }
    );
    return rs;
  }

  @Test
  public void testProcessingDocId() throws SQLException {
    UniqueKey pk = new UniqueKey("numnum:int,strstr:string");
    assertEquals("345/abc", pk.makeUniqueId(makeMockResultSet(345, "abc")));
  }

  private static class PreparedStatementHandler implements 
      InvocationHandler {
    int ncalls = 0;
    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {
      ncalls++;
      String methodName = method.getName(); 
      if ("setInt".equals(methodName)) {
        assertEquals(1, args[0]); 
        assertEquals(888, args[1]); 
      } else if ("setString".equals(methodName)) {
        assertEquals(2, args[0]); 
        assertEquals("bluesky", args[1]); 
      } else {
        throw new IllegalStateException("unexpected call: " + methodName);
      }
      return null;
    }
  }

  @Test
  public void testPreparingRetrieval() throws SQLException {
    PreparedStatementHandler psh = new PreparedStatementHandler();
    PreparedStatement ps = (PreparedStatement) Proxy.newProxyInstance(
        PreparedStatement.class.getClassLoader(),
        new Class[] { PreparedStatement.class }, psh);

    UniqueKey pk = new UniqueKey("numnum:int,strstr:string");
    pk.setContentSqlValues(ps, "888/bluesky");
    assertEquals(2, psh.ncalls);
  }

  private static class PreparedStatementHandlerPerDocCols implements 
      InvocationHandler {
    List<String> callsAndValues = new ArrayList<String>();
    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {
      String methodName = method.getName();
      callsAndValues.add(methodName);
      callsAndValues.add("" + args[0]);
      callsAndValues.add("" + args[1]);
      return null;
    }
  }

  @Test
  public void testPreparingRetrievalPerDocCols() throws SQLException {
    PreparedStatementHandlerPerDocCols psh
        = new PreparedStatementHandlerPerDocCols();
    PreparedStatement ps = (PreparedStatement) Proxy.newProxyInstance(
        PreparedStatement.class.getClassLoader(),
        new Class[] { PreparedStatement.class }, psh);

    UniqueKey pk = new UniqueKey("numnum:int,strstr:string",
        "numnum,numnum,strstr,numnum,strstr,strstr,numnum", "");
    pk.setContentSqlValues(ps, "888/bluesky");
    List<String> golden = Arrays.asList(
        "setInt", "1", "888",
        "setInt", "2", "888",
        "setString", "3", "bluesky",
        "setInt", "4", "888",
        "setString", "5", "bluesky",
        "setString", "6", "bluesky",
        "setInt", "7", "888"
    );
    assertEquals(golden, psh.callsAndValues);
  }

  private static String roundtrip(String in) {
    return UniqueKey.decodeSlashInData(UniqueKey.encodeSlashInData(in));
  }

  @Test
  public void testEmpty() {
    assertEquals("", roundtrip(""));
  }

  @Test
  public void testSimpleEscaping() {
    String id = "my-simple-id";
    assertEquals(id, roundtrip(id));
  }

  @Test
  public void testWithSlash() {
    String id = "my-/simple-id";
    assertEquals(id, roundtrip(id));
  }

  @Test
  public void testWithBackSlash() {
    String id = "my-\\simple-id";
    assertEquals(id, roundtrip(id));
  }

  @Test
  public void testWithTripleBackSlash() {
    String id = "\\\\\\";
    assertEquals(3, id.length());
    assertEquals(id, roundtrip(id));
  }

  @Test
  public void testWithSlashAndBackslash() {
    String id = "my-\\simp/le-id";
    assertEquals(id, roundtrip(id));
  }

  @Test
  public void testWithMessOfSlashAndBackslash() {
    String id = "/\\/\\my-\\/simp/le-id/\\/\\\\\\//\\";
    assertEquals(id, roundtrip(id));
  }

  private static String makeRandomId() {
    char choices[] = "13/\\45\\97%^&%$^)*(/<>|P{UITY*c".toCharArray();
    StringBuilder sb = new StringBuilder();
    Random r = new Random();
    int len = r.nextInt(100);
    for (int i = 0; i < len; i++) {
      sb.append(choices[r.nextInt(choices.length)]);
    }
    return "" + sb;
  }

  @Test
  public void testWithFuzz() {
    int ntests = 100;
    for (int i = 0; i < ntests; i++) {
      String fuzz = makeRandomId();
      assertEquals(fuzz, roundtrip(fuzz));
    }
  }
}
