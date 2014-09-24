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

import org.junit.*;
import org.junit.rules.ExpectedException;

import org.junit.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/** Test cases for {@link PrimaryKey}. */
public class PrimaryKeyTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testSingleInt() {
    PrimaryKey pk = new PrimaryKey("numnum:int");
    assertEquals(1, pk.numElements());
    assertEquals("numnum", pk.name(0));
    assertEquals(PrimaryKey.ColumnType.INT, pk.type(0));
  }

  @Test
  public void testEmptyThrows() {
    thrown.expect(IllegalArgumentException.class);
    PrimaryKey pk = new PrimaryKey(" ");
  }

  @Test
  public void testTwoInt() {
    PrimaryKey pk = new PrimaryKey("numnum:int,other:int");
    assertEquals(2, pk.numElements());
    assertEquals("numnum", pk.name(0));
    assertEquals("other", pk.name(1));
    assertEquals(PrimaryKey.ColumnType.INT, pk.type(0));
    assertEquals(PrimaryKey.ColumnType.INT, pk.type(1));
  }

  @Test
  public void testIntString() {
    PrimaryKey pk = new PrimaryKey("numnum:int,strstr:string");
    assertEquals(2, pk.numElements());
    assertEquals("numnum", pk.name(0));
    assertEquals("strstr", pk.name(1));
    assertEquals(PrimaryKey.ColumnType.INT, pk.type(0));
    assertEquals(PrimaryKey.ColumnType.STRING, pk.type(1));
  }

  @Test
  public void testBadDef() {
    thrown.expect(IllegalArgumentException.class);
    PrimaryKey pk = new PrimaryKey("numnum:int,strstr/string");
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
    PrimaryKey pk = new PrimaryKey("numnum:int,strstr:string");
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

    PrimaryKey pk = new PrimaryKey("numnum:int,strstr:string");
    pk.setStatementValues(ps, "888/bluesky");
    assertEquals(2, psh.ncalls);
  }
}

/*
  @Test
  public void testMakingAllIdsSqlString() {
    PrimaryKey pk = new PrimaryKey("numnum:int,strstr:string");
    String sql = pk.makeGetAllIdsSqlString("bobby_tables");
    assertEquals("select numnum, strstr from bobby_tables", sql);
  }
*/
