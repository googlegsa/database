// Copyright 2014 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.enterprise.adaptor.database;

import static org.junit.Assert.assertTrue;

import org.h2.jdbcx.JdbcDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;

/** Manages an in-memory H2 database for test purposes. */
class JdbcFixture {
  public static final String DRIVER_CLASS = "org.h2.Driver";
  public static final String URL =
      "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DEFAULT_ESCAPE=";
  public static final String USER = "sa";
  public static final String PASSWORD = "";

  private static ArrayDeque<AutoCloseable> openObjects = new ArrayDeque<>();

  /**
   * Gets a JDBC connection to a named in-memory database.
   * <p>
   * Connection options:
   *
   * <pre>DB_CLOSE_DELAY=-1</pre>
   * The database will remain open until the JVM exits, rather than
   * being closed when the last connection to it is closed.
   *
   * <pre>DEFAULT_ESCAPE=</pre>
   * The default escape character is disabled, to match the DQL
   * behavior (and standard SQL).
   */
  public static Connection getConnection() {
    try {
      JdbcDataSource ds = new JdbcDataSource();
      ds.setURL(URL);
      ds.setUser(USER);
      ds.setPassword(PASSWORD);
      return ds.getConnection();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public static void dropAllObjects() throws SQLException {
    try {
      for (AutoCloseable object : openObjects) {
        object.close();
      }
    } catch (Exception e) {
      if (e instanceof SQLException) {
        throw (SQLException) e;
      } else {
        throw new AssertionError(e);
      }
    }
    executeUpdate("drop all objects");
  }

  public static ResultSet executeQuery(String sql) throws SQLException {
    Statement stmt = getConnection().createStatement();
    openObjects.addFirst(stmt);
    ResultSet rs = stmt.executeQuery(sql);
    openObjects.addFirst(rs);
    return rs;
  }

  public static ResultSet executeQueryAndNext(String sql) throws SQLException {
    ResultSet rs = executeQuery(sql);
    assertTrue("ResultSet is empty", rs.next());
    return rs;
  }

  public static void executeUpdate(String... sqls) throws SQLException {
    try (Connection connection = getConnection();
         Statement stmt = connection.createStatement()) {
      for (String sql : sqls) {
        stmt.executeUpdate(sql);
      }
    }
  }

  private JdbcFixture() {
    // Prevents instantiation.
  }
}
