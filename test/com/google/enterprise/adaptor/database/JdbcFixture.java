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

import com.google.common.base.Strings;
import org.h2.jdbcx.JdbcDataSource;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.Properties;

/** Manages an in-memory H2 database for test purposes. */
class JdbcFixture {
  public static String database;
  public static String driver_class;
  public static String url;
  public static String user;
  public static String password;

  private static ArrayDeque<AutoCloseable> openObjects = new ArrayDeque<>();

  /*
   * Initializes the database connection parameters from build.properties file.
   * Defaults to H2 database.
   */
  public static void initialize() throws IOException {
    String propertiesFile = System.getProperty("build.properties");
    Properties properties = new Properties();
    if (!Strings.isNullOrEmpty(propertiesFile)) {
      try {
        properties.load(new FileReader(propertiesFile));
        if (Strings.isNullOrEmpty(properties.getProperty("test.database"))) {
          loadDefaultProperies(properties);
        }
      } catch (FileNotFoundException e) {
        loadDefaultProperies(properties);
      }
    } else {
      loadDefaultProperies(properties);
    }

    database = properties.getProperty("test.database");
    driver_class = properties.getProperty("test.driver");
    url = properties.getProperty("test.url");
    user = properties.getProperty("test.user");
    password = properties.getProperty("test.password");
  }

  /*
   *  Connection options for default H2 database:
   *
   * <pre>DB_CLOSE_DELAY=-1</pre>
   * The database will remain open until the JVM exits, rather than
   * being closed when the last connection to it is closed.
   *
   * <pre>DEFAULT_ESCAPE=</pre>
   * The default escape character is disabled, to match the DQL
   * behavior (and standard SQL).
   */
  private static void loadDefaultProperies(Properties properties) {
    properties.setProperty("test.database", "h2");
    properties.setProperty("test.driver", "org.h2.Driver");
    properties.setProperty("test.url",
        "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DEFAULT_ESCAPE=");
    properties.setProperty("test.user", "sa");
    properties.setProperty("test.password", "");
  }

  /**
   * Gets a JDBC connection to the database.
   */
  public static Connection getConnection() {
    try {
      if (database.equalsIgnoreCase("h2")) {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL(url);
        ds.setUser(user);
        ds.setPassword(password);
        return ds.getConnection();
      } else {
        try {
          Class.forName(driver_class);
          return DriverManager.getConnection(url, user, password);
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
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
    if (database.equalsIgnoreCase("h2")) {
      executeUpdate("drop all objects");
    } else if (database.equalsIgnoreCase("sqlserver")) {
      dropSQLServerTables();
      // TODO (srinivas): drop tables for Oracle database.
    }
  }

  private static void dropSQLServerTables() throws SQLException {
    String sql = "select 'DROP TABLE ' + name + '' FROM sys.tables";
    ResultSet rs = executeQuery(sql);
    while (rs.next()) {
      String dropQuery = rs.getString(1);
      Statement stmt = getConnection().createStatement();
      stmt.execute(dropQuery);
    }
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
