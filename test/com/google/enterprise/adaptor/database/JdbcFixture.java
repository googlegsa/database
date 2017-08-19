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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.US;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Strings;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Manages a JDBC accessed database for test purposes.
 * By default, these tests use an in-memory H2 database,
 * however other database drivers may be used by configuring
 * test.jdbc.* properties in build.properties.
 */
class JdbcFixture {
  public static final Database DATABASE;
  public static final String DRIVER_CLASS;
  public static final String URL;
  public static final String USER;
  public static final String PASSWORD;
  public static final String BOOLEAN;
  public static final String INTEGER;

  private static ConcurrentLinkedDeque<AutoCloseable> openObjects =
      new ConcurrentLinkedDeque<>();

  public static enum Database { H2, MYSQL, ORACLE, SQLSERVER };

  /*
   * Initializes the database connection parameters from build.properties file.
   * Defaults to H2 database.
   *
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
  static {
    Database dbname = Database.H2;
    String dbdriver = "org.h2.Driver";
    String dburl = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DEFAULT_ESCAPE=";
    String dbuser = "sa";
    String dbpassword = "";

    String propertiesFile = System.getProperty("build.properties");
    if (!Strings.isNullOrEmpty(propertiesFile)) {
      try (BufferedReader br =
          Files.newBufferedReader(Paths.get(propertiesFile), UTF_8)) {
        Properties properties = new Properties();
        properties.load(br);
        String name = properties.getProperty("test.jdbc.database");
        if (!Strings.isNullOrEmpty(name)) {
          dbname = Database.valueOf(name.toUpperCase(US));
          dbdriver = properties.getProperty("test.jdbc.driver");
          dburl = properties.getProperty("test.jdbc.url");
          dbuser = properties.getProperty("test.jdbc.user");
          dbpassword = properties.getProperty("test.jdbc.password");
        }
      } catch (NoSuchFileException | IllegalArgumentException e) {
        // use default values.
      } catch (IOException e) {
        // This exception is not displayed by JUnit, so print it, too.
        System.err.println("Failed to load build.properties: " + e);
        throw new RuntimeException(e);
      }
    }

    // TODO(SV): Log JDBC config.
    DATABASE = dbname;
    DRIVER_CLASS = dbdriver;
    URL = dburl;
    USER = dbuser;
    PASSWORD = dbpassword;
    BOOLEAN = (DATABASE == Database.MYSQL || DATABASE == Database.SQLSERVER)
        ? "BIT" : "BOOLEAN";
    INTEGER = (DATABASE == Database.ORACLE) ? "NUMERIC" : "INTEGER";
  }

  /**
   * Returns {@code true} if the Database is of the supplied type.
   */
  public static boolean is(Database database) {
    return DATABASE == database;
  }

  /** Gets a string value matching the corresponding JDBC d escape output. */
  public static String d(String date) {
    return is(Database.ORACLE) ? date + " 00:00:00.0" : date;
  }

  /** Gets a string value matching the corresponding JDBC t escape output. */
  public static String t(String time) {
    return is(Database.ORACLE) ? "1970-01-01 " + time + ".0" : time;
  }

  /**
   * Gets a JDBC connection to the database.
   */
  public static Connection getConnection() {
    try {
      Class.forName(DRIVER_CLASS);
      Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
      openObjects.addFirst(conn);
      return conn;
    } catch (SQLException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public static void dropAllObjects() throws SQLException {
    closeOpenObjects();
    switch (DATABASE) {
      case H2:
        executeUpdate("drop all objects");
        break;
      case MYSQL:
        dropDatabaseTables("select table_name from information_schema.tables "
            + "where table_schema = (select database())", "table_name");
        break;
      case ORACLE:
        dropDatabaseTables("select table_name from user_tables", "table_name");
        break;
      case SQLSERVER:
        dropDatabaseTables("select name from sys.tables", "name");
        break;
    }
    closeOpenObjects();
  }

  private static void closeOpenObjects() throws SQLException {
    try {
      AutoCloseable object;
      while ((object = openObjects.poll()) != null) {
        object.close();
      }
    } catch (Exception e) {
      if (e instanceof SQLException) {
        throw (SQLException) e;
      } else {
        throw new AssertionError(e);
      }
    }
  }

  private static void dropDatabaseTables(String query, String column)
      throws SQLException {
    ResultSet rs = executeQuery(query);
    Connection connection = getConnection();
    while (rs.next()) {
      String dropQuery = "drop table " + rs.getString(column);
      try (Statement stmt = connection.createStatement()) {
        stmt.execute(dropQuery);
      }
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
    Connection connection = getConnection();
    try (Statement stmt = connection.createStatement()) {
      for (String sql : sqls) {
        switch (DATABASE) {
          case MYSQL:
            if (sql.startsWith("create table")) {
              sql = sql
                  .replaceAll("\\bclob\\b", "text")
                  .replaceAll("\\blongvarbinary\\b", "long varbinary")
                  .replaceAll("\\btimestamp\\b(\\(\\d\\))?", "datetime$1 null");
            }
            break;
          case ORACLE:
            if (sql.startsWith("create table")) {
              sql = sql
                  .replaceAll("\\bbigint\\b", "integer")
                  .replaceAll("\\blongvarbinary\\b", "long raw")
                  .replaceAll("\\btime\\b", "date")
                  .replaceAll("\\bvarbinary\\b", "raw");
            }
            break;
          case SQLSERVER:
            if (sql.startsWith("create table")) {
              sql = sql
                  .replaceAll("\\bblob\\b", "varbinary(max)")
                  .replaceAll("\\bclob\\b", "varchar(max)")
                  .replaceAll("\\btimestamp\\b", "datetime2");
            } else if (sql.startsWith("insert")) {
              // A ts escape is treated as a datetime rather than a
              // datetime2, and rounded incorrectly. Use a string literal.
              sql = sql.replaceAll("\\{ts ('[^']*')\\}", "$1");
            }
            break;
        }
        stmt.executeUpdate(sql);
      }
    }
  }

  public static PreparedStatement prepareStatement(String sql)
      throws SQLException {
    PreparedStatement stmt = getConnection().prepareStatement(sql);
    openObjects.addFirst(stmt);
    return stmt;
  }

  private JdbcFixture() {
    // Prevents instantiation.
  }
}
