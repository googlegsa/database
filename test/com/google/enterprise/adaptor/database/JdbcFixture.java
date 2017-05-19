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

import org.h2.jdbcx.JdbcDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/** Manages an in-memory H2 database for test purposes. */
public class JdbcFixture {
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
      ds.setURL("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DEFAULT_ESCAPE=");
      ds.setUser("sa");
      ds.setPassword("");
      return ds.getConnection();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public static void dropAllObjects() throws SQLException {
    executeUpdate("drop all objects");
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
