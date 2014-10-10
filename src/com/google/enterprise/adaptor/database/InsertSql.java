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

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.logging.*;

/**
 * Reads in SQL statements and runs them on your DB.
 * Statement is deemed to end when a line is exactly ");".
 */
class InsertSql {
  private static final Logger log = Logger.getLogger(InsertSql.class.getName());

  private static String driverClass;
  private static String dburl;
  private static String user;
  private static String password;
  private static String fn;

  private static void setParameters(String a[]) {
    if (5 != a.length) {
      throw new IllegalArgumentException("5 parameters are required: "
          + "driver-class db-url user password file-with-sql");
    }
    driverClass = a[0];
    dburl = a[1];
    user = a[2];
    password = a[3];
    fn = a[4];
  }

  private static Connection makeNewConnection() throws SQLException {
    log.fine("about to connect");
    Connection conn = DriverManager.getConnection(dburl, user, password);
    log.fine("connected");
    return conn;
  }

  private static void tryClosingConnection(Connection conn) {
    if (null != conn) {
      try {
        conn.close();
      } catch (SQLException e) {
        log.log(Level.WARNING, "connection close failed", e);
      }
    }
  }
 
  public static void main(String a[]) throws SQLException, 
      ClassNotFoundException, FileNotFoundException, IOException {
    setParameters(a);
    Class.forName(driverClass);
    log.config("loaded driver");
    Connection conn = makeNewConnection();
    Statement stmt = conn.createStatement();
    StatementReader sr = new StatementReader();
    String sql;
    while ((sql = sr.next()) != null) {
      stmt.executeUpdate(sql);
    }
    tryClosingConnection(conn);
  }

  private static class StatementReader {
    private BufferedReader br;
    StatementReader() throws FileNotFoundException {
      br = new BufferedReader(new FileReader(fn));
    }
    String next() throws IOException {
      StringBuilder sb = new StringBuilder(); 
      String line = br.readLine();
      if (line == null) {
        return null;
      }
      while (!line.equals(");")) {
        sb.append(line).append("\n");
        line = br.readLine();
      }
      sb.append(line).append("\n");
      return sb.toString();
    }
  }  
}
