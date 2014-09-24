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

import com.google.enterprise.adaptor.AbstractAdaptor;
import com.google.enterprise.adaptor.AdaptorContext;
import com.google.enterprise.adaptor.Config;
import com.google.enterprise.adaptor.DocId;
import com.google.enterprise.adaptor.DocIdPusher;
import com.google.enterprise.adaptor.InvalidConfigurationException;
import com.google.enterprise.adaptor.Request;
import com.google.enterprise.adaptor.Response;

import java.io.*;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.*;
import java.util.logging.*;

/** Puts SQL database into GSA index. */
public class DatabaseAdaptor extends AbstractAdaptor {
  private static final Logger log
      = Logger.getLogger(DatabaseAdaptor.class.getName());
  private Charset encoding = Charset.forName("UTF-8");

  private int maxIdsPerFeedFile;
  private String driverClass;
  private String dbUrl;
  private PrimaryKey primaryKey;
  private String getIdsSql;
  private String getContentSql;
  private String user;
  private String password;
  private MetadataColumns metadataColumns;
 
  @Override
  public void initConfig(Config config) {
    config.addKey("db.driverclass", null);
    config.addKey("db.url", null);
    config.addKey("db.user", null);
    config.addKey("db.password", null);
    config.addKey("db.primarykey", null);
    config.addKey("db.getIdsSql", null);
    config.addKey("db.getContentSql", null);
    config.addKey("db.metadataColumns", "");
  }

  @Override
  public void init(AdaptorContext context) throws Exception {
    maxIdsPerFeedFile = Integer.parseInt(
        context.getConfig().getValue("feed.maxUrls"));
    if (maxIdsPerFeedFile <= 0) {
      String errmsg = "feed.maxUrls needs to be positive";
      throw new InvalidConfigurationException(errmsg);
    }

    driverClass = context.getConfig().getValue("db.driverclass");
    Class.forName(driverClass);
    log.config("loaded driver");

    dbUrl = context.getConfig().getValue("db.url");
    log.config("db: " + dbUrl);

    user = context.getConfig().getValue("db.user");
    password = context.getConfig().getValue("db.password");
    log.config("db user: " + user);
    log.config("db password: " + password);

    primaryKey = new PrimaryKey(
       context.getConfig().getValue("db.primarykey"));
    log.config("primary key: " + primaryKey);

    getIdsSql = context.getConfig().getValue("db.getIdsSql");
    log.config("get ids sql: " + getIdsSql);

    getContentSql = context.getConfig().getValue("db.getContentSql");
    log.config("get content sql: " + getContentSql);

    metadataColumns = new MetadataColumns(
       context.getConfig().getValue("db.metadataColumns"));
    log.config("metadata columns: " + metadataColumns);
  }

  /** Get all doc ids from database. */
  @Override
  public void getDocIds(DocIdPusher pusher) throws IOException,
         InterruptedException {
    BufferingPusher outstream = new BufferingPusher(pusher);
    Connection conn = null;
    StatementAndResult statementAndResult = null;
    try {
      conn = makeNewConnection();
      statementAndResult = getStreamFromDb(conn, getIdsSql);
      ResultSet rs = statementAndResult.resultSet;
      while (rs.next()) {
        DocId id = new DocId(primaryKey.makeUniqueId(rs));
        log.info("doc id: " + id);
        outstream.add(id);
      }
    } catch (SQLException problem) {
      log.log(Level.SEVERE, "failed getting ids", problem);
      throw new IOException(problem);
    } finally {
      tryClosingStatementAndResult(statementAndResult);
      tryClosingConnection(conn);
    }
    outstream.forcePush();
  }

  /** Gives the bytes of a document referenced with id. */
  @Override
  public void getDocContent(Request req, Response resp) throws IOException {
    DocId id = req.getDocId();
    Connection conn = null;
    StatementAndResult statementAndResult = null;
    try {
      conn = makeNewConnection();

      statementAndResult = getCollectionFromDb(conn, id.getUniqueId());
      ResultSet rs = statementAndResult.resultSet;

      // First handle cases with no data to return.
      boolean hasResult = rs.next();
      if (!hasResult) {
        resp.respondNotFound();
        return;
      }
      ResultSetMetaData rsMetaData = rs.getMetaData();
      int numberOfColumns = rsMetaData.getColumnCount();

      // If we have data then create lines of resulting document.
      StringBuilder line1 = new StringBuilder();
      StringBuilder line2 = new StringBuilder();
      StringBuilder line3 = new StringBuilder();
      for (int i = 1; i < (numberOfColumns + 1); i++) {
        String tableName = rsMetaData.getTableName(i);
        String columnName = rsMetaData.getColumnName(i);
        Object value = rs.getObject(i);
        line1.append(",");
        line1.append(makeIntoCsvField(tableName));
        line2.append(",");
        line2.append(makeIntoCsvField(columnName));
        line3.append(",");
        line3.append(makeIntoCsvField("" + value));
        if (metadataColumns.isMetadataColumnName(columnName)) {
          String key = metadataColumns.getMetadataName(columnName);
          resp.addMetadata(key, "" + value);
        }
      }
      String document = line1.substring(1) + "\n"
          + line2.substring(1) + "\n" + line3.substring(1) + "\n";
      resp.setContentType("text/plain");
      resp.getOutputStream().write(document.getBytes(encoding));
    } catch (SQLException problem) {
      log.log(Level.SEVERE, "failed getting content", problem);
      throw new IOException("retrieval error", problem);
    } finally {
      tryClosingStatementAndResult(statementAndResult);
      tryClosingConnection(conn);
    }
  }

  public static void main(String[] args) {
    AbstractAdaptor.main(new DatabaseAdaptor(), args);
  }

  private static class StatementAndResult {
    Statement statement;
    ResultSet resultSet;
    StatementAndResult(Statement st, ResultSet rs) { 
      if (null == st) {
        throw new NullPointerException();
      }
      if (null == rs) {
        throw new NullPointerException();
      }
      statement = st;
      resultSet = rs;
    }
  }

  private Connection makeNewConnection() throws SQLException {
    log.fine("about to connect");
    Connection conn = DriverManager.getConnection(dbUrl, user, password);
    log.fine("connected");
    return conn;
  }

  private StatementAndResult getCollectionFromDb(Connection conn,
      String uniqueId) throws SQLException {
    PreparedStatement st = conn.prepareStatement(getContentSql);
    primaryKey.setStatementValues(st, uniqueId);  
    log.info("about to query: " + st);
    ResultSet rs = st.executeQuery();
    log.fine("queried");
    return new StatementAndResult(st, rs); 
  }

  private StatementAndResult getStreamFromDb(Connection conn,
      String query) throws SQLException {
    Statement st = conn.createStatement(
        /* 1st streaming flag */ java.sql.ResultSet.TYPE_FORWARD_ONLY,
        /* 2nd streaming flag */ java.sql.ResultSet.CONCUR_READ_ONLY);
    st.setFetchSize(maxIdsPerFeedFile);  // Integer.MIN_VALUE for MySQL?
    log.fine("about to query for stream: " + query);
    ResultSet rs = st.executeQuery(query);
    log.fine("queried for stream");
    return new StatementAndResult(st, rs); 
  }

  private static void tryClosingStatementAndResult(StatementAndResult strs) {
    if (null != strs) {
      try {
        strs.resultSet.close();
      } catch (SQLException e) {
        log.log(Level.WARNING, "result set close failed", e);
      }
      try {
        strs.statement.close();
      } catch (SQLException e) {
        log.log(Level.WARNING, "statement close failed", e);
      }
    }
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

  private static String makeIntoCsvField(String s) {
    /*
     * Fields that contain a special character (comma, newline,
     * or double quote), must be enclosed in double quotes.
     * <...> If a field's value contains a double quote character
     * it is escaped by placing another double quote character next to it.
     */
    String doubleQuote = "\"";
    boolean containsSpecialChar = s.contains(",")
        || s.contains("\n") || s.contains(doubleQuote);
    if (containsSpecialChar) {
      s = s.replace(doubleQuote, doubleQuote + doubleQuote);
      s = doubleQuote + s + doubleQuote;
    }
    return s;
  }

  /**
   * Mechanism that accepts stream of DocId instances, bufferes them,
   * and sends them when it has accumulated maximum allowed amount per
   * feed file.
   */
  private class BufferingPusher {
    DocIdPusher wrapped;
    ArrayList<DocId> saved;
    BufferingPusher(DocIdPusher underlying) {
      wrapped = underlying;
      saved = new ArrayList<DocId>(maxIdsPerFeedFile);
    }
    void add(DocId id) throws InterruptedException {
      saved.add(id);
      if (saved.size() >= maxIdsPerFeedFile) {
        forcePush();
      }
    }
    void forcePush() throws InterruptedException {
      wrapped.pushDocIds(saved);
      log.fine("sent " + saved.size() + " doc ids to pusher");
      saved.clear();
    }
    protected void finalize() throws Throwable {
      if (0 != saved.size()) {
        log.severe("still have saved ids that weren't sent");
      }
    }
  }
}
