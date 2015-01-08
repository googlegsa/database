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

import com.google.common.annotations.VisibleForTesting;
import com.google.enterprise.adaptor.AbstractAdaptor;
import com.google.enterprise.adaptor.Acl;
import com.google.enterprise.adaptor.AdaptorContext;
import com.google.enterprise.adaptor.Config;
import com.google.enterprise.adaptor.DocId;
import com.google.enterprise.adaptor.DocIdPusher;
import com.google.enterprise.adaptor.GroupPrincipal;
import com.google.enterprise.adaptor.InvalidConfigurationException;
import com.google.enterprise.adaptor.PollingIncrementalLister;
import com.google.enterprise.adaptor.Request;
import com.google.enterprise.adaptor.Response;
import com.google.enterprise.adaptor.UserPrincipal;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.logging.*;

/** Puts SQL database into GSA index. */
public class DatabaseAdaptor extends AbstractAdaptor {
  private static final Logger log
      = Logger.getLogger(DatabaseAdaptor.class.getName());
  
  private static boolean isNullOrEmptyString(String str) {
    return null == str || "".equals(str.trim());
  }

  private int maxIdsPerFeedFile;
  private String driverClass;
  private String dbUrl;
  private String user;
  private String password;
  private UniqueKey uniqueKey;
  private String everyDocIdSql;
  private String singleDocContentSql;
  private MetadataColumns metadataColumns;
  private ResponseGenerator respGenerator;
  private String aclSql;
  private String aclPrincipalDelimiter;
  private boolean disableStreaming;
  private Calendar updateTimestampTimezone;

  @Override
  public void initConfig(Config config) {
    config.addKey("db.driverClass", null);
    config.addKey("db.url", null);
    config.addKey("db.user", null);
    config.addKey("db.password", null);
    config.addKey("db.uniqueKey", null);
    config.addKey("db.everyDocIdSql", null);
    config.addKey("db.singleDocContentSql", null);
    config.addKey("db.singleDocContentSqlParameters", "");
    config.addKey("db.metadataColumns", "");
    config.addKey("db.modeOfOperation", null);
    config.addKey("db.updateSql", "");
    config.addKey("db.aclSql", "");
    config.addKey("db.aclSqlParameters", "");
    // By default, the delimiter is a single comma. This delimiter will be taken
    // from admin's config as is. For example, if the delimiter is
    //   "", it means no splitting
    //   "  ", it means to split with exactly two whitespaces
    //   " , ", it means to split with one leading whitespace, one comma, and
    //          one trailing whitespace
    config.addKey("db.aclPrincipalDelimiter", ",");
    config.addKey("db.disableStreaming", "false");
    // For updateTimestampTimezone, the default value will be empty string,
    // which means it ends up being adaptor machine's timezone.
    // 
    // Timezone conversions are required in two cases:
    //  1) the database column used as timestamp doesn't preserve timezone
    //     information, such as datetime in MS SQL Server and TIMESTAMP in
    //     Oracle. In this case, updateTimestampTimezone needs be set according
    //     to database server's timezone.
    //  2) the database column used as timestamp does preserve timezone
    //     information, such as datetimeoffset in MS SQL Server and
    //     TIMESTAMP WITH TIME ZONE in Oracle. In this case,
    //     updateTimestampTimezone needs to be set to UTC or GMT or any
    //     equivalent.
    // 
    // See java.util.TimeZone javadoc for valid values for TimeZone.
    // http://docs.oracle.com/javase/7/docs/api/java/util/TimeZone.html
    config.addKey("db.updateTimestampTimezone", "");
  }

  @Override
  public void init(AdaptorContext context) throws Exception {
    Config cfg = context.getConfig();
    maxIdsPerFeedFile = Integer.parseInt(cfg.getValue("feed.maxUrls"));
    if (maxIdsPerFeedFile <= 0) {
      String errmsg = "feed.maxUrls needs to be positive";
      throw new InvalidConfigurationException(errmsg);
    }

    driverClass = cfg.getValue("db.driverClass");
    Class.forName(driverClass);
    log.config("loaded driver: " + driverClass);

    dbUrl = cfg.getValue("db.url");
    log.config("db: " + dbUrl);

    user = cfg.getValue("db.user");
    log.config("db user: " + user);

    password = context.getSensitiveValueDecoder().decodeValue(
        cfg.getValue("db.password"));

    uniqueKey = new UniqueKey(
        cfg.getValue("db.uniqueKey"),
        cfg.getValue("db.singleDocContentSqlParameters"),
        cfg.getValue("db.aclSqlParameters")
    );
    log.config("primary key: " + uniqueKey);

    everyDocIdSql = cfg.getValue("db.everyDocIdSql");
    log.config("every doc id sql: " + everyDocIdSql);

    singleDocContentSql = cfg.getValue("db.singleDocContentSql");
    log.config("single doc content sql: " + singleDocContentSql);

    metadataColumns = new MetadataColumns(cfg.getValue("db.metadataColumns"));
    log.config("metadata columns: " + metadataColumns);

    respGenerator = loadResponseGenerator(cfg);
    
    if (!isNullOrEmptyString(cfg.getValue("db.aclSql"))) {
      aclSql = cfg.getValue("db.aclSql");
      log.config("acl sql: " + aclSql); 
      aclPrincipalDelimiter = cfg.getValue("db.aclPrincipalDelimiter");
      log.config("aclPrincipalDelimiter: '" + aclPrincipalDelimiter + "'");
    }

    disableStreaming = new Boolean(cfg.getValue("db.disableStreaming"));
    log.config("disableStreaming: " + disableStreaming);

    String tzString = cfg.getValue("db.updateTimestampTimezone");
    if (isNullOrEmptyString(tzString)) {
      updateTimestampTimezone = Calendar.getInstance();
    } else {
      updateTimestampTimezone =
          Calendar.getInstance(TimeZone.getTimeZone(cfg
              .getValue("db.updateTimestampTimezone")));
    }
    log.config("updateTimestampTimezone: "
        + updateTimestampTimezone.getTimeZone().getDisplayName());

    // incremental lister has to be initiated after updateTimestampTimezone
    // because its formatter member variable depends on updateTimestampTimezone
    // to be set.
    DbAdaptorIncrementalLister incrementalLister
        = initDbAdaptorIncrementalLister(cfg);
    if (incrementalLister != null) {
      context.setPollingIncrementalLister(incrementalLister);
    }
  }

  /** Get all doc ids from database. */
  @Override
  public void getDocIds(DocIdPusher pusher) throws IOException,
      InterruptedException {
    BufferedPusher outstream = new BufferedPusher(pusher);
    Connection conn = null;
    StatementAndResult statementAndResult = null;
    try {
      conn = makeNewConnection();
      statementAndResult = getStreamFromDb(conn, everyDocIdSql);
      ResultSet rs = statementAndResult.resultSet;
      while (rs.next()) {
        DocId id = new DocId(uniqueKey.makeUniqueId(rs));
        DocIdPusher.Record record =
            new DocIdPusher.Record.Builder(id).build();
        log.log(Level.FINEST, "doc id: {0}", id);
        outstream.add(record);
      }
    } catch (SQLException ex) {
      throw new IOException(ex);
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
      statementAndResult = getDocFromDb(conn, id.getUniqueId());
      ResultSet rs = statementAndResult.resultSet;
      // First handle cases with no data to return.
      boolean hasResult = rs.next();
      if (!hasResult) {
        resp.respondNotFound();
        return;
      }
      // Generate response metadata first.
      ResultSetMetaData rsMetaData = rs.getMetaData();
      int numberOfColumns = rsMetaData.getColumnCount();
      for (int i = 1; i < (numberOfColumns + 1); i++) {
        String columnName = rsMetaData.getColumnName(i);
        Object value = rs.getObject(i);
        String key = metadataColumns.getMetadataName(columnName);
        if (key != null) {
          resp.addMetadata(key, "" + value);
        }
      }
      // Generate Acl if aclSql is provided.
      if (aclSql != null) {
        Acl acl = getAcl(conn, id.getUniqueId());
        if (acl != null) {
          resp.setAcl(acl);
        }
      }
      // Generate response body.
      // In database adaptor's case, we almost never want to follow the URLs.
      // One record means one document.
      resp.setNoFollow(true); 
      respGenerator.generateResponse(rs, resp);
    } catch (SQLException ex) {
      throw new IOException("retrieval error", ex);
    } finally {
      tryClosingStatementAndResult(statementAndResult);
      tryClosingConnection(conn);
    }
  }

  public static void main(String[] args) {
    AbstractAdaptor.main(new DatabaseAdaptor(), args);
  }
  
  private Acl getAcl(Connection conn, String uniqueId) throws SQLException {
    StatementAndResult statementAndResult = null;
    try {
      statementAndResult = getAclFromDb(conn, uniqueId);
      ResultSet rs = statementAndResult.resultSet;
      ResultSetMetaData metadata = rs.getMetaData();
      return buildAcl(rs, metadata, aclPrincipalDelimiter);
    } finally {
      tryClosingStatementAndResult(statementAndResult);
    }
  }
  
  @VisibleForTesting
  static Acl buildAcl(ResultSet rs, ResultSetMetaData metadata, String delim)
      throws SQLException {
    boolean hasResult = rs.next();
    if (!hasResult) {
      // empty Acl ensures adaptor will mark this document as secure
      return Acl.EMPTY;
    }
    Acl.Builder builder = new Acl.Builder();
    ArrayList<UserPrincipal> permitUsers = new ArrayList<UserPrincipal>();
    ArrayList<UserPrincipal> denyUsers = new ArrayList<UserPrincipal>();
    ArrayList<GroupPrincipal> permitGroups = new ArrayList<GroupPrincipal>();
    ArrayList<GroupPrincipal> denyGroups = new ArrayList<GroupPrincipal>();
    boolean hasPermitUsers =
        hasColumn(metadata, GsaSpecialColumns.GSA_PERMIT_USERS.toString());
    boolean hasDenyUsers =
        hasColumn(metadata, GsaSpecialColumns.GSA_DENY_USERS.toString());
    boolean hasPermitGroups =
        hasColumn(metadata, GsaSpecialColumns.GSA_PERMIT_GROUPS.toString());
    boolean hasDenyGroups =
        hasColumn(metadata, GsaSpecialColumns.GSA_DENY_GROUPS.toString());
    do {
      if (hasPermitUsers) {
        permitUsers.addAll(getUserPrincipalsFromResultSet(rs,
            GsaSpecialColumns.GSA_PERMIT_USERS, delim));
      }
      if (hasDenyUsers) {
        denyUsers.addAll(getUserPrincipalsFromResultSet(rs,
            GsaSpecialColumns.GSA_DENY_USERS, delim));
      }
      if (hasPermitGroups) {
        permitGroups.addAll(getGroupPrincipalsFromResultSet(rs,
            GsaSpecialColumns.GSA_PERMIT_GROUPS, delim));
      }
      if (hasDenyGroups) {
        denyGroups.addAll(getGroupPrincipalsFromResultSet(rs,
            GsaSpecialColumns.GSA_DENY_GROUPS, delim));
      }
    } while (rs.next());
    return builder
        .setPermitUsers(permitUsers)
        .setDenyUsers(denyUsers)
        .setPermitGroups(permitGroups)
        .setDenyGroups(denyGroups)
        .build();
  }
  
  @VisibleForTesting
  static ArrayList<UserPrincipal> getUserPrincipalsFromResultSet(ResultSet rs,
      GsaSpecialColumns column, String delim) throws SQLException {
    ArrayList<UserPrincipal> principals = new ArrayList<UserPrincipal>();
    String value = rs.getString(column.toString());
    if (!isNullOrEmptyString(value)) {
      if ("".equals(delim)) {
        principals.add(new UserPrincipal(value.trim()));
      } else {
        // drop trailing empties
        String principalNames[] = value.split(delim, 0);
        for (String principalName : principalNames) {
          principals.add(new UserPrincipal(principalName.trim()));
        }
      }
    }
    return principals;
  }

  @VisibleForTesting
  static ArrayList<GroupPrincipal> getGroupPrincipalsFromResultSet(ResultSet rs,
      GsaSpecialColumns column, String delim) throws SQLException {
    ArrayList<GroupPrincipal> principals = new ArrayList<GroupPrincipal>();
    String value = rs.getString(column.toString());
    if (!isNullOrEmptyString(value)) {
      if ("".equals(delim)) {
        principals.add(new GroupPrincipal(value.trim()));
      } else {
        // drop trailing empties
        String principalNames[] = value.split(delim, 0);
        for (String principalName : principalNames) {
          principals.add(new GroupPrincipal(principalName.trim()));
        }
      }
    }
    return principals;
  }
  
  private static boolean hasColumn(ResultSetMetaData metadata, String column)
      throws SQLException {
    int columns = metadata.getColumnCount();
    for (int x = 1; x <= columns; x++) {
      if (column.equals(metadata.getColumnName(x))) {
        return true;
      }
    }
    return false;
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
    log.finest("about to connect");
    Connection conn = DriverManager.getConnection(dbUrl, user, password);
    log.finest("connected");
    return conn;
  }

  private StatementAndResult getDocFromDb(Connection conn,
      String uniqueId) throws SQLException {
    PreparedStatement st = conn.prepareStatement(singleDocContentSql);
    uniqueKey.setContentSqlValues(st, uniqueId);  
    log.log(Level.FINER, "about to get doc: {0}",  uniqueId);
    ResultSet rs = st.executeQuery();
    log.finer("got doc");
    return new StatementAndResult(st, rs); 
  }

  private StatementAndResult getAclFromDb(Connection conn,
      String uniqueId) throws SQLException {
    PreparedStatement st = conn.prepareStatement(aclSql);
    uniqueKey.setAclSqlValues(st, uniqueId);  
    log.log(Level.FINER, "about to get acl: {0}",  uniqueId);
    ResultSet rs = st.executeQuery();
    log.finer("got acl");
    return new StatementAndResult(st, rs); 
  }

  private StatementAndResult getStreamFromDb(Connection conn,
      String query) throws SQLException {
    Statement st;
    if (disableStreaming) {
      st = conn.createStatement();
    } else {
      st = conn.createStatement(
          /* 1st streaming flag */ java.sql.ResultSet.TYPE_FORWARD_ONLY,
          /* 2nd streaming flag */ java.sql.ResultSet.CONCUR_READ_ONLY);
    }
    st.setFetchSize(maxIdsPerFeedFile);  // Integer.MIN_VALUE for MySQL?
    log.log(Level.FINER, "about to query for stream: {0}", query);
    ResultSet rs = st.executeQuery(query);
    log.finer("queried for stream");
    return new StatementAndResult(st, rs); 
  }

  private static void tryClosingStatementAndResult(StatementAndResult strs) {
    if (null != strs) {
      try {
        strs.resultSet.close();
      } catch (SQLException ex) {
        log.log(Level.WARNING, "result set close failed", ex);
      }
      try {
        strs.statement.close();
      } catch (SQLException ex) {
        log.log(Level.WARNING, "statement close failed", ex);
      }
    }
  }

  private static void tryClosingConnection(Connection conn) {
    if (null != conn) {
      try {
        conn.close();
      } catch (SQLException ex) {
        log.log(Level.WARNING, "connection close failed", ex);
      }
    }
  }

  /**
   * Mechanism that accepts stream of DocIdPusher.Record instances, buffers
   * them, and sends them when it has accumulated maximum allowed amount per
   * feed file.
   */
  private class BufferedPusher {
    DocIdPusher wrapped;
    ArrayList<DocIdPusher.Record> saved;
    
    BufferedPusher(DocIdPusher underlying) {
      if (null == underlying) {
        throw new NullPointerException();
      }
      wrapped = underlying;
      saved = new ArrayList<DocIdPusher.Record>(maxIdsPerFeedFile);
    }
    
    void add(DocIdPusher.Record record) throws InterruptedException {
      saved.add(record);
      if (saved.size() >= maxIdsPerFeedFile) {
        forcePush();
      }
    }
    
    void forcePush() throws InterruptedException {
      wrapped.pushRecords(saved);
      log.log(Level.FINE, "sent {0} doc ids to pusher", saved.size());
      saved.clear();
    }
    
    protected void finalize() throws Throwable {
      if (0 != saved.size()) {
        log.warning("still have saved ids that weren't sent");
      }
    }
  }

  @VisibleForTesting
  static ResponseGenerator loadResponseGenerator(Config config) {
    String mode = config.getValue("db.modeOfOperation");
    if (isNullOrEmptyString(mode)) {
      String errmsg = "modeOfOperation can not be an empty string";
      throw new InvalidConfigurationException(errmsg);
    }
    log.fine("about to look for " + mode + " in ResponseGenerator");
    Method method = null;
    try {
      method = ResponseGenerator.class.getDeclaredMethod(mode, Map.class);
      return loadResponseGeneratorInternal(method,
          config.getValuesWithPrefix("db.modeOfOperation." + mode + "."));
    } catch (NoSuchMethodException ex) {
      log.fine("did not find" + mode + " in ResponseGenerator, going to look"
          + " for fully qualified name");
    }
    log.fine("about to try " + mode + " as a fully qualified method name");
    int sepIndex = mode.lastIndexOf(".");
    if (sepIndex == -1) {
      String errmsg = mode + " cannot be parsed as a fully quailfied name";
      throw new InvalidConfigurationException(errmsg);
    }
    String className = mode.substring(0, sepIndex);
    String methodName = mode.substring(sepIndex + 1);
    log.log(Level.FINE, "Split {0} into class {1} and method {2}",
        new Object[] {mode, className, methodName});
    Class<?> klass;
    try {
      klass = Class.forName(className);
      method = klass.getDeclaredMethod(methodName, Map.class);
    } catch (ClassNotFoundException ex) {
      String errmsg = "No class " + className + " found";
      throw new InvalidConfigurationException(errmsg, ex);
    } catch (NoSuchMethodException ex) {
      String errmsg = "No method " + methodName + " found for class "
          + className;
      throw new InvalidConfigurationException(errmsg, ex);
    }
    return loadResponseGeneratorInternal(method,
        config.getValuesWithPrefix("db.modeOfOperation." + mode + "."));
  }
  
  @VisibleForTesting
  static ResponseGenerator loadResponseGeneratorInternal(Method method,
      Map<String, String> config) {
    log.fine("loading response generator specific configuration");
    ResponseGenerator respGenerator = null;
    Object retValue = null;
    try {
      retValue = method.invoke(/*static method*/null, config);
    } catch (IllegalAccessException | IllegalArgumentException
        | InvocationTargetException e) {
      String errmsg = "Unexpected exception happened in invoking method";
      throw new InvalidConfigurationException(errmsg, e);
    }
    if (retValue instanceof ResponseGenerator) {
      respGenerator = (ResponseGenerator) retValue;
    } else {
      String errmsg = String.format("Method %s needs to return a %s",
          method.getName(), ResponseGenerator.class.getName()); 
      throw new InvalidConfigurationException(errmsg);
    }
    log.config("loaded response generator: " + respGenerator.toString());
    return respGenerator;
  }

  // Incremental pushing in Database Adaptor based on Timestamp does NOT
  // guarantee to pick ALL updates. Some updates might still need to wait for 
  // next full push to be sent to GSA.
  private class DbAdaptorIncrementalLister implements PollingIncrementalLister {
    private final String updateSql;
    private Timestamp lastUpdateTimestamp;
    private final DateFormat formatter;

    public DbAdaptorIncrementalLister(String updateSql) {
      this.updateSql = updateSql;
      log.config("update sql: " + this.updateSql);
      this.lastUpdateTimestamp = new Timestamp(System.currentTimeMillis());
      formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z");
      formatter.setTimeZone(updateTimestampTimezone.getTimeZone());
    }

    @Override
    public void getModifiedDocIds(DocIdPusher pusher)
        throws IOException, InterruptedException {
      BufferedPusher outstream = new BufferedPusher(pusher);
      Connection conn = null;
      StatementAndResult statementAndResult = null;
      // latestTimestamp will be used to update lastUpdateTimestampInMillis
      // if GSA_TIMESTAMP column is present in the ResultSet and there is at 
      // least one non-null value of that column in the ResultSet.
      Timestamp latestTimestamp = null;
      boolean hasTimestamp = false;
      try {
        conn = makeNewConnection();
        statementAndResult = getUpdateStreamFromDb(conn);
        ResultSet rs = statementAndResult.resultSet;
        hasTimestamp =
            hasColumn(rs.getMetaData(),
                GsaSpecialColumns.GSA_TIMESTAMP.toString());
        log.log(Level.FINEST, "hasTimestamp: {0}", hasTimestamp);
        while (rs.next()) {
          DocId id = new DocId(uniqueKey.makeUniqueId(rs));
          DocIdPusher.Record record =
              new DocIdPusher.Record.Builder(id).setCrawlImmediately(true)
                  .build();
          log.log(Level.FINEST, "doc id: {0}", id);
          outstream.add(record);
          
          // update latestTimestamp
          if (hasTimestamp) {
            Timestamp ts =
                rs.getTimestamp(GsaSpecialColumns.GSA_TIMESTAMP.toString(),
                    updateTimestampTimezone);
            if (ts != null) {
              if (latestTimestamp == null || ts.after(latestTimestamp)) {
                latestTimestamp = ts;
                log.log(Level.FINE, "latestTimestamp updated: {0}",
                    formatter.format(new Date(latestTimestamp.getTime())));
              }
            }
          }
        }
      } catch (SQLException ex) {
        throw new IOException(ex);
      } finally {
        tryClosingStatementAndResult(statementAndResult);
        tryClosingConnection(conn);
      }
      outstream.forcePush();
      if (!hasTimestamp) {
        lastUpdateTimestamp = new Timestamp(System.currentTimeMillis());
      } else if (latestTimestamp != null) {
        lastUpdateTimestamp = latestTimestamp;
      }
      // The Timestamp here will be printed in database timezone.
      log.fine("last pushing timestamp set to: "
          + formatter.format(new Date(lastUpdateTimestamp.getTime())));
    }

    private StatementAndResult getUpdateStreamFromDb(Connection conn)
        throws SQLException {
      PreparedStatement st;
      if (disableStreaming) {
        st = conn.prepareStatement(updateSql);
      } else {
        st = conn.prepareStatement(updateSql,
            /* 1st streaming flag */ java.sql.ResultSet.TYPE_FORWARD_ONLY,
            /* 2nd streaming flag */ java.sql.ResultSet.CONCUR_READ_ONLY);
      }
      st.setTimestamp(1, lastUpdateTimestamp, updateTimestampTimezone);
      ResultSet rs = st.executeQuery();
      return new StatementAndResult(st, rs);
    }
  }

  private DbAdaptorIncrementalLister initDbAdaptorIncrementalLister(
      Config config) {
    String updateSql = config.getValue("db.updateSql");
    if (!isNullOrEmptyString(updateSql)) {
      return new DbAdaptorIncrementalLister(updateSql);
    } else {
      return null;
    }
  }
  
  @VisibleForTesting
  enum GsaSpecialColumns {
    GSA_PERMIT_USERS("GSA_PERMIT_USERS"),
    GSA_DENY_USERS("GSA_DENY_USERS"),
    GSA_PERMIT_GROUPS("GSA_PERMIT_GROUPS"),
    GSA_DENY_GROUPS("GSA_DENY_GROUPS"),
    GSA_TIMESTAMP("GSA_TIMESTAMP")
    ;

    private final String text;

    private GsaSpecialColumns(final String text) {
      this.text = text;
    }

    @Override
    public String toString() {
      return text;
    }
  }
}
