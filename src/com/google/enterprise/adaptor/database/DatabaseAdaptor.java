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

import static java.util.Locale.US;

import com.google.common.annotations.VisibleForTesting;
import com.google.enterprise.adaptor.AbstractAdaptor;
import com.google.enterprise.adaptor.Acl;
import com.google.enterprise.adaptor.AdaptorContext;
import com.google.enterprise.adaptor.AuthnIdentity;
import com.google.enterprise.adaptor.AuthzAuthority;
import com.google.enterprise.adaptor.AuthzStatus;
import com.google.enterprise.adaptor.Config;
import com.google.enterprise.adaptor.DocId;
import com.google.enterprise.adaptor.DocIdPusher;
import com.google.enterprise.adaptor.GroupPrincipal;
import com.google.enterprise.adaptor.InvalidConfigurationException;
import com.google.enterprise.adaptor.PollingIncrementalLister;
import com.google.enterprise.adaptor.Principal;
import com.google.enterprise.adaptor.Request;
import com.google.enterprise.adaptor.Response;
import com.google.enterprise.adaptor.StartupException;
import com.google.enterprise.adaptor.UserPrincipal;

import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

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
  private String actionColumn;
  private MetadataColumns metadataColumns;
  private ResponseGenerator respGenerator;
  private String aclSql;
  private String aclPrincipalDelimiter;
  private String aclNamespace;
  private boolean disableStreaming;
  private boolean encodeDocId;
  private String modeOfOperation;

  @Override
  public void initConfig(Config config) {
    config.addKey("db.driverClass", null);
    config.addKey("db.url", null);
    config.addKey("db.user", null);
    config.addKey("db.password", null);
    config.addKey("db.uniqueKey", null);
    config.addKey("db.everyDocIdSql", null);
    config.addKey("db.singleDocContentSql", "");
    config.addKey("db.singleDocContentSqlParameters", "");
    // column that contains either "add" or "delete" action.
    config.addKey("db.actionColumn", "");
    config.addKey("db.metadataColumns", "");
    // when set to true, if "db.metadataColumns" is blank, it will use all
    // returned columns as metadata.
    config.addKey("db.includeAllColumnsAsMetadata", "false");
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
    // Different values should be picked for this config based on the database
    // the adaptor is connecting to and based on the column type used as
    // timestamp.
    //
    // For MS SQL Server,
    //  datetime, datetime2 : use database server's timezone.
    //  datetimeoffset : use UTC or GMT or any equivalent.
    //
    // For Oracle,
    //  DATE, TIMESTAMP, TIMESTAMP WITH TIME ZONE : use database server's
    //      timezone.
    //  TIMESTAMP WITH LOCAL TIME ZONE : NOT supported now. The call to
    //      getTimestamp will throw exception. This is Oracle specific thing.
    //      To fix, we need to call
    //      oracle.jdbc.driver.OracleConnection.setSessionTimeZone().
    //
    // See java.util.TimeZone javadoc for valid values for TimeZone.
    // http://docs.oracle.com/javase/7/docs/api/java/util/TimeZone.html
    config.addKey("db.updateTimestampTimezone", "");
    config.addKey("adaptor.namespace", Principal.DEFAULT_NAMESPACE);
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

    boolean leaveIdAlone = new Boolean(cfg.getValue("docId.isUrl"));
    encodeDocId = !leaveIdAlone;
    log.config("encodeDocId: " + encodeDocId);

    uniqueKey = new UniqueKey(
        cfg.getValue("db.uniqueKey"),
        cfg.getValue("db.singleDocContentSqlParameters"),
        cfg.getValue("db.aclSqlParameters"),
        encodeDocId
    );
    log.config("primary key: " + uniqueKey);

    everyDocIdSql = cfg.getValue("db.everyDocIdSql");
    log.config("every doc id sql: " + everyDocIdSql);

    singleDocContentSql = cfg.getValue("db.singleDocContentSql");
    log.config("single doc content sql: " + singleDocContentSql);

    actionColumn = cfg.getValue("db.actionColumn");
    log.config("action column: " + actionColumn);

    Boolean includeAllColumnsAsMetadata = new Boolean(cfg.getValue(
        "db.includeAllColumnsAsMetadata"));
    log.config("include all columns as metadata: "
        + includeAllColumnsAsMetadata);

    String metadataColumnsConfig = cfg.getValue("db.metadataColumns");
    if (includeAllColumnsAsMetadata && "".equals(metadataColumnsConfig)) {
      metadataColumns = null;
      log.config("metadata columns: Use all columns in ResultSet");
    } else {
      metadataColumns = new MetadataColumns(metadataColumnsConfig);
      log.config("metadata columns: " + metadataColumns);
    }

    modeOfOperation = cfg.getValue("db.modeOfOperation");
    if (modeOfOperation.equals("urlAndMetadataLister") && encodeDocId) {
      String errmsg = "db.modeOfOperation of \"" + modeOfOperation
          + "\" requires docId.isUrl to be \"true\"";
      throw new InvalidConfigurationException(errmsg);
    }

    if (leaveIdAlone) {
      log.config("adaptor runs in lister-only mode");

      // Warn about ignored properties.
      TreeSet<String> ignored = new TreeSet<>();
      if (!singleDocContentSql.isEmpty()) {
        ignored.add("db.singleDocContentSql");
      }
      if (!modeOfOperation.equals("urlAndMetadataLister")) {
        if (metadataColumns == null) {
          ignored.add("db.includeAllColumnsAsMetadata");
        } else if (!metadataColumns.isEmpty()) {
          ignored.add("db.metadataColumns");
        }
      }
      if (!ignored.isEmpty()) {
        String modeStr = (modeOfOperation.equals("urlAndMetadataLister"))
            ? modeOfOperation : "lister-only";
        log.log(Level.INFO, "The following properties are set but will"
            + " be ignored in {0} mode: {1}",
            new Object[] { modeStr, ignored });
      }
    } else if (singleDocContentSql.isEmpty()) {
      throw new InvalidConfigurationException(
          "db.singleDocContentSql cannot be an empty string");
    }

    respGenerator = loadResponseGenerator(cfg);
    
    if (!isNullOrEmptyString(cfg.getValue("db.aclSql"))) {
      aclSql = cfg.getValue("db.aclSql");
      log.config("acl sql: " + aclSql); 
      aclPrincipalDelimiter = cfg.getValue("db.aclPrincipalDelimiter");
      log.config("aclPrincipalDelimiter: '" + aclPrincipalDelimiter + "'");
    }

    disableStreaming = new Boolean(cfg.getValue("db.disableStreaming"));
    log.config("disableStreaming: " + disableStreaming);

    DbAdaptorIncrementalLister incrementalLister
        = initDbAdaptorIncrementalLister(cfg);
    if (incrementalLister != null) {
      context.setPollingIncrementalLister(incrementalLister);
    }

    if (aclSql == null) {
      context.setAuthzAuthority(new AllPublic());
    } else {
      context.setAuthzAuthority(new AccessChecker());
    }
  
    aclNamespace = cfg.getValue("adaptor.namespace");
    log.config("namespace: " + aclNamespace);

    // Verify all column names.
    try (Connection conn = makeNewConnection()) {
      verifyColumnNames(conn, "db.everyDocIdSql", everyDocIdSql,
          "db.uniqueKey", uniqueKey.getDocIdSqlColumns());
      verifyColumnNames(conn, "db.singleDocContentSql", singleDocContentSql,
          "db.singleDocContentSqlParameters", uniqueKey.getContentSqlColumns());
      verifyColumnNames(conn, "db.aclSql", aclSql,
          "db.aclSqlParameters", uniqueKey.getAclSqlColumns());
      if (!actionColumn.isEmpty()) {
        verifyColumnNames(conn, "db.everyDocIdSql", everyDocIdSql,
            "db.actionColumn", Arrays.asList(actionColumn));
      }
      if (metadataColumns != null) {
        if ("urlAndMetadataLister".equals(modeOfOperation)) {
          verifyColumnNames(conn, "db.everyDocIdSql", everyDocIdSql,
              "db.metadataColumns", metadataColumns.keySet());
        } else {
          verifyColumnNames(conn, "db.singleDocContentSql", singleDocContentSql,
              "db.metadataColumns", metadataColumns.keySet());
        }
      }
      if (respGenerator instanceof ResponseGenerator.SingleColumnContent) {
        ResponseGenerator.SingleColumnContent content =
            (ResponseGenerator.SingleColumnContent) respGenerator;
        verifyColumnNames(conn, "db.singleDocContentSql", singleDocContentSql,
            "db.modeOfOperation." + modeOfOperation + ".columnName",
            Arrays.asList(content.getContentColumnName()));
      }
    } catch (SQLException e) {
      log.log(Level.WARNING, "Unable to validate configured column names");
    }
  }

  /**
   * Verifies that the given column names exist in the query
   * ResultSet. The check is case-insensitive, so in some cases the
   * column names may fail at runtime.
   *
   * <p>Database errors are not fatal, this is a best effort only.
   * This method logs any SQLExceptions that are thrown, but does not
   * throw them.
   *
   * @param sqlConfig the configuration property for the SQL query
   * @param sql the SQL query; the query is prepared, but not executed
   * @param columnConfig the configuration property for the column names
   * @param columnNames the column names to verify
   * @throws InvalidConfigurationException if any of the columns are not found
   */
  @VisibleForTesting
  static void verifyColumnNames(Connection conn, String sqlConfig, String sql,
      String columnConfig, Collection<String> columnNames) {
    if (isNullOrEmptyString(sql)) {
      log.log(Level.FINEST,
          "Skipping validation of empty query {0}", sqlConfig);
      return;
    }
    if (columnNames.isEmpty()) {
      return;
    }
    log.log(Level.FINEST, "Looking for columns {0} in {1}",
        new Object[] { columnNames, sqlConfig });

    // Create a map from case-insensitive names to the originals.
    TreeMap<String, String> targets =
        new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    for (String name : columnNames) {
      if (name.isEmpty()) {
        throw new InvalidConfigurationException("One or more column names from "
            + columnConfig + " are empty: " + columnNames);
      }
      targets.put(name, name);
    }

    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
      ResultSetMetaData rsmd = stmt.getMetaData();
      if (rsmd == null) {
        throw new SQLException("ResultSetMetaData is not available");
      }
      for (int i = 1; i <= rsmd.getColumnCount(); i++) {
        String actual = rsmd.getColumnLabel(i);
        String match = targets.get(actual);
        if (match != null) {
          log.log(Level.FINEST,
              "Matched column \"{0}\" as \"{1}\" in query {2}",
              new Object[] { match, actual, sqlConfig });
          targets.remove(match);
        }
      }
      if (!targets.isEmpty()) {
        throw new InvalidConfigurationException("These columns from "
            + columnConfig + " " + targets.keySet() + " not found in query "
            + sqlConfig + ": " + sql);
      }
    } catch (SQLException e) {
      // Throw if this is a SQL syntax error (SQL state 42xxx).
      String sqlState = e.getSQLState();
      if (sqlState != null && sqlState.startsWith("42")) {
        throw new InvalidConfigurationException(
            "Syntax error in query " + sqlConfig, e);
      }
      log.log(Level.WARNING,
          "Unable to validate configured column names for query {0}: {1}",
          new Object[] { sqlConfig, e });
    }
  }

  /** Get all doc ids from database. */
  @Override
  public void getDocIds(DocIdPusher pusher) throws IOException,
      InterruptedException {
    BufferedPusher outstream = new BufferedPusher(pusher);
    try (Connection conn = makeNewConnection();
        PreparedStatement stmt = getStreamFromDb(conn, everyDocIdSql);
        ResultSet rs = stmt.executeQuery()) {
      log.finer("queried for stream");
      while (rs.next()) {
        DocId id = new DocId(uniqueKey.makeUniqueId(rs, encodeDocId));
        DocIdPusher.Record.Builder builder = new DocIdPusher.Record.Builder(id);
        if (isDeleteAction(rs)) {
          builder.setDeleteFromIndex(true);
        } else if ("urlAndMetadataLister".equals(modeOfOperation)) {
          addMetadataToRecordBuilder(builder, rs);
        }
        DocIdPusher.Record record = builder.build();
        log.log(Level.FINEST, "doc id: {0}", id);
        outstream.add(record);
      }
    } catch (SQLException ex) {
      throw new IOException(ex);
    }
    outstream.forcePush();
  }

  private boolean isDeleteAction(ResultSet rs) throws SQLException {
    if (!actionColumn.equals("")) {
      String action = rs.getString(actionColumn);
      return (action != null && "delete".equals(action.toLowerCase(US)));
    }
    return false;
  }

  private interface MetadataHandler {
    void addMetadata(String k, String v);
  }

  /*
   * Adds all specified metadata columns to the record or response being built.
   */
  private void addMetadata(MetadataHandler meta, ResultSet rs)
      throws SQLException, IOException {
    ResultSetMetaData rsMetaData = rs.getMetaData();
    synchronized (this) {
      if (metadataColumns == null) {
        metadataColumns = new MetadataColumns(rsMetaData);
      }
    }
    for (Map.Entry<String, String> entry : metadataColumns.entrySet()) {
      int index;
      try {
        index = rs.findColumn(entry.getKey());
      } catch (SQLException e) {
        log.log(Level.WARNING, "Skipping metadata column ''{0}'': {1}.",
            new Object[] { entry.getKey(), e.getMessage() });
        continue;
      }
      int columnType = rsMetaData.getColumnType(index);
      log.log(Level.FINEST, "Column name: {0}, Type: {1}", new Object[] {
          entry.getKey(), columnType});

      Object value = null;
      switch (columnType) {
        case Types.CLOB:
          Clob clob = rs.getClob(index);
          if (clob != null) {
            try {
              value = clob.getSubString(1, 4096);
            } finally {
              try {
                clob.free();
              } catch (Exception e) {
                log.log(Level.FINEST, "Error closing CLOB", e);
              }
            }
          }
          break;
        case Types.NCLOB:
          NClob nclob = rs.getNClob(index);
          if (nclob != null) {
            try {
              value = nclob.getSubString(1, 4096);
            } finally {
              try {
                nclob.free();
              } catch (Exception e) {
                log.log(Level.FINEST, "Error closing NCLOB", e);
              }
            }
          }
          break;
        case Types.LONGVARCHAR:
          try (Reader reader = rs.getCharacterStream(index)) {
            if (reader != null) {
              char[] buffer = new char[4096];
              int len;
              if ((len = reader.read(buffer)) != -1) {
                value = new String(buffer, 0, len);
              }
            }
          }
          break;
        case Types.LONGNVARCHAR:
          try (Reader reader = rs.getNCharacterStream(index)) {
            if (reader != null) {
              char[] buffer = new char[4096];
              int len;
              if ((len = reader.read(buffer)) != -1) {
                value = new String(buffer, 0, len);
              }
            }
          }
          break;
        case Types.DATE:
          java.sql.Date dt = rs.getDate(index);
          if (dt != null) {
            value = dt.toString();
          }
          break;
        case Types.TIME:
          Time tm = rs.getTime(index);
          if (tm != null) {
            value = tm.toString();
          }
          break;
        case Types.TIMESTAMP:
          Timestamp ts = rs.getTimestamp(index);
          if (ts != null) {
            value = ts.toString();
          }
          break;
        default:
          value = rs.getObject(index);
          break;
      }
      meta.addMetadata(entry.getValue(), "" + value);
    }
  }

  @VisibleForTesting
  void addMetadataToRecordBuilder(final DocIdPusher.Record.Builder builder,
      ResultSet rs) throws SQLException, IOException {
    addMetadata(
        new MetadataHandler() {
          @Override public void addMetadata(String k, String v) {
            builder.addMetadata(k, v);
          }
        },
        rs);
  }

  @VisibleForTesting
  void addMetadataToResponse(final Response resp, ResultSet rs)
      throws SQLException, IOException {
    addMetadata(
        new MetadataHandler() {
          @Override public void addMetadata(String k, String v) {
            resp.addMetadata(k, v);
          }
        },
        rs);
  }

  /** Gives the bytes of a document referenced with id. */
  @Override
  public void getDocContent(Request req, Response resp) throws IOException {
    if (!encodeDocId) {
      // adaptor operating in lister-only mode 
      resp.respondNotFound();
      return;
    }
    DocId id = req.getDocId();
    try (Connection conn = makeNewConnection();
        PreparedStatement stmt = getDocFromDb(conn, id.getUniqueId());
        ResultSet rs = stmt.executeQuery()) {
      log.finer("got doc");
      // First handle cases with no data to return.
      boolean hasResult = rs.next();
      if (!hasResult) {
        resp.respondNotFound();
        return;
      }
      // Generate response metadata first.
      addMetadataToResponse(resp, rs);
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
    }
  }

  public static void main(String[] args) {
    AbstractAdaptor.main(new DatabaseAdaptor(), args);
  }
  
  private Acl getAcl(Connection conn, String uniqueId) throws SQLException {
    try (PreparedStatement stmt = getAclFromDb(conn, uniqueId);
        ResultSet rs = stmt.executeQuery()) {
      log.finer("got acl");
      return buildAcl(rs, aclPrincipalDelimiter, aclNamespace);
    }
  }
  
  @VisibleForTesting
  static Acl buildAcl(ResultSet rs, String delim, String namespace)
      throws SQLException {
    boolean hasResult = rs.next();
    if (!hasResult) {
      // empty Acl ensures adaptor will mark this document as secure
      return Acl.EMPTY;
    }
    ResultSetMetaData metadata = rs.getMetaData();
    Acl.Builder builder = new Acl.Builder();
    ArrayList<UserPrincipal> permitUsers = new ArrayList<UserPrincipal>();
    ArrayList<UserPrincipal> denyUsers = new ArrayList<UserPrincipal>();
    ArrayList<GroupPrincipal> permitGroups = new ArrayList<GroupPrincipal>();
    ArrayList<GroupPrincipal> denyGroups = new ArrayList<GroupPrincipal>();
    boolean hasPermitUsers =
        hasColumn(metadata, GsaSpecialColumns.GSA_PERMIT_USERS);
    boolean hasDenyUsers =
        hasColumn(metadata, GsaSpecialColumns.GSA_DENY_USERS);
    boolean hasPermitGroups =
        hasColumn(metadata, GsaSpecialColumns.GSA_PERMIT_GROUPS);
    boolean hasDenyGroups =
        hasColumn(metadata, GsaSpecialColumns.GSA_DENY_GROUPS);
    do {
      if (hasPermitUsers) {
        permitUsers.addAll(getUserPrincipalsFromResultSet(rs,
            GsaSpecialColumns.GSA_PERMIT_USERS, delim, namespace));
      }
      if (hasDenyUsers) {
        denyUsers.addAll(getUserPrincipalsFromResultSet(rs,
            GsaSpecialColumns.GSA_DENY_USERS, delim, namespace));
      }
      if (hasPermitGroups) {
        permitGroups.addAll(getGroupPrincipalsFromResultSet(rs,
            GsaSpecialColumns.GSA_PERMIT_GROUPS, delim, namespace));
      }
      if (hasDenyGroups) {
        denyGroups.addAll(getGroupPrincipalsFromResultSet(rs,
            GsaSpecialColumns.GSA_DENY_GROUPS, delim, namespace));
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
      GsaSpecialColumns column, String delim, String namespace)
      throws SQLException {
    ArrayList<UserPrincipal> principals = new ArrayList<UserPrincipal>();
    String value = rs.getString(column.toString());
    if (!isNullOrEmptyString(value)) {
      if ("".equals(delim)) {
        principals.add(new UserPrincipal(value.trim(), namespace));
      } else {
        // drop trailing empties
        String principalNames[] = value.split(delim, 0);
        for (String principalName : principalNames) {
          principals.add(new UserPrincipal(principalName.trim(), namespace));
        }
      }
    }
    return principals;
  }

  @VisibleForTesting
  static ArrayList<GroupPrincipal> getGroupPrincipalsFromResultSet(ResultSet rs,
      GsaSpecialColumns column, String delim, String namespace) throws SQLException {
    ArrayList<GroupPrincipal> principals = new ArrayList<GroupPrincipal>();
    String value = rs.getString(column.toString());
    if (!isNullOrEmptyString(value)) {
      if ("".equals(delim)) {
        principals.add(new GroupPrincipal(value.trim(), namespace));
      } else {
        // drop trailing empties
        String principalNames[] = value.split(delim, 0);
        for (String principalName : principalNames) {
          principals.add(new GroupPrincipal(principalName.trim(), namespace));
        }
      }
    }
    return principals;
  }
  
  @VisibleForTesting
  static boolean hasColumn(ResultSetMetaData metadata, GsaSpecialColumns column)
      throws SQLException {
    int columns = metadata.getColumnCount();
    for (int x = 1; x <= columns; x++) {
      if (column.toString().equalsIgnoreCase(metadata.getColumnLabel(x))) {
        return true;
      }
    }
    return false;
  }

  private Connection makeNewConnection() throws SQLException {
    log.finest("about to connect");
    Connection conn = DriverManager.getConnection(dbUrl, user, password);
    log.finest("connected");
    return conn;
  }

  private PreparedStatement getDocFromDb(Connection conn,
      String uniqueId) throws SQLException {
    PreparedStatement st = conn.prepareStatement(singleDocContentSql);
    uniqueKey.setContentSqlValues(st, uniqueId);  
    log.log(Level.FINER, "about to get doc: {0}",  uniqueId);
    return st;
  }

  private PreparedStatement getAclFromDb(Connection conn,
      String uniqueId) throws SQLException {
    PreparedStatement st = conn.prepareStatement(aclSql);
    uniqueKey.setAclSqlValues(st, uniqueId);  
    log.log(Level.FINER, "about to get acl: {0}",  uniqueId);
    return st;
  }

  private PreparedStatement getStreamFromDb(Connection conn,
      String query) throws SQLException {
    PreparedStatement st;
    if (disableStreaming) {
      st = conn.prepareStatement(query);
    } else {
      st = conn.prepareStatement(query,
          /* 1st streaming flag */ java.sql.ResultSet.TYPE_FORWARD_ONLY,
          /* 2nd streaming flag */ java.sql.ResultSet.CONCUR_READ_ONLY);
    }
    st.setFetchSize(maxIdsPerFeedFile);  // Integer.MIN_VALUE for MySQL?
    log.log(Level.FINER, "about to query for stream: {0}", query);
    return st;
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
      String errmsg = "modeOfOperation cannot be an empty string";
      throw new InvalidConfigurationException(errmsg);
    }
    int sepIndex = mode.lastIndexOf(".");
    if (sepIndex == -1) {
      log.fine("about to look for " + mode + " in ResponseGenerator");
      try {
        Method method =
            ResponseGenerator.class.getDeclaredMethod(mode, Map.class);
        return loadResponseGeneratorInternal(method,
            config.getValuesWithPrefix("db.modeOfOperation." + mode + "."));
      } catch (NoSuchMethodException ex) {
        throw new InvalidConfigurationException(mode
            + " is not a valid built-in modeOfOperation."
            + " Supported modes are: " + getResponseGeneratorMethods());
      }
    }
    log.fine("about to try " + mode + " as a fully qualified method name");
    String className = mode.substring(0, sepIndex);
    String methodName = mode.substring(sepIndex + 1);
    log.log(Level.FINE, "Split {0} into class {1} and method {2}",
        new Object[] {mode, className, methodName});
    try {
      Class<?> klass = Class.forName(className);
      Method method = klass.getDeclaredMethod(methodName, Map.class);
      return loadResponseGeneratorInternal(method,
          config.getValuesWithPrefix("db.modeOfOperation." + mode + "."));
    } catch (ClassNotFoundException ex) {
      String errmsg = "No class " + className + " found";
      throw new InvalidConfigurationException(errmsg, ex);
    } catch (NoSuchMethodException ex) {
      String errmsg = "No method " + methodName + " found for class "
          + className;
      throw new InvalidConfigurationException(errmsg, ex);
    }
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
        | NullPointerException e) {
      throw new InvalidConfigurationException(e);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof StartupException) {
        throw (StartupException) cause;
      } else {
        throw new InvalidConfigurationException(cause);
      }
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

  /** Gets the valid modeOfOperation method names from ResponseGenerator. */
  private static String getResponseGeneratorMethods() {
    TreeSet<String> methods = new TreeSet<>();
    for (Method method : ResponseGenerator.class.getDeclaredMethods()) {
      int modifiers = method.getModifiers();
      Class<?>[] parameterTypes = method.getParameterTypes();
      if ((modifiers & Modifier.PRIVATE) != Modifier.PRIVATE
          && (modifiers & Modifier.STATIC) == Modifier.STATIC
          && parameterTypes.length == 1
          && parameterTypes[0].equals(Map.class)
          && ResponseGenerator.class.isAssignableFrom(method.getReturnType())) {
        methods.add(method.getName());
      }
    }
    return methods.toString();
  }

  // Incremental pushing in Database Adaptor based on Timestamp does NOT
  // guarantee to pick ALL updates. Some updates might still need to wait for 
  // next full push to be sent to GSA.
  private class DbAdaptorIncrementalLister implements PollingIncrementalLister {
    private final String updateSql;
    private Calendar updateTimestampTimezone;
    private Timestamp lastUpdateTimestamp;
    private final DateFormat formatter;

    public DbAdaptorIncrementalLister(String updateSql, Calendar updateTsTz) {
      this.updateSql = updateSql;
      this.updateTimestampTimezone = updateTsTz;
      log.config("update sql: " + this.updateSql);
      this.lastUpdateTimestamp = new Timestamp(System.currentTimeMillis());
      formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z");
      formatter.setTimeZone(updateTimestampTimezone.getTimeZone());
    }

    @Override
    public void getModifiedDocIds(DocIdPusher pusher)
        throws IOException, InterruptedException {
      BufferedPusher outstream = new BufferedPusher(pusher);
      // latestTimestamp will be used to update lastUpdateTimestampInMillis
      // if GSA_TIMESTAMP column is present in the ResultSet and there is at 
      // least one non-null value of that column in the ResultSet.
      Timestamp latestTimestamp = null;
      boolean hasTimestamp = false;
      try (Connection conn = makeNewConnection();
          PreparedStatement stmt = getUpdateStreamFromDb(conn);
          ResultSet rs = stmt.executeQuery()) {
        hasTimestamp =
            hasColumn(rs.getMetaData(), GsaSpecialColumns.GSA_TIMESTAMP);
        log.log(Level.FINEST, "hasTimestamp: {0}", hasTimestamp);
        while (rs.next()) {
          DocId id = new DocId(uniqueKey.makeUniqueId(rs, encodeDocId));
          DocIdPusher.Record.Builder builder =
              new DocIdPusher.Record.Builder(id).setCrawlImmediately(true);
          if ("urlAndMetadataLister".equals(modeOfOperation)) {
            addMetadataToRecordBuilder(builder, rs);
          }
          DocIdPusher.Record record = builder.build();
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

    private PreparedStatement getUpdateStreamFromDb(Connection conn)
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
      return st;
    }
  }

  private DbAdaptorIncrementalLister initDbAdaptorIncrementalLister(
      Config config) {
    String tzString = config.getValue("db.updateTimestampTimezone");
    final Calendar updateTimestampTimezone;
    if (isNullOrEmptyString(tzString)) {
      updateTimestampTimezone = Calendar.getInstance();
    } else {
      updateTimestampTimezone =
          Calendar.getInstance(TimeZone.getTimeZone(tzString));
    }
    log.config("updateTimestampTimezone: "
        + updateTimestampTimezone.getTimeZone().getDisplayName());
    String updateSql = config.getValue("db.updateSql");
    if (!isNullOrEmptyString(updateSql)) {
      return new DbAdaptorIncrementalLister(updateSql, updateTimestampTimezone);
    } else {
      return null;
    }
  }
  
  @VisibleForTesting
  enum GsaSpecialColumns {
    GSA_PERMIT_USERS,
    GSA_DENY_USERS,
    GSA_PERMIT_GROUPS,
    GSA_DENY_GROUPS,
    GSA_TIMESTAMP;
  }

  private static class AllPublic implements AuthzAuthority {
    public Map<DocId, AuthzStatus> isUserAuthorized(AuthnIdentity userIdentity,
        Collection<DocId> ids) throws IOException {
      Map<DocId, AuthzStatus> result =
          new HashMap<DocId, AuthzStatus>(ids.size() * 2);
      for (DocId docId : ids) {
        result.put(docId, AuthzStatus.PERMIT);
      }
      return Collections.unmodifiableMap(result);
    }
  }

  private static Map<DocId, AuthzStatus> allDeny(Collection<DocId> ids) {
    Map<DocId, AuthzStatus> result
        = new HashMap<DocId, AuthzStatus>(ids.size() * 2);
    for (DocId id : ids) {
      result.put(id, AuthzStatus.DENY); 
    }
    return Collections.unmodifiableMap(result);  
  }

  private class AccessChecker implements AuthzAuthority {
    public Map<DocId, AuthzStatus> isUserAuthorized(AuthnIdentity userIdentity,
        Collection<DocId> ids) throws IOException {
     if (null == userIdentity) {
        log.info("null identity to authorize");
        return allDeny(ids);  // TODO: consider way to permit public
      }
      UserPrincipal user = userIdentity.getUser();
      if (null == user) {
        log.info("null user to authorize");
        return allDeny(ids);  // TODO: consider way to permit public
      }
      log.log(Level.INFO, "about to authorize {0} {1}",
          new Object[]{user, userIdentity.getGroups()});
      Map<DocId, AuthzStatus> result
          = new HashMap<DocId, AuthzStatus>(ids.size() * 2);
      try (Connection conn = makeNewConnection()) {
        for (DocId id : ids) {
          log.log(Level.FINE, "about to get acl of doc {0}", id);
          Acl acl = getAcl(conn, id.getUniqueId());
          List<Acl> aclChain = Arrays.asList(acl);
          log.log(Level.FINE,
              "about to authorize user {0} for doc {1} and acl {2}",
              new Object[]{user, id, acl});
          AuthzStatus decision = Acl.isAuthorized(userIdentity, aclChain); 
          log.log(Level.FINE,
              "authorization decision {0} for user {1} and doc {2}",
              new Object[]{decision, user, id});
          result.put(id, decision);
        }
      } catch (SQLException ex) {
        throw new IOException("authz retrieval error", ex);
      }
      return Collections.unmodifiableMap(result);
    }
  }
}
