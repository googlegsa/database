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

import static com.google.enterprise.adaptor.DocIdPusher.Record;
import static com.google.enterprise.adaptor.Principal.DEFAULT_NAMESPACE;
import static com.google.enterprise.adaptor.database.JdbcFixture.executeQuery;
import static com.google.enterprise.adaptor.database.JdbcFixture.executeQueryAndNext;
import static com.google.enterprise.adaptor.database.JdbcFixture.executeUpdate;
import static com.google.enterprise.adaptor.database.JdbcFixture.getConnection;
import static com.google.enterprise.adaptor.database.Logging.captureLogMessages;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.US;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.enterprise.adaptor.Acl;
import com.google.enterprise.adaptor.AuthnIdentity;
import com.google.enterprise.adaptor.AuthzAuthority;
import com.google.enterprise.adaptor.AuthzStatus;
import com.google.enterprise.adaptor.Config;
import com.google.enterprise.adaptor.DocId;
import com.google.enterprise.adaptor.GroupPrincipal;
import com.google.enterprise.adaptor.InvalidConfigurationException;
import com.google.enterprise.adaptor.Metadata;
import com.google.enterprise.adaptor.PollingIncrementalLister;
import com.google.enterprise.adaptor.Response;
import com.google.enterprise.adaptor.StartupException;
import com.google.enterprise.adaptor.TestHelper;
import com.google.enterprise.adaptor.TestHelper.RecordingContext;
import com.google.enterprise.adaptor.UserPrincipal;
import com.google.enterprise.adaptor.database.DatabaseAdaptor.GsaSpecialColumns;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

/** Test cases for {@link DatabaseAdaptor}. */
public class DatabaseAdaptorTest {
  // Class order:
  //     JUnit helpers
  //     Tests of static DatabaseAdaptor methods
  //     getObjectUnderTest
  //     Tests that use getObjectUnderTest
  //         init
  //         getDocIds
  //         getModifiedDocIds
  //         getDocContent
  //         isUserAuthorized
  //
  // Test names: test<Method><Feature>_<testCase>

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @After
  public void dropAllObjects() throws SQLException {
    JdbcFixture.dropAllObjects();
  }

  @Test
  public void testVerifyColumnNames_found() throws Exception {
    executeUpdate("create table data(id int, other varchar)");
    DatabaseAdaptor.verifyColumnNames(getConnection(),
        "found", "select * from data", "found", Arrays.asList("id", "other"));
  }

  @Test
  public void testVerifyColumnNames_missing() throws Exception {
    executeUpdate("create table data(id int, other varchar)");
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("[foo] not found in query");
    DatabaseAdaptor.verifyColumnNames(getConnection(),
        "found", "select * from data", "found", Arrays.asList("id", "foo"));
  }

  @Test
  public void testVerifyColumnNames_lowercase() throws Exception {
    executeUpdate("create table data(id int, \"lower\" varchar)");
    DatabaseAdaptor.verifyColumnNames(getConnection(),
        "found", "select * from data", "found", Arrays.asList("LOWER"));
  }

  @Test
  public void testVerifyColumnNames_sqlException() throws Exception {
    executeUpdate("create table data(id int, other varchar)");
    Connection conn = getConnection();
    conn.close();
    DatabaseAdaptor.verifyColumnNames(conn,
        "found", "select * from data", "found", Arrays.asList("id", "other"));
  }

  @Test
  public void testVerifyColumnNames_syntaxError() throws Exception {
    executeUpdate("create table data(id int, other varchar)");
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("Syntax error in query");
    DatabaseAdaptor.verifyColumnNames(getConnection(),
        "found", "select from data", "found", Arrays.asList("id", "other"));
  }

  @Test
  public void testVerifyColumnNames_nullMetaData() throws Exception {
    executeUpdate("create table data(id int, other varchar)");
    DatabaseAdaptor.verifyColumnNames(getConnection(),
        "found", "insert into data(id) values(42)",
        "found", Arrays.asList("id", "other"));
  }

  @Test
  public void testLoadResponseGenerator_empty() {
    Config config = new Config();
    config.addKey("db.modeOfOperation", "");
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("modeOfOperation cannot be an empty string");
    DatabaseAdaptor.loadResponseGenerator(config);
  }

  @Test
  public void testLoadResponseGeneratorWithFactoryMethod() {
    Config config = new Config();
    config.addKey("db.modeOfOperation", "rowToText");
    assertNotNull("loaded response generator is null",
        DatabaseAdaptor.loadResponseGenerator(config));
  }

  @Test
  public void testLoadResponseGeneratorWithBuiltinFullyQualifiedMethod() {
    Config config = new Config();
    String modeOfOperation = ResponseGenerator.class.getName() + ".rowToText";
    config.addKey("db.modeOfOperation", modeOfOperation);
    assertNotNull("loaded response generator is null",
        DatabaseAdaptor.loadResponseGenerator(config));
  }

  private static class DummyResponseGenerator extends ResponseGenerator {
    public DummyResponseGenerator(Map<String, String> config) {
      super(config);
    }
    @Override
    public void generateResponse(ResultSet rs, Response resp) {
    }
  }

  static ResponseGenerator createDummy(Map<String, String> config) {
    return new DummyResponseGenerator(config);
  }

  @Test
  public void testLoadResponseGeneratorWithCustomFullyQualifiedMethod() {
    Config config = new Config();
    String modeOfOperation = getClass().getName() + ".createDummy";
    config.addKey("db.modeOfOperation", modeOfOperation);
    assertNotNull("loaded response generator is null",
        DatabaseAdaptor.loadResponseGenerator(config));
  }

  @Test
  public void testLoadResponseGeneratorModeOfOpNoSuchFactoryMethod() {
    Config config = new Config();
    config.addKey("db.modeOfOperation", "noThisMethod");
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(
        "noThisMethod is not a valid built-in modeOfOperation");
    DatabaseAdaptor.loadResponseGenerator(config);
  }

  @Test
  public void testLoadResponseGeneratorModeOfOpNoSuchClassMethod() {
    Config config = new Config();
    String modeOfOperation = getClass().getName() + ".noThisMode";
    config.addKey("db.modeOfOperation", modeOfOperation);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("No method noThisMode found for class");
    DatabaseAdaptor.loadResponseGenerator(config);
  }

  @Test
  public void testLoadResponseGeneratorModeOfOpNoSuchClass() {
    Config config = new Config();
    config.addKey("db.modeOfOperation", "noThisClass.noThisMethod");
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("No class noThisClass found");
    DatabaseAdaptor.loadResponseGenerator(config);
  }

  ResponseGenerator instanceDummy(Map<String, String> config) {
    return new DummyResponseGenerator(config);
  }

  @Test
  public void testLoadResponseGenerator_instanceMethod() {
    String modeOfOperation = getClass().getName() + ".instanceDummy";
    Config config = new Config();
    config.addKey("db.modeOfOperation", modeOfOperation);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectCause(isA(NullPointerException.class));
    DatabaseAdaptor.loadResponseGenerator(config);
  }

  private ResponseGenerator privateDummy(Map<String, String> config) {
    return new DummyResponseGenerator(config);
  }

  @Test
  public void testLoadResponseGenerator_inaccessibleMethod() {
    String modeOfOperation = getClass().getName() + ".privateDummy";
    Config config = new Config();
    config.addKey("db.modeOfOperation", modeOfOperation);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectCause(isA(IllegalAccessException.class));
    DatabaseAdaptor.loadResponseGenerator(config);
  }

  static void voidDummy(Map<String, String> config) {
  }

  @Test
  public void testLoadResponseGenerator_voidMethod() {
    String modeOfOperation = getClass().getName() + ".voidDummy";
    Config config = new Config();
    config.addKey("db.modeOfOperation", modeOfOperation);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("needs to return a");
    DatabaseAdaptor.loadResponseGenerator(config);
  }

  static ResponseGenerator nullDummy(Map<String, String> config) {
    return null;
  }

  @Test
  public void testLoadResponseGenerator_nullMethod() {
    String modeOfOperation = getClass().getName() + ".nullDummy";
    Config config = new Config();
    config.addKey("db.modeOfOperation", modeOfOperation);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("needs to return a");
    DatabaseAdaptor.loadResponseGenerator(config);
  }

  static ResponseGenerator startupExceptionDummy(Map<String, String> config) {
    throw new StartupException("Come on, you're not even trying");
  }

  @Test
  public void testLoadResponseGenerator_startupException() {
    String modeOfOperation = getClass().getName() + ".startupExceptionDummy";
    Config config = new Config();
    config.addKey("db.modeOfOperation", modeOfOperation);
    thrown.expect(StartupException.class);
    thrown.expectMessage("Come on, you're not even trying");
    thrown.expectCause(nullValue(Throwable.class));
    DatabaseAdaptor.loadResponseGenerator(config);
  }

  static ResponseGenerator runtimeExceptionDummy(Map<String, String> config) {
    throw new RuntimeException("Come on, you're not even trying");
  }

  @Test
  public void testLoadResponseGenerator_runtimeException() {
    String modeOfOperation = getClass().getName() + ".runtimeExceptionDummy";
    Config config = new Config();
    config.addKey("db.modeOfOperation", modeOfOperation);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectCause(isA(RuntimeException.class));
    thrown.expectMessage("Come on, you're not even trying");
    DatabaseAdaptor.loadResponseGenerator(config);
  }

  @Test
  public void testHasColumn() throws Exception {
    executeUpdate("create table acl(id int, col1 varchar, col2 varchar, "
        + "col3 timestamp)");
    String query = "select id, col1 as GSA_PERMIT_GROUPS, "
        + "col2 as \"gsa_permit_users\", col3 as \"GSA_TimeStamp\" from acl";
    ResultSet rs = executeQuery(query);
    ResultSetMetaData rsMetaData = rs.getMetaData();
    assertTrue(DatabaseAdaptor.hasColumn(rsMetaData,
        GsaSpecialColumns.GSA_PERMIT_GROUPS));
    assertTrue(DatabaseAdaptor.hasColumn(rsMetaData,
        GsaSpecialColumns.GSA_PERMIT_USERS));
    assertFalse(DatabaseAdaptor.hasColumn(rsMetaData,
        GsaSpecialColumns.GSA_DENY_GROUPS));
    assertFalse(DatabaseAdaptor.hasColumn(rsMetaData,
        GsaSpecialColumns.GSA_DENY_USERS));
    assertTrue(DatabaseAdaptor.hasColumn(rsMetaData,
        GsaSpecialColumns.GSA_TIMESTAMP));
  }

  @Test
  public void testAclSqlResultSetHasNoAclColumns() throws SQLException {
    Acl golden = Acl.EMPTY;

    executeUpdate("create table acl(id integer)");
    executeUpdate("insert into acl(id) values(1)");
    ResultSet rs = executeQuery("select * from acl");
    Acl acl = DatabaseAdaptor.buildAcl(rs, ",", DEFAULT_NAMESPACE);
    assertEquals(golden, acl);
  }

  @Test
  public void testAclSqlResultSetHasNoRecord() throws SQLException {
    Acl golden = Acl.EMPTY;

    executeUpdate("create table acl");
    ResultSet rs = executeQuery("select * from acl");
    Acl acl = DatabaseAdaptor.buildAcl(rs, ",", DEFAULT_NAMESPACE);
    assertEquals(golden, acl);
  }

  @Test
  public void testAclSqlResultSetHasOneRecord() throws SQLException {
    executeUpdate("create table acl("
        + GsaSpecialColumns.GSA_PERMIT_GROUPS + " varchar,"
        + GsaSpecialColumns.GSA_PERMIT_USERS + " varchar,"
        + GsaSpecialColumns.GSA_DENY_GROUPS + " varchar,"
        + GsaSpecialColumns.GSA_DENY_USERS + " varchar)");
    executeUpdate("insert into acl("
        + GsaSpecialColumns.GSA_PERMIT_GROUPS + ","
        + GsaSpecialColumns.GSA_PERMIT_USERS + ","
        + GsaSpecialColumns.GSA_DENY_GROUPS + ","
        + GsaSpecialColumns.GSA_DENY_USERS + ") values ("
        + "'pgroup1, pgroup2', 'puser1, puser2', "
        + "'dgroup1, dgroup2', 'duser1, duser2')");
    Acl golden = new Acl.Builder()
        .setPermitUsers(Arrays.asList(
            new UserPrincipal("puser2"),
            new UserPrincipal("puser1")))
        .setDenyUsers(Arrays.asList(
            new UserPrincipal("duser1"),
            new UserPrincipal("duser2")))
        .setPermitGroups(Arrays.asList(
            new GroupPrincipal("pgroup1"),
            new GroupPrincipal("pgroup2")))
        .setDenyGroups(Arrays.asList(
            new GroupPrincipal("dgroup2"),
            new GroupPrincipal("dgroup1")))
        .build();
    ResultSet rs = executeQuery("select * from acl");
    Acl acl = DatabaseAdaptor.buildAcl(rs, ",", DEFAULT_NAMESPACE);
    assertEquals(golden, acl);
  }

  @Test
  public void testAclSqlResultSetHasNonOverlappingTwoRecord()
      throws SQLException {
    executeUpdate("create table acl("
        + GsaSpecialColumns.GSA_PERMIT_GROUPS + " varchar,"
        + GsaSpecialColumns.GSA_PERMIT_USERS + " varchar,"
        + GsaSpecialColumns.GSA_DENY_GROUPS + " varchar,"
        + GsaSpecialColumns.GSA_DENY_USERS + " varchar)");
    executeUpdate("insert into acl("
        + GsaSpecialColumns.GSA_PERMIT_GROUPS + ","
        + GsaSpecialColumns.GSA_PERMIT_USERS + ","
        + GsaSpecialColumns.GSA_DENY_GROUPS + ","
        + GsaSpecialColumns.GSA_DENY_USERS + ") values "
        + "('pgroup1, pgroup2', 'puser1, puser2', "
        + "'dgroup1, dgroup2', 'duser1, duser2'), "
        + "('pgroup3, pgroup4', 'puser3, puser4', "
        + "'dgroup3, dgroup4', 'duser3, duser4')");
    Acl golden = new Acl.Builder()
        .setPermitUsers(Arrays.asList(
            new UserPrincipal("puser2"),
            new UserPrincipal("puser1"),
            new UserPrincipal("puser4"),
            new UserPrincipal("puser3")))
        .setDenyUsers(Arrays.asList(
            new UserPrincipal("duser2"),
            new UserPrincipal("duser1"),
            new UserPrincipal("duser4"),
            new UserPrincipal("duser3")))
        .setPermitGroups(Arrays.asList(
            new GroupPrincipal("pgroup1"),
            new GroupPrincipal("pgroup3"),
            new GroupPrincipal("pgroup4"),
            new GroupPrincipal("pgroup2")))
        .setDenyGroups(Arrays.asList(
            new GroupPrincipal("dgroup1"),
            new GroupPrincipal("dgroup3"),
            new GroupPrincipal("dgroup4"),
            new GroupPrincipal("dgroup2")))
        .build();
    ResultSet rs = executeQuery("select * from acl");
    Acl acl = DatabaseAdaptor.buildAcl(rs, ",", DEFAULT_NAMESPACE);
    assertEquals(golden, acl);
  }
  
  @Test
  public void testAclSqlResultSetHasOverlappingTwoRecord()
      throws SQLException {
    executeUpdate("create table acl("
        + GsaSpecialColumns.GSA_PERMIT_GROUPS + " varchar,"
        + GsaSpecialColumns.GSA_PERMIT_USERS + " varchar,"
        + GsaSpecialColumns.GSA_DENY_GROUPS + " varchar,"
        + GsaSpecialColumns.GSA_DENY_USERS + " varchar)");
    executeUpdate("insert into acl("
        + GsaSpecialColumns.GSA_PERMIT_GROUPS + ","
        + GsaSpecialColumns.GSA_PERMIT_USERS + ","
        + GsaSpecialColumns.GSA_DENY_GROUPS + ","
        + GsaSpecialColumns.GSA_DENY_USERS + ") values "
        + "('pgroup1, pgroup1', 'puser1, puser2', "
        + "'dgroup1, dgroup1', 'duser1, duser2'), "
        + "('pgroup1', 'puser2, puser1', "
        + "'dgroup2, dgroup2', 'duser4, duser2')");
    Acl golden = new Acl.Builder()
        .setPermitUsers(Arrays.asList(
            new UserPrincipal("puser2"),
            new UserPrincipal("puser1")))
        .setDenyUsers(Arrays.asList(
            new UserPrincipal("duser2"),
            new UserPrincipal("duser1"),
            new UserPrincipal("duser4")))
        .setPermitGroups(Arrays.asList(
            new GroupPrincipal("pgroup1")))
        .setDenyGroups(Arrays.asList(
            new GroupPrincipal("dgroup1"),
            new GroupPrincipal("dgroup2")))
        .build();
    ResultSet rs = executeQuery("select * from acl");
    Acl acl = DatabaseAdaptor.buildAcl(rs, ",", DEFAULT_NAMESPACE);
    assertEquals(golden, acl);
  }
  
  @Test
  public void testAclSqlResultSetOneColumnPerRow() throws SQLException {
    executeUpdate("create table acl("
        + GsaSpecialColumns.GSA_PERMIT_GROUPS + " varchar,"
        + GsaSpecialColumns.GSA_PERMIT_USERS + " varchar,"
        + GsaSpecialColumns.GSA_DENY_GROUPS + " varchar,"
        + GsaSpecialColumns.GSA_DENY_USERS + " varchar)");
    executeUpdate("insert into acl(" + GsaSpecialColumns.GSA_PERMIT_GROUPS + ")"
        + " values ('pgroup1, pgroup2')");
    executeUpdate("insert into acl(" + GsaSpecialColumns.GSA_PERMIT_USERS + ")"
        + " values ('puser1, puser2')");
    executeUpdate("insert into acl(" + GsaSpecialColumns.GSA_DENY_GROUPS + ")"
        + " values ('dgroup1, dgroup2')");
    executeUpdate("insert into acl(" + GsaSpecialColumns.GSA_DENY_USERS + ")"
        + " values ('duser1, duser2')");
    Acl golden = new Acl.Builder()
        .setPermitGroups(Arrays.asList(
            new GroupPrincipal("pgroup1"),
            new GroupPrincipal("pgroup2")))
        .setPermitUsers(Arrays.asList(
            new UserPrincipal("puser1"),
            new UserPrincipal("puser2")))
        .setDenyGroups(Arrays.asList(
            new GroupPrincipal("dgroup2"),
            new GroupPrincipal("dgroup1")))
        .setDenyUsers(Arrays.asList(
            new UserPrincipal("duser1"),
            new UserPrincipal("duser2")))
        .build();
    ResultSet rs = executeQuery("select * from acl");
    Acl acl = DatabaseAdaptor.buildAcl(rs, ",", DEFAULT_NAMESPACE);
    assertEquals(golden, acl);
  }
  
  @Test
  public void testGetPrincipalsWithEmptyDelim() throws SQLException {
    executeUpdate("create table acl("
        + GsaSpecialColumns.GSA_DENY_GROUPS + " varchar,"
        + GsaSpecialColumns.GSA_DENY_USERS + " varchar)");
    executeUpdate("insert into acl("
        + GsaSpecialColumns.GSA_DENY_GROUPS + ","
        + GsaSpecialColumns.GSA_DENY_USERS + ") values ("
        + "'dgroup1, dgroup2', 'duser1, duser2')");
    List<UserPrincipal> goldenUsers = Arrays.asList(
        new UserPrincipal("duser1, duser2"));
    List<GroupPrincipal> goldenGroups = Arrays.asList(
        new GroupPrincipal("dgroup1, dgroup2"));
    ResultSet rs = executeQueryAndNext("select * from acl");
    ArrayList<UserPrincipal> users =
        DatabaseAdaptor.getUserPrincipalsFromResultSet(rs,
            GsaSpecialColumns.GSA_DENY_USERS, "", DEFAULT_NAMESPACE);
    ArrayList<GroupPrincipal> groups =
        DatabaseAdaptor.getGroupPrincipalsFromResultSet(rs,
            GsaSpecialColumns.GSA_DENY_GROUPS, "", DEFAULT_NAMESPACE);
    assertEquals(goldenUsers, users);
    assertEquals(goldenGroups, groups);
  }
  
  @Test
  public void testGetPrincipalsWithUserDefinedDelim() throws SQLException {
    executeUpdate("create table acl("
        + GsaSpecialColumns.GSA_DENY_GROUPS + " varchar,"
        + GsaSpecialColumns.GSA_DENY_USERS + " varchar)");
    executeUpdate("insert into acl("
        + GsaSpecialColumns.GSA_DENY_GROUPS + ","
        + GsaSpecialColumns.GSA_DENY_USERS + ") values ("
        + "'dgroup1 ; dgroup2', 'duser1 ; duser2')");
    List<UserPrincipal> goldenUsers = Arrays.asList(
        new UserPrincipal("duser1"), 
        new UserPrincipal("duser2"));
    List<GroupPrincipal> goldenGroups = Arrays.asList(
        new GroupPrincipal("dgroup1"),
        new GroupPrincipal("dgroup2"));
    ResultSet rs = executeQueryAndNext("select * from acl");
    ArrayList<UserPrincipal> users =
        DatabaseAdaptor.getUserPrincipalsFromResultSet(rs,
            GsaSpecialColumns.GSA_DENY_USERS, " ; ", DEFAULT_NAMESPACE);
    ArrayList<GroupPrincipal> groups =
        DatabaseAdaptor.getGroupPrincipalsFromResultSet(rs,
            GsaSpecialColumns.GSA_DENY_GROUPS, " ; ", DEFAULT_NAMESPACE);
    assertEquals(goldenUsers, users);
    assertEquals(goldenGroups, groups);
  }

  @Test
  public void testGetPrincipals_nullOrEmpty() throws SQLException {
    executeUpdate("create table acl("
        + GsaSpecialColumns.GSA_DENY_GROUPS + " varchar,"
        + GsaSpecialColumns.GSA_DENY_USERS + " varchar)");
    executeUpdate("insert into acl("
        + GsaSpecialColumns.GSA_DENY_GROUPS + ") values ('')");
    List<GroupPrincipal> goldenGroups = Arrays.asList();
    ResultSet rs = executeQueryAndNext("select * from acl");
    ArrayList<UserPrincipal> users =
        DatabaseAdaptor.getUserPrincipalsFromResultSet(rs,
            GsaSpecialColumns.GSA_DENY_USERS, "", DEFAULT_NAMESPACE);
    ArrayList<GroupPrincipal> groups =
        DatabaseAdaptor.getGroupPrincipalsFromResultSet(rs,
            GsaSpecialColumns.GSA_DENY_GROUPS, "", DEFAULT_NAMESPACE);
    assertEquals(Arrays.asList(), users);
    assertEquals(Arrays.asList(), groups);
  }

  private static class Holder<T> {
    private T value;

    public void set(T value) {
      this.value = value;
    }

    public T get() {
      return value;
    }
  }

  /**
   * Returns a Database adaptor instance with the supplied config overrides.
   * Adaptor.initConfig() and Adaptor.init() have already been called.
   */
  private DatabaseAdaptor getObjectUnderTest(Map<String, String> moreEntries)
      throws Exception {
    return getObjectUnderTest(moreEntries, new Holder<RecordingContext>());
  }

  /**
   * Returns a Database adaptor instance with the supplied config overrides.
   * Adaptor.initConfig() and Adaptor.init() have already been called.
   * Stores the generated RecordingContext into the given holder.
   */
  private DatabaseAdaptor getObjectUnderTest(Map<String, String> moreEntries,
      Holder<RecordingContext> contextHolder) throws Exception {
    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.driverClass", JdbcFixture.DRIVER_CLASS);
    configEntries.put("db.url", JdbcFixture.URL);
    configEntries.put("db.user", JdbcFixture.USER);
    configEntries.put("db.password", JdbcFixture.PASSWORD);
    configEntries.put("gsa.hostname", "localhost");

    File file = tempFolder.newFile("dba.test.properties");
    Properties properties = new Properties();
    properties.putAll(configEntries);
    properties.putAll(moreEntries);
    properties.store(new FileOutputStream(file), "");

    Config config = new Config();
    DatabaseAdaptor adaptor = new DatabaseAdaptor();
    adaptor.initConfig(config);
    config.load(file);
    RecordingContext context = TestHelper.createConfigAdaptorContext(config);
    contextHolder.set(context);
    adaptor.init(context);
    return adaptor;
  }

  @Test
  public void testInitFeedMaxUrls_zero() throws Exception {
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("feed.maxUrls", "0");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.uniqueKey", "");
    moreEntries.put("db.modeOfOperation", "");
    moreEntries.put("db.everyDocIdSql", "");

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("feed.maxUrls needs to be positive");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitFeedMaxUrls_negative() throws Exception {
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("feed.maxUrls", "-100");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.uniqueKey", "");
    moreEntries.put("db.modeOfOperation", "");
    moreEntries.put("db.everyDocIdSql", "");

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("feed.maxUrls needs to be positive");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitUniqueKeyInvalidType() throws Exception {
    executeUpdate("create table data(productid int, other varchar)");
    // Type of unique key id value cannot be "notvalid", since it's invalid.
    // That cat be int, string, timestamp, date, time, and long.
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.uniqueKey", "productid:notvalid");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.everyDocIdSql", "select productid from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where productid = ?");
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("Invalid UniqueKey type 'notvalid'");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitUniqueKeyContainsRepeatedKeyName() throws Exception {
    executeUpdate("create table data(productid int, other varchar)");
    // Value of unique key cannot contain repeated key name.
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.uniqueKey", "productid:int,productid:string");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.everyDocIdSql", "select productid from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where productid = ?");
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("key name 'productid' was repeated");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitUniqueKeyContainsRepeatedKeyNameDifferentCase()
      throws Exception {
    executeUpdate("create table data(productid int, other varchar)");
    // Value of unique key cannot contain repeated key name.
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.uniqueKey", "productid:int,PRODUCTID:string");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.everyDocIdSql", "select productid from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where productid = ?");
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("key name 'PRODUCTID' was repeated");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitUniqueKeyEmpty() throws Exception {
    executeUpdate("create table data(id int, url varchar)");
    // Value of unique id cannot be empty.
    // The value has to be something like "keyname:type"
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.uniqueKey", "");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    moreEntries.put("docId.isUrl", "true");
    moreEntries.put("db.everyDocIdSql", "select id from data");
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("db.uniqueKey parameter: value cannot be empty");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitUniqueKeyOptionalTypes() throws Exception {
    executeUpdate("create table data(id int, name varchar, ordered timestamp)");
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.uniqueKey", "id, name, ordered:timestamp");
    moreEntries.put("db.everyDocIdSql", "select id, name, ordered from data");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

    List<String> messages = new ArrayList<String>();
    captureLogMessages(DatabaseAdaptor.class, "primary key: ", messages);
    getObjectUnderTest(moreEntries);
    assertEquals(messages.toString(), 1, messages.size());
    // Verify the unspecified types were correctly determined from the DB.
    assertThat(messages.get(0).toLowerCase(US),
        containsString("{id=int, name=string, ordered=timestamp}"));
  }

  @Test
  public void testInitUniqueKeyUrl() throws Exception {
    executeUpdate("create table data(id int, url varchar)");
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    moreEntries.put("docId.isUrl", "true");
    moreEntries.put("db.uniqueKey", "id:int, url:string");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "select id, url from data");
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("db.uniqueKey value: The key must be a single");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitMetadataColumns_defaults() throws Exception {
    executeUpdate("create table data(id int, foo varchar, bar varchar)");
    executeUpdate("insert into data(id, foo, bar) "
                  + "values(1, 'fooVal', 'barVal')");

    Map<String, String> moreEntries = new HashMap<String, String>();
    // Required for validation, but not specific to this test.
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select id from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);
    ResultSet resultSet = executeQueryAndNext("select * from data");
    Record.Builder builder = new Record.Builder(new DocId("1"));
    adaptor.addMetadataToRecordBuilder(builder, resultSet);

    assertNull(builder.build().getMetadata());
  }

  @Test
  public void testInitMetadataColumns_falseBlank() throws Exception {
    executeUpdate("create table data(id int, foo varchar, bar varchar)");
    executeUpdate("insert into data(id, foo, bar) "
                  + "values(1, 'fooVal', 'barVal')");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.includeAllColumnsAsMetadata", "false");
    moreEntries.put("db.metadataColumns", "");

    // Required for validation, but not specific to this test.
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select id from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);
    ResultSet resultSet = executeQueryAndNext("select * from data");
    Record.Builder builder = new Record.Builder(new DocId("1"));
    adaptor.addMetadataToRecordBuilder(builder, resultSet);

    assertNull(builder.build().getMetadata());
  }

  @Test
  public void testInitMetadataColumns_trueBlank() throws Exception {
    executeUpdate("create table data(id int, foo varchar, bar varchar)");
    executeUpdate("insert into data(id, foo, bar) "
                  + "values(1, 'fooVal', 'barVal')");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.includeAllColumnsAsMetadata", "true");
    moreEntries.put("db.metadataColumns", "");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select id from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);
    ResultSet resultSet = executeQueryAndNext("select * from data");
    Record.Builder builder = new Record.Builder(new DocId("1"));
    adaptor.addMetadataToRecordBuilder(builder, resultSet);

    Metadata golden = new Metadata();
    golden.add("ID", "1");
    golden.add("FOO", "fooVal");
    golden.add("BAR", "barVal");
    assertEquals(golden, builder.build().getMetadata());
  }

  @Test
  public void testInitMetadataColumns_falseSet() throws Exception {
    executeUpdate("create table data(id int, db_col1 varchar, col2 varchar)");
    executeUpdate("insert into data(id, db_col1, col2) "
                  + "values(1, 'col1Val', 'col2Val')");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.metadataColumns", "db_col1:gsa1,col2:gsa2");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select id from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);
    ResultSet resultSet = executeQueryAndNext("select * from data");
    Record.Builder builder = new Record.Builder(new DocId("1"));
    adaptor.addMetadataToRecordBuilder(builder, resultSet);

    Metadata golden = new Metadata();
    golden.add("gsa1", "col1Val");
    golden.add("gsa2", "col2Val");
    assertEquals(golden, builder.build().getMetadata());
  }

  @Test
  public void testInitMetadataColumns_trueSet() throws Exception {
    executeUpdate("create table data(id int, db_col1 varchar, col2 varchar)");
    executeUpdate("insert into data(id, db_col1, col2) "
                  + "values(1, 'col1Val', 'col2Val')");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.includeAllColumnsAsMetadata", "true");
    moreEntries.put("db.metadataColumns", "db_col1:gsa1,col2:gsa2");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select id from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

    List<String> messages = new ArrayList<String>();
    captureLogMessages(DatabaseAdaptor.class, "will be ignored", messages);
    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);
    assertEquals(messages.toString(), 1, messages.size());
    assertThat(messages.get(0),
        containsString("ignored: [db.includeAllColumnsAsMetadata]"));

    ResultSet resultSet = executeQueryAndNext("select * from data");
    Record.Builder builder = new Record.Builder(new DocId("1"));
    adaptor.addMetadataToRecordBuilder(builder, resultSet);

    Metadata golden = new Metadata();
    golden.add("gsa1", "col1Val");
    golden.add("gsa2", "col2Val");
    assertEquals(golden, builder.build().getMetadata());
  }

  @Test
  public void testInitMetadataColumns_columnNotFound() throws Exception {
    // Simulate a skipped column verification by creating the missing
    // column for init but removing it for addMetadataToRecordBuilder.
    executeUpdate(
        "create table data(id int, \"fake column\" varchar, col2 varchar)");
    executeUpdate("insert into data(id, col2) values(1, 'col2Val')");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.metadataColumns", "fake column:gsa1,col2:gsa2");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select id from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);

    executeUpdate("alter table data drop column \"fake column\"");

    ResultSet resultSet = executeQueryAndNext("select * from data");
    Record.Builder builder = new Record.Builder(new DocId("1"));
    adaptor.addMetadataToRecordBuilder(builder, resultSet);

    Metadata golden = new Metadata();
    golden.add("gsa2", "col2Val");
    assertEquals(golden, builder.build().getMetadata());
  }

  @Test
  public void testInitMetadataColumns_caseInsensitive() throws Exception {
    executeUpdate("create table data(id int, foo varchar, bar varchar)");
    executeUpdate("insert into data(id, foo, bar) "
                  + "values(1, 'fooVal', 'barVal')");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.metadataColumns", "Foo:gsa_foo,Bar:gsa_bar");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select id from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);
    ResultSet resultSet = executeQueryAndNext("select * from data");
    Record.Builder builder = new Record.Builder(new DocId("1"));
    adaptor.addMetadataToRecordBuilder(builder, resultSet);

    Metadata golden = new Metadata();
    golden.add("gsa_foo", "fooVal");
    golden.add("gsa_bar", "barVal");
    assertEquals(golden, builder.build().getMetadata());
  }

  @Test
  public void testInitMetadataColumns_emptyColumnName() throws Exception {
    executeUpdate("create table data(id int, foo varchar, bar varchar)");
    executeUpdate("insert into data(id, foo, bar) "
                  + "values(1, 'fooVal', 'barVal')");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.metadataColumns", "Foo:gsa_foo,:");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select id from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("column names from db.metadataColumns are empty");
    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitUrlMetadataLister_noDocIdIsUrl() throws Exception {
    executeUpdate("create table data(id integer, url varchar)");
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    moreEntries.put("db.uniqueKey", "url:string");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "select url from data");
    moreEntries.put("db.singleDocContentSql", "select id, url from data");
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("requires docId.isUrl to be \"true\"");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitUrlMetadataLister_docIdIsUrlFalse() throws Exception {
    executeUpdate("create table data(id integer, url varchar)");
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    moreEntries.put("docId.isUrl", "false");
    moreEntries.put("db.uniqueKey", "url:string");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "select id, url from data");
    moreEntries.put("db.singleDocContentSql", "select * from data");
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("requires docId.isUrl to be \"true\"");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitUrlMetadataLister_docIdIsUrlTrue() throws Exception {
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    moreEntries.put("docId.isUrl", "true");
    moreEntries.put("db.uniqueKey", "url:string");
    // Required for validation, but not specific to this test.
    executeUpdate("create table data(url varchar)");
    moreEntries.put("db.everyDocIdSql", "select url from data");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitLister_ignoredProperties_metadataColumns()
      throws Exception {
    executeUpdate("create table data(url varchar, other varchar)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("docId.isUrl", "true");
    moreEntries.put("db.uniqueKey", "url:string");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "select url from data");
    // Ignored properties in this mode.
    moreEntries.put("db.singleDocContentSql",
        "select * from data where url = ?");
    moreEntries.put("db.metadataColumns", "other");

    List<String> messages = new ArrayList<String>();
    captureLogMessages(DatabaseAdaptor.class, "will be ignored", messages);
    getObjectUnderTest(moreEntries);
    assertEquals(messages.toString(), 1, messages.size());
    assertThat(messages.get(0),
        containsString("[db.metadataColumns, db.singleDocContentSql]"));
   }

  @Test
  public void testInitLister_ignoredProperties_allColumns() throws Exception {
    executeUpdate("create table data(url varchar, other varchar)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("docId.isUrl", "true");
    moreEntries.put("db.uniqueKey", "url:string");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "select url from data");
    // Ignored properties in this mode.
    moreEntries.put("db.includeAllColumnsAsMetadata", "true");

    List<String> messages = new ArrayList<String>();
    captureLogMessages(DatabaseAdaptor.class, "will be ignored", messages);
    getObjectUnderTest(moreEntries);
    assertEquals(messages.toString(), 1, messages.size());
    assertThat(messages.get(0),
        containsString("[db.includeAllColumnsAsMetadata]"));
   }

  @Test
  public void testInitEmptyQuery_singleDocContentSql() throws Exception {
    executeUpdate("create table data(id int, other varchar)");
    executeUpdate("insert into data(id) values(1001)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.singleDocContentSql", "");
    moreEntries.put("db.modeOfOperation", "rowToText");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "select id from data");

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("db.singleDocContentSql cannot be an empty string");
    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitEmptyQuery_singleDocContentSql_lister() throws Exception {
    executeUpdate("create table data(url varchar, other varchar)");
    executeUpdate("insert into data(url) values('http://')");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("docId.isUrl", "true");
    moreEntries.put("db.uniqueKey", "url:string");
    moreEntries.put("db.singleDocContentSql", "");
    moreEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "select url from data");

    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitEmptyQuery_everyDocIdSql() throws Exception {
    executeUpdate("create table data(id int, other varchar)");
    executeUpdate("insert into data(id) values(1001)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");
    moreEntries.put("db.modeOfOperation", "rowToText");

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("db.everyDocIdSql cannot be an empty string");
    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitUpdateSql_ignoredProperties() throws Exception {
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.updateTimestampTimezone", "GMT-8");
    // Required for validation, but not specific to this test.
    executeUpdate("create table data(id int)");
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select id from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

    List<String> messages = new ArrayList<String>();
    captureLogMessages(DatabaseAdaptor.class, "will be ignored", messages);
    getObjectUnderTest(moreEntries);
    assertEquals(messages.toString(), 1, messages.size());
    assertThat(messages.get(0),
        containsString("[db.updateTimestampTimezone]"));
  }

  @Test
  public void testInitVerifyColumnNames_uniqueKey() throws Exception {
    executeUpdate("create table data(id int, other varchar)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select other from data");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("[id] not found in query");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitVerifyColumnNames_uniqueKey_updateSql() throws Exception {
    executeUpdate("create table data(id int, other varchar)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.updateSql", "select other from data");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "select id from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("[id] not found in query");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void
      testInitVerifyColumnNames_singleDocContentSql_uniqueKeyNotInSelect()
          throws Exception {
    executeUpdate("create table data(id int, other varchar)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.singleDocContentSql",
        "select other from data where id = ?");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "select id from data");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitVerifyColumnNames_singleDocContentSql_differentCase()
      throws Exception {
    executeUpdate("create table data(productid int, other varchar)");
    // Value of unique key cannot contain repeated key name.
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.uniqueKey", "productid:int");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.everyDocIdSql", "select productid from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where productid = ?");
    moreEntries.put("db.singleDocContentSqlParameters", "PRODUCTID");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitVerifyColumnNames_aclSql_uniqueKeyNotInSelect()
      throws Exception {
    executeUpdate("create table data(id int, other varchar)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.aclSql", "select other from data where id = ?");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "select id from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitVerifyColumnNames_aclSql_differentCase()
      throws Exception {
    executeUpdate("create table data(productid int, other varchar)");
    // Value of unique key cannot contain repeated key name.
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.uniqueKey", "productid:int");
    moreEntries.put("db.aclSql",
        "select productid, other from data where productid = ?");
    moreEntries.put("db.aclSqlParameters", "PRODUCTID");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.everyDocIdSql", "select productid from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where productid = ?");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitVerifyColumnNames_urlAndMetadata() throws Exception {
    executeUpdate("create table data(url varchar, other varchar)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    moreEntries.put("docId.isUrl", "true");
    moreEntries.put("db.uniqueKey", "url:string");
    moreEntries.put("db.everyDocIdSql", "select url from data");
    moreEntries.put("db.metadataColumns", "other:other");

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("[other] not found in query");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitVerifyColumnNames_urlAndMetadata_updateSql()
      throws Exception {
    executeUpdate("create table data(url varchar, other varchar)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    moreEntries.put("docId.isUrl", "true");
    moreEntries.put("db.uniqueKey", "url:string");
    moreEntries.put("db.updateSql", "select url from data");
    moreEntries.put("db.metadataColumns", "other:other");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "select url, other from data");

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("[other] not found in query");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitVerifyColumnNames_metadataColumns() throws Exception {
    executeUpdate("create table data(id int, other varchar)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.singleDocContentSql",
        "select id from data where id = ?");
    moreEntries.put("db.metadataColumns", "other:other");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "select id from data");

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("[other] not found in query");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitVerifyColumnNames_metadataColumnAlias() throws Exception {
    executeUpdate("create table data(id int, other varchar)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.singleDocContentSql",
        "select id, other as col1 from data where id = ?");
    moreEntries.put("db.metadataColumns", "col1:col1");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "select id from data");
    moreEntries.put("db.aclSql", "select * from data where id = ?");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitVerifyColumnNames_metadataColumnAliasBad()
      throws Exception {
    executeUpdate("create table data(id int, other varchar)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.singleDocContentSql",
        "select id, other as col1 from data where id = ?");
    moreEntries.put("db.metadataColumns", "other:other");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "select id from data");

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("[other] not found in query");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitVerifyColumnNames_modeOfOperation() throws Exception {
    executeUpdate("create table data(id int, other varchar)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "contentColumn");
    moreEntries.put("db.modeOfOperation.contentColumn.columnName", "blob");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.singleDocContentSql",
        "select id from data where id = ?");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "select id from data");

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("[blob] not found in query");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitVerifyColumnNames_sqlException() throws Exception {
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.user", "not_sa");
    // Required for validation, but not specific to this test.
    executeUpdate("create table data(id int)");
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select id from data");
    moreEntries.put("db.singleDocContentSql",
        "select id from data where id = ?");

    List<String> messages = new ArrayList<String>();
    captureLogMessages(DatabaseAdaptor.class, "Unable to validate", messages);
    getObjectUnderTest(moreEntries);
    assertEquals(messages.toString(), 1, messages.size());
  }

  @Test
  public void testGetDocIds() throws Exception {
    executeUpdate("create table data(id integer, other varchar)");
    executeUpdate("insert into data(id, other) values(1, 'hello world'),"
        + "(2, 'hello world'), (3, 'hello world'), (4, 'hello world')");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select id from data order by id");
    moreEntries.put("db.metadataColumns", "other");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);
    RecordingDocIdPusher pusher = new RecordingDocIdPusher();
    adaptor.getDocIds(pusher);

    assertEquals(
        Arrays.asList(
            new Record.Builder(new DocId("1")).setMetadata(null).build(),
            new Record.Builder(new DocId("2")).setMetadata(null).build(),
            new Record.Builder(new DocId("3")).setMetadata(null).build(),
            new Record.Builder(new DocId("4")).setMetadata(null).build()),
        pusher.getRecords());
   }

  @Test
  public void testGetDocIds_columnAlias() throws Exception {
    executeUpdate("create table data(id integer, other varchar)");
    executeUpdate("insert into data(id, other) values(1, 'hello world'),"
        + "(2, 'hello world'), (3, 'hello world'), (4, 'hello world')");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "foo:int");
    moreEntries.put("db.everyDocIdSql",
        "select id as foo from data order by id");
    moreEntries.put("db.metadataColumns", "other");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);
    RecordingDocIdPusher pusher = new RecordingDocIdPusher();
    adaptor.getDocIds(pusher);

    assertEquals(
        Arrays.asList(
            new Record.Builder(new DocId("1")).setMetadata(null).build(),
            new Record.Builder(new DocId("2")).setMetadata(null).build(),
            new Record.Builder(new DocId("3")).setMetadata(null).build(),
            new Record.Builder(new DocId("4")).setMetadata(null).build()),
        pusher.getRecords());
   }

  @Test
  public void testGetDocIds_urlAndMetadataLister() throws Exception {
    executeUpdate("create table data(url varchar, name varchar)");
    executeUpdate(
        "insert into data(url, name) values('http://localhost/','John')");

    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.uniqueKey", "url:string");
    configEntries.put("db.everyDocIdSql", "select * from data");
    configEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    configEntries.put("docId.isUrl", "true");
    configEntries.put("db.metadataColumns", "URL:col1, NAME:col2");

    DatabaseAdaptor adaptor = getObjectUnderTest(configEntries);
    RecordingDocIdPusher pusher = new RecordingDocIdPusher();
    adaptor.getDocIds(pusher);

    Metadata metadata = new Metadata();
    metadata.add("col1",  "http://localhost/");
    metadata.add("col2",  "John");
    assertEquals(
        Arrays.asList(new Record.Builder(new DocId("http://localhost/"))
          .setMetadata(metadata).build()),
        pusher.getRecords());
  }

  @Test
  public void testGetDocIds_docIdIsUrl() throws Exception {
    // Add time to show the records as modified.
    executeUpdate("create table data(url varchar)");
    executeUpdate("insert into data(url) values "
        + "('http://host/foo'), ('foo/bar'), ('http://host/bar'), "
        + "('http://host/foo bar')");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    moreEntries.put("docId.isUrl", "true");
    moreEntries.put("db.uniqueKey", "url:string");
    moreEntries.put("db.everyDocIdSql", "select url from data");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.updateSql", "select url from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where url = ?");

    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);
    RecordingDocIdPusher pusher = new RecordingDocIdPusher();
    List<String> messages = new ArrayList<String>();
    captureLogMessages(DatabaseAdaptor.class, "Invalid DocId URL", messages);
    adaptor.getDocIds(pusher);

    assertEquals(messages.toString(), 1, messages.size());
    assertEquals(
        Arrays.asList(
            new Record.Builder(new DocId("http://host/foo")).build(),
            new Record.Builder(new DocId("http://host/bar")).build(),
            new Record.Builder(new DocId("http://host/foo%20bar")).build()),
        pusher.getRecords());
  }

  @Test
  public void testGetDocIds_gsaAction() throws Exception {
    executeUpdate("create table data(id integer, url varchar, action varchar)");
    executeUpdate("insert into data(id, url, action) values"
        + "('1001', 'http://localhost/?q=1001', 'add'),"
        + "('1002', 'http://localhost/?q=1002', 'delete'),"
        + "('1003', 'http://localhost/?q=1003', 'DELETE'),"
        + "('1004', 'http://localhost/?q=1004', 'foo'),"
        + "('1005', 'http://localhost/?q=1005', null)");

    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.uniqueKey", "url:string");
    configEntries.put("db.everyDocIdSql",
        "select id, url, action as GSA_ACTION from data order by url");
    configEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    configEntries.put("docId.isUrl", "true");
    configEntries.put("db.metadataColumns", "id:id");

    DatabaseAdaptor adaptor = getObjectUnderTest(configEntries);
    RecordingDocIdPusher pusher = new RecordingDocIdPusher();
    adaptor.getDocIds(pusher);

    Metadata metadata1 = new Metadata();
    metadata1.add("id", "1001");
    Metadata metadata4 = new Metadata();
    metadata4.add("id", "1004");
    Metadata metadata5 = new Metadata();
    metadata5.add("id", "1005");
    assertEquals(Arrays.asList(new Record[] {
        new Record.Builder(new DocId("http://localhost/?q=1001"))
            .setMetadata(metadata1).build(),
        new Record.Builder(new DocId("http://localhost/?q=1002"))
            .setDeleteFromIndex(true).build(),
        new Record.Builder(new DocId("http://localhost/?q=1003"))
            .setDeleteFromIndex(true).build(),
        new Record.Builder(new DocId("http://localhost/?q=1004"))
            .setMetadata(metadata4).build(),
        new Record.Builder(new DocId("http://localhost/?q=1005"))
            .setMetadata(metadata5).build()}),
        pusher.getRecords());
  }

  @Test
  public void testGetDocIds_disableStreaming() throws Exception {
    executeUpdate("create table data(id integer)");
    executeUpdate("insert into data(id) values(1), (2), (3), (4)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select id from data order by id");
    moreEntries.put("db.disableStreaming", "true");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.singleDocContentSql",
        "select id from data where id = ?");

    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);
    RecordingDocIdPusher pusher = new RecordingDocIdPusher();
    adaptor.getDocIds(pusher);

    assertEquals(
        Arrays.asList(
            new Record.Builder(new DocId("1")).setMetadata(null).build(),
            new Record.Builder(new DocId("2")).setMetadata(null).build(),
            new Record.Builder(new DocId("3")).setMetadata(null).build(),
            new Record.Builder(new DocId("4")).setMetadata(null).build()),
        pusher.getRecords());
  }

  @Test
  public void testGetDocIds_feedMaxUrls() throws Exception {
    executeUpdate("create table data(id varchar)");
    executeUpdate("insert into data(id) values(1), (2), (3), ('hello')");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select id from data order by id");
    moreEntries.put("feed.maxUrls", "2");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);
    RecordingDocIdPusher pusher = new RecordingDocIdPusher();
    try {
      adaptor.getDocIds(pusher);
      fail("Expected an IOException");
    } catch (IOException e) {
      assertThat(e.getCause(), instanceOf(SQLException.class));
    }

    // BufferedPusher will push buffered records after the SQLException,
    // so DocId("3") is included here.
    assertEquals(
        Arrays.asList(
            new Record.Builder(new DocId("1")).setMetadata(null).build(),
            new Record.Builder(new DocId("2")).setMetadata(null).build(),
            new Record.Builder(new DocId("3")).setMetadata(null).build()),
        pusher.getRecords());
  }

  /** Tests that the BufferedPusher doesn't try to push 0 items on close. */
  @Test
  public void testGetDocIds_feedMaxUrls_emptyOnClose() throws Exception {
    executeUpdate("create table data(id integer)");
    executeUpdate("insert into data(id) values(1), (2), (3), (4)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select id from data order by id");
    moreEntries.put("feed.maxUrls", "2");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

    List<String> messages = new ArrayList<String>();
    captureLogMessages(DatabaseAdaptor.class, "doc ids to pusher", messages);

    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);
    RecordingDocIdPusher pusher = new RecordingDocIdPusher();
    adaptor.getDocIds(pusher);

    assertEquals(
        Arrays.asList(
            new Record.Builder(new DocId("1")).setMetadata(null).build(),
            new Record.Builder(new DocId("2")).setMetadata(null).build(),
            new Record.Builder(new DocId("3")).setMetadata(null).build(),
            new Record.Builder(new DocId("4")).setMetadata(null).build()),
        pusher.getRecords());
    assertEquals(messages.toString(), 2, messages.size());
  }

  @Test
  public void testGetDocIds_nullPusher() throws Exception {
    // Required for validation, but not specific to this test.
    executeUpdate("create table data(id integer)");
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select id from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);

    // Of course this throws a NullPointerException, but the point is that
    // it throws without ever pushing a record, for more predicatability.
    thrown.expect(NullPointerException.class);
    adaptor.getDocIds(null);
  }

  private PollingIncrementalLister getPollingIncrementalLister(
      Map<String, String> moreEntries) throws Exception {
    Holder<RecordingContext> contextHolder = new Holder<>();
    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries, contextHolder);
    return contextHolder.get().getPollingIncrementalLister();
  }

  @Test
  public void testGetModifiedDocIds() throws Exception {
    // Subtract time to show the records as unchanged.
    // Add time to show the records as modified.
    executeUpdate("create table data(id integer, "
        + "other varchar default 'hello, world', ts timestamp)");
    executeUpdate("insert into data(id, ts) values "
        + "(1, dateadd('minute', 1, current_timestamp())),"
        + "(2, dateadd('minute', 2, current_timestamp())),"
        + "(3, dateadd('minute', 1, current_timestamp())),"
        + "(4, dateadd('minute', -1, current_timestamp()))");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.updateSql",
        "select id from data where ts >= ? order by id");
    moreEntries.put("db.metadataColumns", "other");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "select id from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

    List<Record> golden =
        Arrays.asList(
            new Record.Builder(new DocId("1"))
                .setMetadata(null).setCrawlImmediately(true).build(),
            new Record.Builder(new DocId("2"))
                .setMetadata(null).setCrawlImmediately(true).build(),
            new Record.Builder(new DocId("3"))
                .setMetadata(null).setCrawlImmediately(true).build());

    PollingIncrementalLister lister = getPollingIncrementalLister(moreEntries);
    RecordingDocIdPusher pusher = new RecordingDocIdPusher();
    lister.getModifiedDocIds(pusher);
    assertEquals(golden, pusher.getRecords());

    // Without a GSA_TIMESTAMP column, we continue using the current time.
    pusher.reset();
    lister.getModifiedDocIds(pusher);
    assertEquals(golden, pusher.getRecords());
  }

  @Test
  public void testGetModifiedDocIds_gsaAction() throws Exception {
    executeUpdate("create table data(url varchar, action varchar, "
        + "other varchar default 'hello, world', ts timestamp)");
    executeUpdate("insert into data(url, action, ts) values"
        + "('http://localhost/0', 'add', "
            + "dateadd('minute', -1, current_timestamp())),"
        + "('http://localhost/1', 'add', "
            + "dateadd('minute', 1, current_timestamp())),"
        + "('http://localhost/2', 'delete', "
            + "dateadd('minute', 1, current_timestamp())),"
        + "('http://localhost/3', 'DELETE', "
            + "dateadd('minute', 1, current_timestamp())),"
        + "('http://localhost/4', 'foo', "
            + "dateadd('minute', 1, current_timestamp())),"
        + "('http://localhost/5', null, "
            + "dateadd('minute', 1, current_timestamp()))");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    moreEntries.put("docId.isUrl", "true");
    moreEntries.put("db.uniqueKey", "url");
    moreEntries.put("db.updateSql", "select url, action as gsa_action, other "
        + "from data where ts >= ? order by url");
    moreEntries.put("db.metadataColumns", "other");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "select * from data");
    moreEntries.put("db.singleDocContentSql", "");

    PollingIncrementalLister lister = getPollingIncrementalLister(moreEntries);
    RecordingDocIdPusher pusher = new RecordingDocIdPusher();
    lister.getModifiedDocIds(pusher);

    Metadata metadata = new Metadata();
    metadata.add("other", "hello, world");
    assertEquals(Arrays.asList(new Record[] {
        new Record.Builder(new DocId("http://localhost/1"))
            .setMetadata(metadata).setCrawlImmediately(true).build(),
        new Record.Builder(new DocId("http://localhost/2"))
            .setDeleteFromIndex(true).setCrawlImmediately(true).build(),
        new Record.Builder(new DocId("http://localhost/3"))
            .setDeleteFromIndex(true).setCrawlImmediately(true).build(),
        new Record.Builder(new DocId("http://localhost/4"))
            .setMetadata(metadata).setCrawlImmediately(true).build(),
        new Record.Builder(new DocId("http://localhost/5"))
            .setMetadata(metadata).setCrawlImmediately(true).build()}),
        pusher.getRecords());
  }

  @Test
  public void testGetModifiedDocIds_gsaTimestamp() throws Exception {
    // Subtract time to show the records as unchanged.
    // Add time to show the records as modified.
    executeUpdate("create table data(id integer, ts timestamp)");
    executeUpdate("insert into data(id, ts) values "
        + "(1, dateadd('minute', 1, current_timestamp())),"
        + "(2, dateadd('minute', 2, current_timestamp())),"
        + "(3, dateadd('minute', 1, current_timestamp())),"
        + "(4, dateadd('minute', -1, current_timestamp()))");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.updateSql",
        "select id, ts as gsa_timestamp from data where ts >= ? order by id");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "select id from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

    PollingIncrementalLister lister = getPollingIncrementalLister(moreEntries);
    RecordingDocIdPusher pusher = new RecordingDocIdPusher();
    lister.getModifiedDocIds(pusher);
    assertEquals(
        Arrays.asList(
            new Record.Builder(new DocId("1"))
                .setMetadata(null).setCrawlImmediately(true).build(),
            new Record.Builder(new DocId("2"))
                .setMetadata(null).setCrawlImmediately(true).build(),
            new Record.Builder(new DocId("3"))
                .setMetadata(null).setCrawlImmediately(true).build()),
        pusher.getRecords());

    // With a GSA_TIMESTAMP column, we use the latest timestamp value.
    pusher.reset();
    lister.getModifiedDocIds(pusher);
    assertEquals(
        Arrays.asList(
            new Record.Builder(new DocId("2"))
                .setMetadata(null).setCrawlImmediately(true).build()),
        pusher.getRecords());
  }

  @Test
  public void testGetModifiedDocIds_nullGsaTimestamp() throws Exception {
    // Subtract time to show the records as unchanged.
    // Add time to show the records as modified.
    executeUpdate("create table data(id integer, ts timestamp)");
    executeUpdate("insert into data(id, ts) values "
        + "(1, dateadd('minute', 1, current_timestamp())),"
        + "(2, dateadd('minute', 2, current_timestamp())),"
        + "(3, dateadd('minute', 1, current_timestamp())),"
        + "(4, dateadd('minute', -1, current_timestamp()))");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.updateSql",
        "select id, null as gsa_timestamp from data where ts >= ? order by id");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "select id from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

    List<Record> golden =
        Arrays.asList(
            new Record.Builder(new DocId("1"))
                .setMetadata(null).setCrawlImmediately(true).build(),
            new Record.Builder(new DocId("2"))
                .setMetadata(null).setCrawlImmediately(true).build(),
            new Record.Builder(new DocId("3"))
                .setMetadata(null).setCrawlImmediately(true).build());

    PollingIncrementalLister lister = getPollingIncrementalLister(moreEntries);
    RecordingDocIdPusher pusher = new RecordingDocIdPusher();
    lister.getModifiedDocIds(pusher);
    assertEquals(golden, pusher.getRecords());

    // With a null GSA_TIMESTAMP column, we continue using the current time.
    pusher.reset();
    lister.getModifiedDocIds(pusher);
    assertEquals(golden, pusher.getRecords());
  }

  @Test
  public void testGetModifiedDocIds_timezone() throws Exception {
    // Add time to show the records as modified.
    executeUpdate("create table data(id integer, ts timestamp)");
    executeUpdate("insert into data(id, ts) values "
        + "(1, dateadd('minute', 1, current_timestamp()))");

    // Get the (possibly fictional or impossible) time zone to the east of us.
    long offset = TimeZone.getDefault().getOffset(new Date().getTime());
    long hours = TimeUnit.HOURS.convert(offset, TimeUnit.MILLISECONDS);
    String aheadOneHour = String.format("GMT%+d", hours + 1);

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.updateSql",
        "select id, ts as gsa_timestamp from data where ts >= ? order by id");
    moreEntries.put("db.updateTimestampTimezone", aheadOneHour);
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "select id from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

    PollingIncrementalLister lister = getPollingIncrementalLister(moreEntries);
    RecordingDocIdPusher pusher = new RecordingDocIdPusher();
    lister.getModifiedDocIds(pusher);
    assertEquals(Arrays.asList(), pusher.getRecords());
  }

  @Test
  public void testGetModifiedDocIds_docIdIsUrl() throws Exception {
    // Add time to show the records as modified.
    executeUpdate("create table data(url varchar, ts timestamp)");
    executeUpdate("insert into data(url, ts) values "
        + "('http://host/foo', dateadd('minute', 1, current_timestamp())),"
        + "('foo/bar', dateadd('minute', 1, current_timestamp())),"
        + "('http://host/bar', dateadd('minute', 1, current_timestamp())),"
        + "('http://host/foo bar', dateadd('minute', 1, current_timestamp()))");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    moreEntries.put("docId.isUrl", "true");
    moreEntries.put("db.uniqueKey", "url:string");
    moreEntries.put("db.updateSql", "select url from data where ts >= ?");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "select url from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where url = ?");

    PollingIncrementalLister lister = getPollingIncrementalLister(moreEntries);
    RecordingDocIdPusher pusher = new RecordingDocIdPusher();
    List<String> messages = new ArrayList<String>();
    captureLogMessages(DatabaseAdaptor.class, "Invalid DocId URL", messages);
    lister.getModifiedDocIds(pusher);

    assertEquals(messages.toString(), 1, messages.size());
    assertEquals(
        Arrays.asList(
            new Record.Builder(new DocId("http://host/foo"))
            .setCrawlImmediately(true).build(),
            new Record.Builder(new DocId("http://host/bar"))
            .setCrawlImmediately(true).build(),
            new Record.Builder(new DocId("http://host/foo%20bar"))
            .setCrawlImmediately(true).build()),
        pusher.getRecords());
  }

  @Test
  public void testGetModifiedDocIds_urlAndMetadataLister() throws Exception {
    // Add time to show the records as modified.
    executeUpdate("create table data(url varchar,"
        + " other varchar, ts timestamp)");
    executeUpdate("insert into data(url, other, ts) values ('http://localhost',"
        + " 'hello world', dateadd('minute', 1, current_timestamp()))");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    moreEntries.put("docId.isUrl", "true");
    moreEntries.put("db.uniqueKey", "url:string");
    moreEntries.put("db.updateSql",
        "select url, other from data where ts >= ?");
    moreEntries.put("db.metadataColumns", "other");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "select url, other from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where url = ?");

    PollingIncrementalLister lister = getPollingIncrementalLister(moreEntries);
    RecordingDocIdPusher pusher = new RecordingDocIdPusher();
    lister.getModifiedDocIds(pusher);

    Metadata metadata = new Metadata();
    metadata.add("other",  "hello world");
    assertEquals(
        Arrays.asList(
            new Record.Builder(new DocId("http://localhost"))
            .setMetadata(metadata).setCrawlImmediately(true).build()),
        pusher.getRecords());
  }


  @Test
  public void testGetModifiedDocIds_disableStreaming() throws Exception {
    // Subtract time to show the records as unchanged.
    // Add time to show the records as modified.
    executeUpdate("create table data(id integer, ts timestamp)");
    executeUpdate("insert into data(id, ts) values "
        + "(1, dateadd('minute', 1, current_timestamp())),"
        + "(2, dateadd('minute', 2, current_timestamp())),"
        + "(3, dateadd('minute', 1, current_timestamp())),"
        + "(4, dateadd('minute', -1, current_timestamp()))");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.updateSql",
        "select id, ts as gsa_timestamp from data where ts >= ? order by id");
    moreEntries.put("db.disableStreaming", "true");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "select id from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

    PollingIncrementalLister lister = getPollingIncrementalLister(moreEntries);
    RecordingDocIdPusher pusher = new RecordingDocIdPusher();
    lister.getModifiedDocIds(pusher);
    assertEquals(
        Arrays.asList(
            new Record.Builder(new DocId("1"))
                .setMetadata(null).setCrawlImmediately(true).build(),
            new Record.Builder(new DocId("2"))
                .setMetadata(null).setCrawlImmediately(true).build(),
            new Record.Builder(new DocId("3"))
                .setMetadata(null).setCrawlImmediately(true).build()),
        pusher.getRecords());
  }

  @Test
  public void testGetModifiedDocIds_sqlException() throws Exception {
    // Create a SQLException by selecting an integer column as GSA_TIMESTAMP.
    // Add time to show the records as modified.
    executeUpdate("create table data(id integer, ts timestamp)");
    executeUpdate("insert into data(id, ts) values "
        + "(1, dateadd('minute', 1, current_timestamp()))");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.updateSql",
        "select id, id as gsa_timestamp from data where ts >= ? order by id");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "select id from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

    PollingIncrementalLister lister = getPollingIncrementalLister(moreEntries);
    RecordingDocIdPusher pusher = new RecordingDocIdPusher();
    thrown.expect(IOException.class);
    thrown.expectCause(isA(SQLException.class));
    lister.getModifiedDocIds(pusher);
  }

  @Test
  public void testGetDocContent() throws Exception {
    executeUpdate("create table data(ID  integer, NAME  varchar)");
    executeUpdate("insert into data(ID, NAME) values('1001', 'John')");

    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.uniqueKey", "ID:int");
    configEntries.put("db.everyDocIdSql", "select id from data");
    configEntries.put("db.singleDocContentSql",
        "select * from data where ID = ?");
    configEntries.put("db.modeOfOperation", "rowToText");
    configEntries.put("db.metadataColumns", "ID:col1, NAME:col2");

    DatabaseAdaptor adaptor = getObjectUnderTest(configEntries);
    MockRequest request = new MockRequest(new DocId("1001"));
    RecordingResponse response = new RecordingResponse();
    adaptor.getDocContent(request, response);

    Metadata metadata = new Metadata();
    metadata.add("col1",  "1001");
    metadata.add("col2",  "John");
    assertEquals(metadata, response.getMetadata());
  }

  @Test
  public void testGetDocContent_noUniqueKeyFieldsInSelect() throws Exception {
    executeUpdate("create table data(id integer, content varchar)");
    executeUpdate("insert into data(id, content) values('1', 'Hello World')");

    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.uniqueKey", "id:int");
    configEntries.put("db.everyDocIdSql", "select id from data");
    configEntries.put("db.singleDocContentSql",
        "select content from data where id = ?");
    configEntries.put("db.modeOfOperation", "contentColumn");
    configEntries.put("db.modeOfOperation.contentColumn.columnName", "content");

    DatabaseAdaptor adaptor = getObjectUnderTest(configEntries);
    MockRequest request = new MockRequest(new DocId("1"));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RecordingResponse response = new RecordingResponse(baos);
    adaptor.getDocContent(request, response);
    assertEquals("Hello World", baos.toString(UTF_8.toString()));
  }

  @Test
  public void testGetDocContent_wrongParameterColumnInQuery() throws Exception {
    executeUpdate("create table data(id integer, notId integer)");
    executeUpdate("insert into data(id, notId) values(1, 0), (2, 1)");

    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.uniqueKey", "id:int");
    configEntries.put("db.everyDocIdSql", "select id from data");
    configEntries.put("db.singleDocContentSql",
        "select * from data where notId = ?");
    configEntries.put("db.modeOfOperation", "rowToText");
    configEntries.put("db.metadataColumns", "id:id, notId:notId");

    DatabaseAdaptor adaptor = getObjectUnderTest(configEntries);
    MockRequest request = new MockRequest(new DocId("1"));
    RecordingResponse response = new RecordingResponse();
    adaptor.getDocContent(request, response);

    Metadata metadata = new Metadata();
    metadata.add("id", "2");
    metadata.add("notId", "1");
    assertEquals(metadata, response.getMetadata());
  }

  @Test
  public void testGetDocContent_lister() throws Exception {
    executeUpdate("create table data(url varchar, name varchar)");
    executeUpdate("insert into data(url, name) values('http://', 'John')");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("docId.isUrl", "true");
    moreEntries.put("db.uniqueKey", "url:string");
    moreEntries.put("db.everyDocIdSql", "select * from data");

    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);
    MockRequest request = new MockRequest(new DocId("http://"));
    RecordingResponse response = new RecordingResponse();
    adaptor.getDocContent(request, response);
    assertEquals(RecordingResponse.State.NOT_FOUND, response.getState());
   }

  @Test
  public void testGetDocContent_noResults() throws Exception {
    executeUpdate("create table data(id integer, name varchar)");
    executeUpdate("insert into data(id, name) values(1001, 'John')");

    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.uniqueKey", "ID:int");
    configEntries.put("db.everyDocIdSql", "select id from data");
    configEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");
    configEntries.put("db.modeOfOperation", "rowToText");

    DatabaseAdaptor adaptor = getObjectUnderTest(configEntries);
    MockRequest request = new MockRequest(new DocId("1002"));
    RecordingResponse response = new RecordingResponse();
    adaptor.getDocContent(request, response);
    assertEquals(RecordingResponse.State.NOT_FOUND, response.getState());
  }

  @Test
  public void testGetDocContent_sqlException() throws Exception {
    // Simulate a SQLException by creating a table for init
    // but removing it for getDocContent.
    executeUpdate("create table data(id integer, name varchar)");

    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.uniqueKey", "ID:int");
    configEntries.put("db.everyDocIdSql", "select id from data");
    configEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");
    configEntries.put("db.modeOfOperation", "rowToText");

    DatabaseAdaptor adaptor = getObjectUnderTest(configEntries);

    executeUpdate("drop table data");

    MockRequest request = new MockRequest(new DocId("1002"));
    RecordingResponse response = new RecordingResponse();
    thrown.expect(IOException.class);
    thrown.expectMessage("retrieval error");
    thrown.expectCause(isA(SQLException.class));
    adaptor.getDocContent(request, response);
  }

  @Test
  public void testMetadataColumns_date() throws Exception {
    executeUpdate("create table data(id integer, col date)");
    executeUpdate("insert into data(id, col) values(1001, {d '2004-10-06'})");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select * from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where ID = ?");
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.metadataColumns", "ID:col1, COL:col2");

    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);
    MockRequest request = new MockRequest(new DocId("1001"));
    RecordingResponse response = new RecordingResponse();
    adaptor.getDocContent(request, response);

    Metadata metadata = new Metadata();
    metadata.add("col1", "1001");
    metadata.add("col2", "2004-10-06");
    assertEquals(metadata, response.getMetadata());
  }

  @Test
  public void testMetadataColumns_dateNull() throws Exception {
    executeUpdate("create table data(id integer, col date)");
    executeUpdate("insert into data(id) values(1001)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select * from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where ID = ?");
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.metadataColumns", "ID:col1, COL:col2");

    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);
    MockRequest request = new MockRequest(new DocId("1001"));
    RecordingResponse response = new RecordingResponse();
    adaptor.getDocContent(request, response);

    Metadata metadata = new Metadata();
    metadata.add("col1", "1001");
    assertEquals(metadata, response.getMetadata());
  }

  @Test
  public void testMetadataColumns_time() throws Exception {
    executeUpdate("create table data(id integer, col time)");
    executeUpdate("insert into data(id, col) values(1001, {t '09:15:30'})");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select * from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where ID = ?");
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.metadataColumns", "ID:col1, COL:col2");

    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);
    MockRequest request = new MockRequest(new DocId("1001"));
    RecordingResponse response = new RecordingResponse();
    adaptor.getDocContent(request, response);

    Metadata metadata = new Metadata();
    metadata.add("col1", "1001");
    metadata.add("col2", "09:15:30");
    assertEquals(metadata, response.getMetadata());
  }

  @Test
  public void testMetadataColumns_timeNull() throws Exception {
    executeUpdate("create table data(id integer, col time)");
    executeUpdate("insert into data(id) values(1001)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select * from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where ID = ?");
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.metadataColumns", "ID:col1, COL:col2");

    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);
    MockRequest request = new MockRequest(new DocId("1001"));
    RecordingResponse response = new RecordingResponse();
    adaptor.getDocContent(request, response);

    Metadata metadata = new Metadata();
    metadata.add("col1", "1001");
    assertEquals(metadata, response.getMetadata());
  }

  @Test
  public void testMetadataColumns_timestamp() throws Exception {
    executeUpdate("create table data(id integer, col timestamp)");
    executeUpdate("insert into data(id, col) values(1001, "
        + "{ts '2009-10-05 09:20:49.512'})");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select * from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where ID = ?");
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.metadataColumns", "ID:col1, COL:col2");

    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);
    MockRequest request = new MockRequest(new DocId("1001"));
    RecordingResponse response = new RecordingResponse();
    adaptor.getDocContent(request, response);

    Metadata metadata = new Metadata();
    metadata.add("col1", "1001");
    metadata.add("col2", "2009-10-05 09:20:49.512");
    assertEquals(metadata, response.getMetadata());
  }

  @Test
  public void testMetadataColumns_timestampNull() throws Exception {
    executeUpdate("create table data(id integer, col timestamp)");
    executeUpdate("insert into data(id) values(1001)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select * from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where ID = ?");
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.metadataColumns", "ID:col1, COL:col2");

    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);
    MockRequest request = new MockRequest(new DocId("1001"));
    RecordingResponse response = new RecordingResponse();
    adaptor.getDocContent(request, response);

    Metadata metadata = new Metadata();
    metadata.add("col1", "1001");
    assertEquals(metadata, response.getMetadata());
  }

  @Test
  public void testMetadataColumns_clob() throws Exception {
    // NCLOB shows up as CLOB in H2
    String content = "Hello World";
    executeUpdate("create table data(id int, content clob)");
    String sql = "insert into data(id, content) values (1, ?)";
    try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
      ps.setString(1, content);
      assertEquals(1, ps.executeUpdate());
    }

    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.uniqueKey", "ID:int");
    configEntries.put("db.everyDocIdSql", "select * from data");
    configEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");
    configEntries.put("db.modeOfOperation", "rowToText");
    configEntries.put("db.metadataColumns", "ID:col1, CONTENT:col2");

    DatabaseAdaptor adaptor = getObjectUnderTest(configEntries);
    MockRequest request = new MockRequest(new DocId("1"));
    RecordingResponse response = new RecordingResponse();
    adaptor.getDocContent(request, response);

    Metadata expected = new Metadata();
    expected.add("col1", "1");
    expected.add("col2", content);
    assertEquals(expected, response.getMetadata());
  }

  @Test
  public void testMetadataColumns_clobNull() throws Exception {
    executeUpdate("create table data(id int, content clob)");
    executeUpdate("insert into data(id) values (1)");

    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.uniqueKey", "ID:int");
    configEntries.put("db.everyDocIdSql", "select * from data");
    configEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");
    configEntries.put("db.modeOfOperation", "rowToText");
    configEntries.put("db.metadataColumns", "ID:col1, CONTENT:col2");

    DatabaseAdaptor adaptor = getObjectUnderTest(configEntries);
    MockRequest request = new MockRequest(new DocId("1"));
    RecordingResponse response = new RecordingResponse();
    adaptor.getDocContent(request, response);

    Metadata expected = new Metadata();
    expected.add("col1", "1");
    assertEquals(expected, response.getMetadata());
  }

  @Test
  public void testMetadataColumns_blob() throws Exception {
    String content = "hello world";
    executeUpdate("create table data(id int, content blob)");
    String sql = "insert into data(id, content) values (1, ?)";
    try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
      ps.setBytes(1, content.getBytes(UTF_8));
      assertEquals(1, ps.executeUpdate());
    }

    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.uniqueKey", "ID:int");
    configEntries.put("db.everyDocIdSql", "select * from data");
    configEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");
    configEntries.put("db.modeOfOperation", "rowToText");
    configEntries.put("db.metadataColumns", "id, content");

    List<String> messages = new ArrayList<String>();
    captureLogMessages(DatabaseAdaptor.class,
        "Metadata column type not supported", messages);
    DatabaseAdaptor adaptor = getObjectUnderTest(configEntries);
    MockRequest request = new MockRequest(new DocId("1"));
    RecordingResponse response = new RecordingResponse();
    adaptor.getDocContent(request, response);

    assertEquals(messages.toString(), 1, messages.size());
    Metadata expected = new Metadata();
    expected.add("id", "1");
    assertEquals(expected, response.getMetadata());
  }

  @Test
  public void testMetadataColumns_blobNull() throws Exception {
    executeUpdate("create table data(id int, content blob)");
    executeUpdate("insert into data(id) values (1)");

    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.uniqueKey", "ID:int");
    configEntries.put("db.everyDocIdSql", "select * from data");
    configEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");
    configEntries.put("db.modeOfOperation", "rowToText");
    configEntries.put("db.metadataColumns", "id, content");

    List<String> messages = new ArrayList<String>();
    captureLogMessages(DatabaseAdaptor.class,
        "Metadata column type not supported", messages);
    DatabaseAdaptor adaptor = getObjectUnderTest(configEntries);
    MockRequest request = new MockRequest(new DocId("1"));
    RecordingResponse response = new RecordingResponse();
    adaptor.getDocContent(request, response);

    assertEquals(messages.toString(), 1, messages.size());
    Metadata expected = new Metadata();
    expected.add("id", "1");
    assertEquals(expected, response.getMetadata());
  }

  @Test
  public void testMetadataColumns_array() throws Exception {
    String[] content = { "hello", "world" };
    executeUpdate("create table data(id int, content array)");
    String sql = "insert into data(id, content) values (1, ?)";
    try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
      ps.setObject(1, content);
      assertEquals(1, ps.executeUpdate());
    }

    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.uniqueKey", "ID:int");
    configEntries.put("db.everyDocIdSql", "select * from data");
    configEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");
    configEntries.put("db.modeOfOperation", "rowToText");
    configEntries.put("db.metadataColumns", "id, content");

    List<String> messages = new ArrayList<String>();
    captureLogMessages(DatabaseAdaptor.class,
        "Metadata column type not supported", messages);
    DatabaseAdaptor adaptor = getObjectUnderTest(configEntries);
    MockRequest request = new MockRequest(new DocId("1"));
    RecordingResponse response = new RecordingResponse();
    adaptor.getDocContent(request, response);

    assertEquals(messages.toString(), 1, messages.size());
    Metadata expected = new Metadata();
    expected.add("id", "1");
    assertEquals(expected, response.getMetadata());
  }

  @Test
  public void testMetadataColumns_arrayNull() throws Exception {
    executeUpdate("create table data(id int, content array)");
    executeUpdate("insert into data(id) values (1)");

    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.uniqueKey", "ID:int");
    configEntries.put("db.everyDocIdSql", "select * from data");
    configEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");
    configEntries.put("db.modeOfOperation", "rowToText");
    configEntries.put("db.metadataColumns", "id, content");

    List<String> messages = new ArrayList<String>();
    captureLogMessages(DatabaseAdaptor.class,
        "Metadata column type not supported", messages);
    DatabaseAdaptor adaptor = getObjectUnderTest(configEntries);
    MockRequest request = new MockRequest(new DocId("1"));
    RecordingResponse response = new RecordingResponse();
    adaptor.getDocContent(request, response);

    assertEquals(messages.toString(), 1, messages.size());
    Metadata expected = new Metadata();
    expected.add("id", "1");
    assertEquals(expected, response.getMetadata());
  }

  @Test
  public void testMetadataColumns_integer() throws Exception {
    executeUpdate("create table data(id int, content integer)");
    executeUpdate("insert into data(id, content) values (1, 345697)");

    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.uniqueKey", "ID:int");
    configEntries.put("db.everyDocIdSql", "select * from data");
    configEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");
    configEntries.put("db.modeOfOperation", "rowToText");
    configEntries.put("db.metadataColumns", "ID:col1, CONTENT:col2");

    DatabaseAdaptor adaptor = getObjectUnderTest(configEntries);
    MockRequest request = new MockRequest(new DocId("1"));
    RecordingResponse response = new RecordingResponse();
    adaptor.getDocContent(request, response);

    Metadata expected = new Metadata();
    expected.add("col1", "1");
    expected.add("col2", "345697");
    assertEquals(expected, response.getMetadata());
  }

  @Test
  public void testMetadataColumns_varchar() throws Exception {
    // LONGVARCHAR, LONGNVARCHAR show up as VARCHAR in H2.
    String content = "Hello World";
    executeUpdate("create table data(id int, content varchar)");
    String sql = "insert into data(id, content) values (1, ?)";
    try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
      ps.setString(1, content);
      assertEquals(1, ps.executeUpdate());
    }

    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.uniqueKey", "ID:int");
    configEntries.put("db.everyDocIdSql", "select * from data");
    configEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");
    configEntries.put("db.modeOfOperation", "rowToText");
    configEntries.put("db.metadataColumns", "ID:col1, CONTENT:col2");

    DatabaseAdaptor adaptor = getObjectUnderTest(configEntries);
    MockRequest request = new MockRequest(new DocId("1"));
    RecordingResponse response = new RecordingResponse();
    adaptor.getDocContent(request, response);

    Metadata expected = new Metadata();
    expected.add("col1", "1");
    expected.add("col2", content);
    assertEquals(expected, response.getMetadata());
  }

  @Test
  public void testMetadataColumns_columnAlias() throws Exception {
    String content = "Who is number 1?";
    executeUpdate("create table data(id int, content varchar)");
    String sql = "insert into data(id, content) values (6, ?)";
    try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
      ps.setString(1, content);
      assertEquals(1, ps.executeUpdate());
    }

    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.uniqueKey", "id:int");
    configEntries.put("db.everyDocIdSql", "select * from data");
    configEntries.put("db.singleDocContentSql",
        "select id, content as quote from data where id = ?");
    configEntries.put("db.modeOfOperation", "rowToText");
    configEntries.put("db.metadataColumns", "id, quote");

    DatabaseAdaptor adaptor = getObjectUnderTest(configEntries);
    MockRequest request = new MockRequest(new DocId("6"));
    RecordingResponse response = new RecordingResponse();
    adaptor.getDocContent(request, response);

    Metadata expected = new Metadata();
    expected.add("id", "6");
    expected.add("quote", content);
    assertEquals(expected, response.getMetadata());
  }

  @Test
  public void testGetDocContentAcl() throws Exception {
    executeUpdate("create table data(id integer)");
    executeUpdate("insert into data(id) values (1001), (1002)");
    executeUpdate("create table acl(id int, allowed_groups varchar,"
        + " allowed_users varchar)");
    executeUpdate("insert into acl(id, allowed_groups, allowed_users) "
        + "values (1001, 'pgroup1', 'puser1'), (1002, 'pgroup2', 'puser2')");

    String aclSql = "select id, allowed_groups as gsa_permit_groups, "
        + "allowed_users as gsa_permit_users from acl where id = ?";

    ResultSet rs = executeQuery(aclSql.replace("?", "'1001'"));
    ResultSetMetaData rsmd = rs.getMetaData();
    assertEquals("allowed_groups", rsmd.getColumnName(2).toLowerCase(US));
    assertEquals("gsa_permit_groups", rsmd.getColumnLabel(2).toLowerCase(US));

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select * from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");
    moreEntries.put("db.aclSql", aclSql);
    moreEntries.put("db.modeOfOperation", "rowToText");

    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);
    MockRequest request = new MockRequest(new DocId("1001"));
    RecordingResponse response = new RecordingResponse();
    adaptor.getDocContent(request, response);

    Acl golden = new Acl.Builder()
        .setPermitUsers(Arrays.asList(new UserPrincipal("puser1")))
        .setPermitGroups(Arrays.asList(new GroupPrincipal("pgroup1")))
        .build();
    assertEquals(golden, response.getAcl());
  }

  @Test
  public void testGetDocContentAcl_empty() throws Exception {
    executeUpdate("create table data(id integer)");
    executeUpdate("insert into data(id) values (1001)");
    executeUpdate("create table acl(id integer)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select * from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where ID = ?");
    moreEntries.put("db.aclSql", "select * from acl where id = ?");
    moreEntries.put("db.modeOfOperation", "rowToText");

    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);
    MockRequest request = new MockRequest(new DocId("1001"));
    RecordingResponse response = new RecordingResponse();
    adaptor.getDocContent(request, response);

    assertEquals(Acl.EMPTY, response.getAcl());
  }

  @Test
  public void testGetDocContentAcl_sqlException() throws Exception {
    executeUpdate("create table data(id integer)");
    executeUpdate("insert into data(id) values (1001)");
    // Simulate a SQLException by creating a table for init
    // but removing it for getDocContent.
    executeUpdate("create table acl(id integer)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select * from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where ID = ?");
    moreEntries.put("db.aclSql", "select * from acl where id = ?");
    moreEntries.put("db.modeOfOperation", "rowToText");

    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries);

    executeUpdate("drop table acl");

    MockRequest request = new MockRequest(new DocId("1001"));
    RecordingResponse response = new RecordingResponse();
    thrown.expect(IOException.class);
    thrown.expectMessage("retrieval error");
    thrown.expectCause(isA(SQLException.class));
    adaptor.getDocContent(request, response);
  }

  private AuthzAuthority getAuthzAuthority(Map<String, String> moreEntries)
      throws Exception {
    Holder<RecordingContext> contextHolder = new Holder<>();
    DatabaseAdaptor adaptor = getObjectUnderTest(moreEntries, contextHolder);
    return contextHolder.get().getAuthzAuthority();
  }

  private AuthnIdentity getAuthnIdentity(final UserPrincipal user) {
    return new AuthnIdentity() {
      @Override public UserPrincipal getUser() {
        return user;
      }

      @Override public String getPassword() {
        return null;
      }

      @Override public Set<GroupPrincipal> getGroups() {
        return new HashSet<>();
      }
    };
  }

  @Test
  public void testAuthzAuthority_public() throws Exception {
    Map<String, String> moreEntries = new HashMap<String, String>();
    // Required for validation, but not specific to this test.
    executeUpdate("create table data(id int)");
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select id from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

    AuthzAuthority authority = getAuthzAuthority(moreEntries);
    Map<DocId, AuthzStatus> answers = authority.isUserAuthorized(
        getAuthnIdentity(new UserPrincipal("alice")),
        Arrays.asList(new DocId("1"), new DocId("2")));

    HashMap<DocId, AuthzStatus> golden = new HashMap<>();
    golden.put(new DocId("1"), AuthzStatus.PERMIT);
    golden.put(new DocId("2"), AuthzStatus.PERMIT);
    assertEquals(golden, answers);
  }

  @Test
  public void testAuthzAuthorityAcl_nullIdentity() throws Exception {
    executeUpdate("create table acl(id integer)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.aclSql", "select * from acl where id = ?");
    // Required for validation, but not specific to this test.
    executeUpdate("create table data(id int)");
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select id from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

    AuthzAuthority authority = getAuthzAuthority(moreEntries);
    Map<DocId, AuthzStatus> answers = authority.isUserAuthorized(
        null,
        Arrays.asList(new DocId("1"), new DocId("2")));

    HashMap<DocId, AuthzStatus> golden = new HashMap<>();
    golden.put(new DocId("1"), AuthzStatus.DENY);
    golden.put(new DocId("2"), AuthzStatus.DENY);
    assertEquals(golden, answers);
  }

  @Test
  public void testAuthzAuthorityAcl_nullUser() throws Exception {
    executeUpdate("create table acl(id integer)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.aclSql", "select * from acl where id = ?");
    // Required for validation, but not specific to this test.
    executeUpdate("create table data(id int)");
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select id from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

    AuthzAuthority authority = getAuthzAuthority(moreEntries);
    Map<DocId, AuthzStatus> answers = authority.isUserAuthorized(
        getAuthnIdentity(null),
        Arrays.asList(new DocId("1"), new DocId("2")));

    HashMap<DocId, AuthzStatus> golden = new HashMap<>();
    golden.put(new DocId("1"), AuthzStatus.DENY);
    golden.put(new DocId("2"), AuthzStatus.DENY);
    assertEquals(golden, answers);
  }

  @Test
  public void testAuthzAuthorityAcl_noResults() throws Exception {
    executeUpdate("create table acl(id integer)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.aclSql", "select * from acl where id = ?");
    // Required for validation, but not specific to this test.
    executeUpdate("create table data(id int)");
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select id from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

    AuthzAuthority authority = getAuthzAuthority(moreEntries);
    Map<DocId, AuthzStatus> answers = authority.isUserAuthorized(
        getAuthnIdentity(new UserPrincipal("alice")),
        Arrays.asList(new DocId("1"), new DocId("2")));

    HashMap<DocId, AuthzStatus> golden = new HashMap<>();
    golden.put(new DocId("1"), AuthzStatus.INDETERMINATE);
    golden.put(new DocId("2"), AuthzStatus.INDETERMINATE);
    assertEquals(golden, answers);
  }

  @Test
  public void testAuthzAuthorityAcl_permit() throws Exception {
    executeUpdate("create table acl(id integer, gsa_permit_users varchar)");
    executeUpdate("insert into acl(id, gsa_permit_users) values "
        + "(2, 'alice')");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.aclSql", "select * from acl where id = ?");
    // Required for validation, but not specific to this test.
    executeUpdate("create table data(id int)");
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select id from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

    AuthzAuthority authority = getAuthzAuthority(moreEntries);
    Map<DocId, AuthzStatus> answers = authority.isUserAuthorized(
        getAuthnIdentity(new UserPrincipal("alice")),
        Arrays.asList(new DocId("1"), new DocId("2")));

    HashMap<DocId, AuthzStatus> golden = new HashMap<>();
    golden.put(new DocId("1"), AuthzStatus.INDETERMINATE);
    golden.put(new DocId("2"), AuthzStatus.PERMIT);
    assertEquals(golden, answers);
  }

  @Test
  public void testAuthzAuthorityAcl_sqlException() throws Exception {
    // Simulate a SQLException by creating a table for init
    // but removing it for isUserAuthorized.
    executeUpdate("create table acl(id integer)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.aclSql", "select * from acl where id = ?");
    // Required for validation, but not specific to this test.
    executeUpdate("create table data(id int)");
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select id from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

    AuthzAuthority authority = getAuthzAuthority(moreEntries);

    executeUpdate("drop table acl");

    thrown.expect(IOException.class);
    thrown.expectMessage("authz retrieval error");
    thrown.expectCause(isA(SQLException.class));
    Map<DocId, AuthzStatus> answers = authority.isUserAuthorized(
        getAuthnIdentity(new UserPrincipal("alice")),
        Arrays.asList(new DocId("1"), new DocId("2")));
  }
}
