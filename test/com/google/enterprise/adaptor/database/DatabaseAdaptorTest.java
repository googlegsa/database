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
import static org.hamcrest.CoreMatchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.enterprise.adaptor.Acl;
import com.google.enterprise.adaptor.AuthnIdentity;
import com.google.enterprise.adaptor.AuthzAuthority;
import com.google.enterprise.adaptor.AuthzStatus;
import com.google.enterprise.adaptor.Config;
import com.google.enterprise.adaptor.DocId;
import com.google.enterprise.adaptor.GroupPrincipal;
import com.google.enterprise.adaptor.InvalidConfigurationException;
import com.google.enterprise.adaptor.Metadata;
import com.google.enterprise.adaptor.Response;
import com.google.enterprise.adaptor.TestHelper;
import com.google.enterprise.adaptor.TestHelper.RecordingContext;
import com.google.enterprise.adaptor.UserPrincipal;
import com.google.enterprise.adaptor.database.DatabaseAdaptor.GsaSpecialColumns;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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
  //         getDocContent
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
  public void testLoadResponseGeneratorWithFactoryMethod() {
    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.modeOfOperation", "rowToText");
    final Config config = new Config();
    for (Map.Entry<String, String> entry : configEntries.entrySet()) {
      TestHelper.setConfigValue(config, entry.getKey(), entry.getValue());
    }
    assertNotNull("loaded response generator is null",
        DatabaseAdaptor.loadResponseGenerator(config));
  }

  @Test
  public void testLoadResponseGeneratorWithBuiltinFullyQualifiedMethod() {
    Map<String, String> configEntries = new HashMap<String, String>();
    String modeOfOperation = "com.google.enterprise.adaptor.database"
        + ".ResponseGenerator.rowToText";
    configEntries.put("db.modeOfOperation", modeOfOperation);
    final Config config = new Config();
    for (Map.Entry<String, String> entry : configEntries.entrySet()) {
      TestHelper.setConfigValue(config, entry.getKey(), entry.getValue());
    }
    assertNotNull("loaded response generator is null",
        DatabaseAdaptor.loadResponseGenerator(config));
  }

  @Test
  public void testLoadResponseGeneratorWithCustomFullyQualifiedMethod() {
    Map<String, String> configEntries = new HashMap<String, String>();
    String modeOfOperation = "com.google.enterprise.adaptor.database"
        + ".DatabaseAdaptorTest.createDummy";
    configEntries.put("db.modeOfOperation", modeOfOperation);
    final Config config = new Config();
    for (Map.Entry<String, String> entry : configEntries.entrySet()) {
      TestHelper.setConfigValue(config, entry.getKey(), entry.getValue());
    }
    assertNotNull("loaded response generator is null",
        DatabaseAdaptor.loadResponseGenerator(config));
  }

  @Test
  public void testLoadResponseGeneratorModeOfOpNoSuchFactoryMethod() {
    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.modeOfOperation", "noThisMethod");
    final Config config = new Config();
    for (Map.Entry<String, String> entry : configEntries.entrySet()) {
      TestHelper.setConfigValue(config, entry.getKey(), entry.getValue());
    }
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(
        "noThisMethod is not a valid built-in modeOfOperation");
    DatabaseAdaptor.loadResponseGenerator(config);
  }

  @Test
  public void testLoadResponseGeneratorModeOfOpNoSuchClassMethod() {
    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.modeOfOperation",
        "com.google.enterprise.adaptor.database"
            + ".DatabaseAdaptorTest.noThisMode");
    final Config config = new Config();
    for (Map.Entry<String, String> entry : configEntries.entrySet()) {
      TestHelper.setConfigValue(config, entry.getKey(), entry.getValue());
    }
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("No method noThisMode found for class "
        + "com.google.enterprise.adaptor.database.DatabaseAdaptorTest");
    DatabaseAdaptor.loadResponseGenerator(config);
  }

  @Test
  public void testLoadResponseGeneratorModeOfOpNoSuchClass() {
    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.modeOfOperation", "noThisClass.noThisMethod");
    final Config config = new Config();
    for (Map.Entry<String, String> entry : configEntries.entrySet()) {
      TestHelper.setConfigValue(config, entry.getKey(), entry.getValue());
    }
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("No class noThisClass found");
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

  static ResponseGenerator createDummy(Map<String, String> config) {
    return new DummyResponseGenerator(config);
  }

  private static class DummyResponseGenerator extends ResponseGenerator {
    public DummyResponseGenerator(Map<String, String> config) {
      super(config);
    }
    @Override
    public void generateResponse(ResultSet rs, Response resp)
        throws IOException, SQLException {
      // do nothing
    }
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
  public void testInitUniqueKeyMissingType() throws Exception {
    // Value of unique id cannot be "productid", because that is missing type.
    // The value has to be something like "productid:int"
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.uniqueKey", "productid");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.everyDocIdSql", "");
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("Invalid UniqueKey definition");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitUniqueKeyInvalidType() throws Exception {
    // Type of unique key id value cannot be "notvalid", since it's invalid.
    // That cat be int, string, timestamp, date, time, and long.
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.uniqueKey", "productid:notvalid");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.everyDocIdSql", "");
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("Invalid UniqueKey type 'notvalid'");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitUniqueKeyContainsRepeatedKeyName() throws Exception {
    // Value of unique key cannot contain repeated key name.
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.uniqueKey", "productid:int,productid:string");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.everyDocIdSql", "");
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("key name 'productid' was repeated");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitUniqueKeyEmpty() throws Exception {
    // Value of unique id cannot be empty.
    // The value has to be something like "keyname:type"
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.uniqueKey", "");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    moreEntries.put("db.everyDocIdSql", "");
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("db.uniqueKey parameter: value cannot be empty");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitUniqueKeyUrl() throws Exception {
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    moreEntries.put("docId.isUrl", "true");
    moreEntries.put("db.uniqueKey", "id:int, url:string");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "");
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
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    moreEntries.put("db.uniqueKey", "url:string");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "");
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("requires docId.isUrl to be \"true\"");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitUrlMetadataLister_docIdIsUrlFalse() throws Exception {
    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    moreEntries.put("docId.isUrl", "false");
    moreEntries.put("db.uniqueKey", "url:string");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "");
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
  public void testInitVerifyColumnNames_singleDocContentSql() throws Exception {
    executeUpdate("create table data(id int, other varchar)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.singleDocContentSql",
        "select other from data where id = ?");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "select id from data");

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("[id] not found in query");
    getObjectUnderTest(moreEntries);
  }

  @Test
  public void testInitVerifyColumnNames_aclSql() throws Exception {
    executeUpdate("create table data(id int, other varchar)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.aclSql", "select other from data where id = ?");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.everyDocIdSql", "select id from data");
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("[id] not found in query");
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
  public void testInitVerifyColumnNames_actionColumn() throws Exception {
    executeUpdate("create table data(id int, other varchar)");

    Map<String, String> moreEntries = new HashMap<String, String>();
    moreEntries.put("db.modeOfOperation", "rowToText");
    moreEntries.put("db.uniqueKey", "id:int");
    moreEntries.put("db.everyDocIdSql", "select id from data");
    moreEntries.put("db.actionColumn", "other");
    // Required for validation, but not specific to this test.
    moreEntries.put("db.singleDocContentSql",
        "select * from data where id = ?");

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
  public void testGetDocIds() throws Exception {
    executeUpdate("create table data(url varchar, name varchar)");
    executeUpdate("insert into data(url, name) values('http://', 'John')");

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
    metadata.add("col1",  "http://");
    metadata.add("col2",  "John");
    assertEquals(
        Arrays.asList(new Record.Builder(new DocId("http://"))
          .setMetadata(metadata).build()),
        pusher.getRecords());
  }

  @Test
  public void testGetDocIdsActionColumn() throws Exception {
    executeUpdate("create table data(id integer, url varchar, action varchar)");
    executeUpdate("insert into data(id, url, action) values"
        + "('1001', 'http://localhost/?q=1001', 'add'),"
        + "('1002', 'http://localhost/?q=1002', 'delete'),"
        + "('1003', 'http://localhost/?q=1003', 'DELETE'),"
        + "('1004', 'http://localhost/?q=1004', 'foo')");

    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.uniqueKey", "url:string");
    configEntries.put("db.everyDocIdSql", "select * from data order by url");
    configEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    configEntries.put("docId.isUrl", "true");
    configEntries.put("db.metadataColumns", "id:id");
    configEntries.put("db.actionColumn", "action");

    DatabaseAdaptor adaptor = getObjectUnderTest(configEntries);
    RecordingDocIdPusher pusher = new RecordingDocIdPusher();
    adaptor.getDocIds(pusher);

    Metadata metadata1 = new Metadata();
    metadata1.add("id", "1001");
    Metadata metadata4 = new Metadata();
    metadata4.add("id", "1004");
    assertEquals(Arrays.asList(new Record[] {
        new Record.Builder(new DocId("http://localhost/?q=1001"))
            .setMetadata(metadata1).build(),
        new Record.Builder(new DocId("http://localhost/?q=1002"))
            .setDeleteFromIndex(true).build(),
        new Record.Builder(new DocId("http://localhost/?q=1003"))
            .setDeleteFromIndex(true).build(),
        new Record.Builder(new DocId("http://localhost/?q=1004"))
            .setMetadata(metadata4).build()}),
        pusher.getRecords());
  }

  @Test
  public void testGetDocIdsActionColumnMissing() throws Exception {
    // Simulate a skipped column verification by creating the missing
    // column for init but removing it for getDocIds.
    executeUpdate("create table data(id integer, url varchar, action varchar)");
    executeUpdate("insert into data(id, url, action) values"
        + "(1001, 'http://localhost/?q=1001', 'add')");

    Map<String, String> configEntries = new HashMap<String, String>();
    configEntries.put("db.actionColumn", "action");
    configEntries.put("db.uniqueKey", "url:string");
    configEntries.put("db.everyDocIdSql", "select * from data");
    configEntries.put("db.modeOfOperation", "urlAndMetadataLister");
    configEntries.put("docId.isUrl", "true");
    configEntries.put("db.metadataColumns", "id:col1");

    DatabaseAdaptor adaptor = getObjectUnderTest(configEntries);

    executeUpdate("alter table data drop column action");

    RecordingDocIdPusher pusher = new RecordingDocIdPusher();
    thrown.expect(IOException.class);
    thrown.expectCause(isA(SQLException.class));
    thrown.expectMessage("Column \"action\" not found");
    adaptor.getDocIds(pusher);
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
    expected.add("col2", "null");
    assertEquals(expected, response.getMetadata());
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
    metadata.add("col2", "null");
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
    metadata.add("col2", "null");
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
    metadata.add("col2", "null");
    assertEquals(metadata, response.getMetadata());
  }

  @Test
  public void testGetDocContentAcl() throws Exception {
    executeUpdate("create table data(id integer)");
    executeUpdate("insert into data(id) values (1001), (1002)");
    executeUpdate("create table acl(id int, gsa_permit_groups varchar,"
        + " gsa_permit_users varchar)");
    executeUpdate("insert into acl(id, gsa_permit_groups, gsa_permit_users) "
        + "values (1001, 'pgroup1', 'puser1'), (1002, 'pgroup2', 'puser2')");

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
