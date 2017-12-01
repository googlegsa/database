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

import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

/** Test cases for {@link MetadataColumns}. */
public class MetadataColumnsTest {
  @SuppressWarnings("unchecked")
  private static <T> Map<T, T> asMap(T... entries) {
    if (entries.length % 2 != 0) {
      throw new IllegalArgumentException(
          "A map requires an even number of entries");
    }
    Map<T, T> map = new HashMap<>();
    for (int i = 0; i < entries.length - 1; i += 2) {
      map.put(entries[i], entries[i + 1]);
    }
    return map;
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testEmptyIsAllowed() {
    MetadataColumns mc = new MetadataColumns(" ");
  }

  @Test
  public void testMissingColumnNameGivesNull() {
    MetadataColumns mc = new MetadataColumns(" ");
    assertEquals(null, mc.get("not-exist"));
  }

  @Test
  public void testSimpleCase() {
    String configDef = "xf_date:CREATE_DATE,name:AUTHOR";
    MetadataColumns mc = new MetadataColumns(configDef);
    assertEquals(asMap("xf_date", "CREATE_DATE", "name", "AUTHOR"), mc);
  }

  @Test
  public void testColonInMetadataName() {
    String configDef = "xf_date:DATE:CREATE";
    MetadataColumns mc = new MetadataColumns(configDef);
    assertEquals(asMap("xf_date", "DATE:CREATE"), mc);
  }

  @Test
  public void testIdentityColumns() {
    String configDef = "id, name, account:acct";
    MetadataColumns mc = new MetadataColumns(configDef);
    assertEquals(asMap("id", "id", "name", "name", "account", "acct"), mc);
  }

  @Test
  public void testEmptyEntry() {
    String configDef = "id,,name";
    MetadataColumns mc = new MetadataColumns(configDef);
    assertEquals(asMap("id", "id", "name", "name"), mc);
  }

  @Test
  public void testEmptyMetadataKey() {
    String configDef = "id:,name";
    MetadataColumns mc = new MetadataColumns(configDef);
    assertEquals(asMap("id", "id", "name", "name"), mc);
  }

  @Test
  public void testSpacesBetweenDeclerations() {
    String configDef = "xf_date:CREATE_DATE,name:AUTHOR";
    MetadataColumns mc = new MetadataColumns(configDef);
    assertEquals(mc, new MetadataColumns("xf_date:CREATE_DATE ,name:AUTHOR"));
    assertEquals(mc, new MetadataColumns("xf_date:CREATE_DATE, name:AUTHOR"));
    assertEquals(mc, new MetadataColumns("xf_date:CREATE_DATE , name:AUTHOR"));
    assertEquals(mc, new MetadataColumns(" xf_date:CREATE_DATE ,name:AUTHOR "));
  }

  @Test
  public void testSpacesWithinDeclerations() {
    String configDef = "xf:CREATE_DATE,name:AUTHOR";
    MetadataColumns mc = new MetadataColumns(configDef);
    assertEquals(mc, new MetadataColumns("xf :CREATE_DATE,name:AUTHOR"));
    assertEquals(mc, new MetadataColumns("xf: CREATE_DATE,name:AUTHOR"));
    assertEquals(mc, new MetadataColumns("xf : CREATE_DATE,name:AUTHOR"));
    assertEquals(mc, new MetadataColumns("xf  :   CREATE_DATE,name : AUTHOR "));
    assertEquals(mc, new MetadataColumns(" xf  :  CREATE_DATE, name:AUTHOR "));
  }
}
