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

import static org.junit.Assert.*;

import org.junit.*;
import org.junit.rules.ExpectedException;

import org.junit.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/** Test cases for {@link MetadataColumns}. */
public class MetadataColumnsTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testEmptyThrows() {
    thrown.expect(IllegalArgumentException.class);
    MetadataColumns mc = new MetadataColumns(" ");
  }

  @Test
  public void testSimpleCase() {
    String configDef = "xf_date:CREATE_DATE,name:AUTHOR";
    MetadataColumns mc = new MetadataColumns(configDef);
    assertTrue(mc.isMetadataColumnName("xf_date"));
    assertTrue(mc.isMetadataColumnName("name"));
    assertFalse(mc.isMetadataColumnName("CREATE_DATE"));
    assertFalse(mc.isMetadataColumnName("AUTHOR"));
    assertFalse(mc.isMetadataColumnName("xYz"));
    assertEquals("CREATE_DATE", mc.getMetadataName("xf_date"));
    assertEquals("AUTHOR", mc.getMetadataName("name"));
    assertNull(mc.getMetadataName("CREATE_DATE"));
    assertNull(mc.getMetadataName("AUTHOR"));
    assertNull(mc.getMetadataName("xYz"));
  }

  @Test
  public void testColonInMetadataKey() {
    String configDef = "xf_date:DATE:CREATE";
    MetadataColumns mc = new MetadataColumns(configDef);
    assertEquals("DATE:CREATE", mc.getMetadataName("xf_date"));
  }

  @Test
  public void testToString() {
    String configDef = "xf_date:DATE:CREATE,a:b";
    MetadataColumns mc = new MetadataColumns(configDef);
    assertEquals("MetadataColumns({a=b, xf_date=DATE:CREATE})", "" + mc);
  }
}
