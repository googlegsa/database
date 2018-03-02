package com.google.enterprise.adaptor.database;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;

public abstract interface ContentValue {
  public void writeContentValue(ResultSet rs, int index, OutputStream out)
      throws SQLException, IOException;
}
