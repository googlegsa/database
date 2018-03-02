package com.google.enterprise.adaptor.database.dbext;

import com.google.enterprise.adaptor.database.ContentValue;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ContentColValue implements ContentValue {
  private static final Logger log =
      Logger.getLogger(ContentValue.class.getName());
  private static final Charset ENCODING = Charset.forName("UTF-8");

  @Override
  public void writeContentValue(ResultSet rs, int index, OutputStream out)
      throws SQLException, IOException {
    try {
      StringBuilder builder = new StringBuilder();
      builder.append(rs.getString(index));
      while (rs.next()) {
        builder.append(rs.getString(index));
      }
      out.write(builder.toString().getBytes(ENCODING));
      log.log(Level.FINEST, "ContentColvalue: {0}", builder.toString());
    } catch (SQLException e) {
      log.log(Level.FINEST, "Error in getting content", e);
      throw e;
    }
  }
}
