package com.doer.testkit;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;

/** One SQL statement in autocommit; SQLException is rethrown unchecked. */
public final class Sql {

    private Sql() {
    }

    public static void update(DataSource dataSource, String sql) {
        try (Connection con = dataSource.getConnection();
                PreparedStatement pst = con.prepareStatement(sql)) {
            pst.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /** The first column of the first row, or null. */
    public static Long selectLong(DataSource dataSource, String sql) {
        try (Connection con = dataSource.getConnection();
                PreparedStatement pst = con.prepareStatement(sql);
                ResultSet rs = pst.executeQuery()) {
            if (rs.next()) {
                long x = rs.getLong(1);
                if (!rs.wasNull()) {
                    return x;
                }
            }
            return null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /** The first column of the first row, or null. */
    public static String selectString(DataSource dataSource, String sql) {
        try (Connection con = dataSource.getConnection();
                PreparedStatement pst = con.prepareStatement(sql);
                ResultSet rs = pst.executeQuery()) {
            return rs.next() ? rs.getString(1) : null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
