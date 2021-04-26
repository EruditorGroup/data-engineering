package ru.profi;

import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import ru.yandex.clickhouse.ClickHouseConnection;

public class CHLoader extends AbstractLoader {
    final String sql;
    final ClickHouseConnection connection;
    PreparedStatement stmt;
    ResultSet rs;
    ResultSetMetaData metaData;
    Boolean[] isStringFlags;
    int columnCount;
    final String lineSeparator;
    final String lineSeparatorReplacement;
    final String delimiter = "|";

    public CHLoader(ClickHouseConnection connection, String sql) {
        this.connection = connection;
        this.sql = sql;
        this.lineSeparator = System.getProperty("line.separator");
        this.lineSeparatorReplacement = this.lineSeparator
                .replace("\n", "\\" + "\\" + "\n")
                .replace("\r", "\\" + "\\" + "\r")
        ;
    }

    @Override
    public void load() throws SQLException {
        stmt = connection.prepareStatement(sql);
        rs = stmt.executeQuery();
        metaData = rs.getMetaData();
        columnCount = metaData.getColumnCount();

        isStringFlags = new Boolean[columnCount];
        for (int i = 1; i <= columnCount; i++) {
            isStringFlags[i - 1] = metaData.getColumnTypeName(i).equals("String");
        }
    }

    @Override
    public byte[] nextRow() {
        if (rs == null) {
            return null;
        }

        try {
            if (!rs.next()) {
                return null;
            }

            String[] values = new String[columnCount];

            for (int i = 0; i < columnCount; i++) {
                String stringVal = rs.getString(i + 1);
                values[i] = isStringFlags[i] && stringVal != null
                        ? stringVal
                        .replaceAll("\\|", "\\\\|")
                        .replaceAll(this.lineSeparator, this.lineSeparatorReplacement)
                        : stringVal;
            }

            return (String.join(delimiter, values) + this.lineSeparator).getBytes(StandardCharsets.UTF_8);
        } catch (SQLException e) {
            return null;
        }
    }

    public void close() throws SQLException {
        stmt.close();
        rs.close();
        connection.close();
    }
}