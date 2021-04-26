import org.junit.jupiter.api.Test;
import static org.mockito.Mockito.*;

import ru.profi.CHLoader;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDriver;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;


public class CHLoaderTest {

    @Test
    public void readerTest() throws IOException, SQLException {

        ClickHouseConnection connectionMock = mock(ClickHouseConnection.class);
        PreparedStatement stmtMock = mock(PreparedStatement.class);
        ResultSet rsMock = mock(ResultSet.class);
        ResultSetMetaData rsMetaMock = mock(ResultSetMetaData.class);
        when(connectionMock.prepareStatement(anyString())).thenReturn(stmtMock);
        when(stmtMock.executeQuery()).thenReturn(rsMock);
        when(rsMock.getMetaData()).thenReturn(rsMetaMock);

        when(rsMock.next()).thenReturn(true);
        when(rsMock.getString(1)).thenReturn("12345");
        when(rsMetaMock.getColumnCount()).thenReturn(1);

        CHLoader reader = new CHLoader(connectionMock, "1 row sql");
        assertEquals(reader.read(new byte[10], 1, 4), 4);
        assertEquals(reader.read(new byte[10], 1, 5), 5);
        assertNotEquals(reader.read(new byte[10], 1, 5), 6);
        assertEquals(reader.read(new byte[10], 1, 0), 0);

        when(rsMock.next()).thenReturn(true).thenReturn(false);
        when(rsMock.getString(1)).thenReturn("abc").thenReturn(null);
        when(rsMetaMock.getColumnCount()).thenReturn(1);

        reader = new CHLoader(connectionMock, "2 rows 1 column sql");
        assertEquals(reader.read(new byte[10], 1, 7), 3);

    }
}

