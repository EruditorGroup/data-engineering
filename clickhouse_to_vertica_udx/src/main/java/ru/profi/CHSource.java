package ru.profi;

import com.vertica.sdk.DataBuffer;
import com.vertica.sdk.ServerInterface;
import com.vertica.sdk.State.StreamState;
import com.vertica.sdk.UDSource;
import com.vertica.sdk.UdfException;

import ru.yandex.clickhouse.ClickHouseConnection;

import java.io.IOException;
import java.sql.SQLException;


public class CHSource extends UDSource {
    private final String query;
    protected final ClickHouseConnection connection;
    protected AbstractLoader reader;


    public CHSource(ClickHouseConnection connection, String query) {
        super();
        this.connection = connection;
        this.query = query;
    }

    @Override
    public void setup(ServerInterface srvInterface ) throws UdfException{
        reader = new CHLoader(connection, query);

        try {
            reader.load();
        } catch (Exception e) {
            throw new UdfException(0, "Error while ClickHouse loader initialization: " + e.getMessage());
        }
    }

    @Override
    public void destroy(ServerInterface srvInterface ) throws UdfException {
        if (reader != null)
            try {
                reader.close();
            } catch ( SQLException e) {
                String msg = e.getMessage();
                throw new UdfException(0, msg);
            }
    }

    @Override
    public StreamState process(ServerInterface srvInterface, DataBuffer output) throws UdfException {
        long offset;
        try {
            offset = reader.read(output.buf, output.offset,output.buf.length - output.offset);
        } catch (IOException e) {
            throw new UdfException(1, e);
        }

        srvInterface.log("%d bytes loaded, buffer length: %d, buffer offset: %d", offset, output.buf.length, output.offset);

        if (offset == -1 && output.offset == 0) {
            return StreamState.DONE;
        }

        output.offset += offset;

        if (offset == -1 || offset < output.buf.length) {
            return StreamState.DONE;
        } else {
            return StreamState.OUTPUT_NEEDED;
        }
    }
}