package ru.profi;

import com.vertica.sdk.*;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDriver;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;


public class CHSourceFactory extends SourceFactory {
    ClickHouseConnection connection = null;

    @Override
    public void plan(ServerInterface srvInterface,
                     NodeSpecifyingPlanContext planCtxt) throws UdfException {
        if (!srvInterface.getParamReader().containsParameter("query")) {
            throw new UdfException(0, "Required parameter \"query\" not found");
        }

        findExecutionNodes(srvInterface.getParamReader(), planCtxt, srvInterface.getCurrentNodeName());
    }

    @Override
    public void getParameterType(ServerInterface srvInterface, SizedColumnTypes parameterTypes) {
        parameterTypes.addVarchar(65000, "query");
        parameterTypes.addInt( "timeout");
    }

    @Override
    public ArrayList<UDSource> prepareUDSources(ServerInterface srvInterface,
                                                NodeSpecifyingPlanContext planCtxt) throws UdfException {

        String query = srvInterface.getParamReader().getString("query");

        long timeout = srvInterface.getParamReader().containsParameter("timeout")
                ? srvInterface.getParamReader().getLong("timeout")
                : 30000;

        ArrayList<UDSource> sources = new ArrayList<>();

        try {
            sources.add(new CHSource(getClickHouseConnection(timeout), query));
        } catch (Exception e) {
            throw new UdfException(0, "Cannot initialize ClickHouse: " + e.getMessage());
        }

        return sources;

    }

    private void findExecutionNodes(ParamReader args,
                                    NodeSpecifyingPlanContext planCtxt, String defaultList) throws UdfException {
        String nodes;
        ArrayList<String> clusterNodes = new ArrayList<String>(planCtxt.getClusterNodes());
        ArrayList<String> executionNodes = new ArrayList<String>();

        // If we found the nodes arg,
        if (args.containsParameter("nodes")) {
            nodes = args.getString("nodes");
        } else if (defaultList != "" ) {
            nodes = defaultList;
        } else {
            // We have nothing to do here.
            return;
        }

        // Check for special magic values first
        if (nodes == "ALL NODES") {
            executionNodes = clusterNodes;
        } else if (nodes == "ANY NODE") {
            Collections.shuffle(clusterNodes);
            executionNodes.add(clusterNodes.get(0));
        } else if (nodes == "") {
            // Return the empty nodes list.
            // Vertica will deal with this case properly.
        } else {
            // Have to actually parse the string          
            // "nodes" is a comma-separated list of node names.
            String[] nodeNames = nodes.split(",");

            for (int i = 0; i < nodeNames.length; i++){
                if (clusterNodes.contains(nodeNames[i])) {
                    executionNodes.add(nodeNames[i]);
                } else {
                    String msg = String.format("Specified node '%s' but no node by that name is available.  Available nodes are \"%s\".",
                            nodeNames[i], clusterNodes.toString());
                    throw new UdfException(0, msg);
                }
            }
        }

        planCtxt.setTargetNodes(executionNodes);
    }


    private static ClickHouseConnection getClickHouseConnection(long timeout) throws Exception {
        ClickHouseDriver chDriver = new ClickHouseDriver();
        ClickHouseProperties props = new ClickHouseProperties();
        Properties properties = new Properties();

        try (InputStream inputStream = Job.class
                .getClassLoader().getResourceAsStream("clickhouse.properties")) {
            properties.load(inputStream);
        }

        props.setUser((String) properties.getOrDefault("user", "default"));
        props.setPassword((String) properties.getOrDefault("pass", "default"));
        props.setDatabase((String) properties.getOrDefault("db", "default"));
        props.setSocketTimeout((int) timeout);

        return chDriver.connect("jdbc:clickhouse://"
                + properties.getOrDefault("host", "localhost")
                + ":" + properties.getOrDefault("port", "8123"), props);
    }
}