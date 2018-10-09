package com.cst.jstorm.commons.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.StringUtils;

import java.io.IOException;

/**
 * @author Johnney.chiu
 * create on 2018/1/25 11:12
 * @Description
 */
public class HBaseConnectionUtil {

    Logger logger = LoggerFactory.getLogger(HBaseConnectionUtil.class);
    public static class HbaseConnectionProperties {

        @Value("${rootDir}")
        private String rootDir;

        @Value("${hbase.zookeeper.quorum}")
        private String zkServer;


        @Value("${zkPort}")
        private String port;

        @Value("${znode.parent}")
        private String znodeParent;

        @Value("${hbase.htable.threads.max}")
        private String hbaseTableHhreads;

        @Value("${hbase.pool.max.size}")
        private String hbasePoolMaxSize;

        @Value("${hbase.pool.core.size}")
        private String hbasePoolCoreSize;

        @Value("${hbase.pool.keep.alive}")
        private String hbaseKeepAlive;

        private String zkDataDir;
        private static final String HBASE_QUORUM = "hbase.zookeeper.quorum";
        private static final String HBASE_ROOTDIR = "hbase.rootdir";
        private static final String HBASE_ZNODE_DATA = "hbase.zookeeper.property.dataDir";
        private static final String HBASE_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";
        private static final String HBASE_ZNODE_PARENT = "zookeeper.znode.parent";
        private static final String HBASE_TABLE_THREADS_SIZE = "hbase.htable.threads.max";
        private static final String HBASE_CONNECTION_THREADS_MAX = "hbase.hconnection.threads.max";
        private static final String HBASE_CONNECTION_THREADS_CORE = "hbase.hconnection.threads.core";

        public HbaseConnectionProperties(String rootDir, String zkServer, String port, String zkDataDir,
                                         String znodeParent,String hbaseTableHhreads,String hbasePoolMaxSize,
                                         String hbasePoolCoreSize) {
            this.rootDir = rootDir;
            this.zkServer = zkServer;
            this.port = port;
            this.zkDataDir = zkDataDir;
            this.znodeParent = znodeParent;
            this.hbaseTableHhreads = hbaseTableHhreads;
            this.hbasePoolMaxSize = hbasePoolMaxSize;
            this.hbasePoolCoreSize = hbasePoolCoreSize;
        }

        public HbaseConnectionProperties() {
        }

        public HbaseConnectionProperties(String rootDir, String zkServer, String port,String znodeParent) {
            this.rootDir = rootDir;
            this.zkServer = zkServer;
            this.port = port;
            this.znodeParent = znodeParent;
        }

        public Connection buildHBaseConnection() throws IOException {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set(HBASE_QUORUM, zkServer);
            configuration.set(HBASE_ROOTDIR, rootDir);
            //configuration.set(HBASE_ZNODE_DATA, zkDataDir);
            configuration.set(HBASE_ZOOKEEPER_CLIENTPORT, port);
            configuration.set(HBASE_CONNECTION_THREADS_MAX,hbasePoolMaxSize);
            configuration.set(HBASE_CONNECTION_THREADS_CORE,hbasePoolCoreSize);
            if(!StringUtils.isEmpty(znodeParent))
                configuration.set(HBASE_ZNODE_PARENT, znodeParent);
            return ConnectionFactory.createConnection(configuration);
        }


        @Override
        public String toString() {
            return "HbaseConnectionProperties{" +
                    "rootDir='" + rootDir + '\'' +
                    ", zkServer='" + zkServer + '\'' +
                    ", port='" + port + '\'' +
                    ", znodeParent='" + znodeParent + '\'' +
                    ", hbaseTableHhreads='" + hbaseTableHhreads + '\'' +
                    ", zkDataDir='" + zkDataDir + '\'' +
                    '}';
        }
    }


    public static Connection createHBaseConnnection(String zkServer, String port, String rootDir,
                                                    String zkDataDir,String znodeParent,String hbaseTableHhreads) throws IOException {
        return new HbaseConnectionProperties(rootDir, zkServer, port, zkDataDir,znodeParent,hbaseTableHhreads,hbaseTableHhreads,hbaseTableHhreads).buildHBaseConnection();
    }

    public static Connection createHBaseConnnection(String zkServer, String port, String rootDir,String znodeParent) throws IOException {
        return new HbaseConnectionProperties(rootDir, zkServer, port,znodeParent).buildHBaseConnection();
    }

    public static Connection createHBaseConnnection(HbaseConnectionProperties hbaseConnectionProperties) throws IOException {
       return hbaseConnectionProperties.buildHBaseConnection();
    }


}
