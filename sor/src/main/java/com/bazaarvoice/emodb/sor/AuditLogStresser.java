package com.bazaarvoice.emodb.sor;

import com.bazaarvoice.emodb.common.cassandra.AstyanaxCluster;
import com.bazaarvoice.emodb.common.cassandra.CassandraConfiguration;
import com.bazaarvoice.emodb.common.cassandra.CassandraPartitioner;
import com.bazaarvoice.emodb.sor.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxStorage;
import com.bazaarvoice.emodb.table.db.astyanax.TableUuidFormat;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.TimeUUIDSerializer;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * This test does not write "legitimate" audits.  The UUID for the table it populates is nonsense as are the audits.
 * This test only simulates the <em>rate</em> of writes to the audit table with representative audit rows.
 */
public class AuditLogStresser {

    private final String _cluster;
    private final String _dataCenter;
    private final String _keyspaceName;
    private final String _table;
    private final long _uuid;
    private final int _shards;
    private final RateLimiter _rateLimiter;
    private final int _testSeconds;
    private final String _cassandraSeeds;
    private final int _numThreads;

    private AstyanaxCluster _astyanaxCluster;
    private Keyspace _keyspace;
    private AstyanaxStorage _storage;
    private ColumnFamily<ByteBuffer, UUID> _auditColumnFamily;
    private Stopwatch _stopwatch;
    private ByteBuffer _protoAudit;

    private final MetricRegistry _metricRegistry = new MetricRegistry();
    private final Meter _auditWrites = _metricRegistry.meter("stresser.auditWrites");
    private final Meter _auditWriteFailures = _metricRegistry.meter("stresser.auditWriteFailures");

    public static void main(String args[]) throws Exception {

        ArgumentParser argParser = ArgumentParsers.newArgumentParser("auto-log-stress", true);

        argParser.addArgument("cluster")
                .type(String.class)
                .required(true)
                .help("Cassandra cluster, such as \"ci_sor_ugc_default\"");

        argParser.addArgument("dataCenter")
                .type(String.class)
                .required(true)
                .help("Cassandra data center, such as \"us-east-qa\"");

        argParser.addArgument("keyspace")
                .type(String.class)
                .required(true)
                .help("Cassandra keyspace for audit table, such as \"ugc_global\"");

        argParser.addArgument("table")
                .type(String.class)
                .required(true)
                .help("Cassandra audit table, such as \"ugc_audit\"");

        argParser.addArgument("-u", "--uuid")
                .dest("uuid")
                .type(String.class)
                .setDefault("8888888888888888")
                .help("UUID of table to use for stress testing");

        argParser.addArgument("-s", "--shards")
                .dest("shards")
                .type(Integer.class)
                .setDefault(8)
                .help("Number of shards in stress test table");

        argParser.addArgument("-r", "--rate")
                .dest("rate")
                .type(Double.class)
                .setDefault(400d)
                .help("Target audit writes per second");

        argParser.addArgument("-t", "--time")
                .dest("time")
                .type(Integer.class)
                .setDefault(60)
                .help("Time after which to end the test (seconds)");

        argParser.addArgument("-c", "--seeds")
                .dest("seeds")
                .type(String.class)
                .required(true)
                .help("Cassandra seeds");

        argParser.addArgument("--threads")
                .dest("threads")
                .type(Integer.class)
                .setDefault(8)
                .help("Number of threads to use for concurrent writes");

        Namespace ns = argParser.parseArgs(args);

        AuditLogStresser auditLogStresser = new AuditLogStresser(
                ns.getString("cluster"), ns.getString("dataCenter"),
                ns.getString("keyspace"), ns.getString("table"),
                TableUuidFormat.decode(ns.getString("uuid")), ns.getInt("shards"),
                ns.getDouble("rate"), ns.getInt("time"), ns.getString("seeds"), ns.getInt("threads"));

        auditLogStresser.runStressTest();
    }

    public AuditLogStresser(String cluster, String dataCenter, String keyspaceName, String table, long uuid, int shards,
                            double writesPerSecond, int testSeconds, String cassandraSeeds, int numThreads) {
        _cluster = cluster;
        _dataCenter = dataCenter;
        _keyspaceName = keyspaceName;
        _table = table;
        _uuid = uuid;
        _shards = shards;
        _rateLimiter = RateLimiter.create(writesPerSecond);
        _testSeconds = testSeconds;
        _cassandraSeeds = cassandraSeeds;
        _numThreads = numThreads;
    }

    public void runStressTest() throws Exception {
        try {
            startCassandra();

            _auditColumnFamily = new ColumnFamily<>(_table, ByteBufferSerializer.get(), TimeUUIDSerializer.get());

            Map<String, Object> protoAuditMap = new LinkedHashMap<>();
            protoAuditMap.put("program", "audit-log-stresser");
            protoAuditMap.put("host", "host-id-0123457890abcdef");
            protoAuditMap.put("comment", "Test audit written by audit-log-stresser");
            protoAuditMap.put("~sha1", "fa52e77d871d6bffd3fcc33041db1c87006df3bf");
            protoAuditMap.put("~tags", Lists.newArrayList("alstress"));

            String protoAuditString = "A1:" + new ObjectMapper().writeValueAsString(protoAuditMap);
            _protoAudit = ByteBuffer.wrap(protoAuditString.getBytes(Charsets.UTF_8));

            List<Thread> threads = new ArrayList<>(_numThreads);
            for (int i=0; i < _numThreads; i++) {
                threads.add(new Thread(this::writeAudits));
            }

            _stopwatch = Stopwatch.createStarted();
            threads.forEach(Thread::start);

            while (threads.stream().anyMatch(Thread::isAlive)) {
                Thread.sleep(1000);
                System.out.println(String.format("writes = %d, failures = %s", _auditWrites.getCount(), _auditWriteFailures.getCount()));
            }
        } finally {
            closeCassandra();
        }
    }

    private void startCassandra() throws Exception {
        CassandraConfiguration cassandraConfig = new CassandraConfiguration();
        cassandraConfig.setCluster(_cluster);
        cassandraConfig.setDataCenter(_dataCenter);
        cassandraConfig.setSeeds(_cassandraSeeds);
        cassandraConfig.setPartitioner(CassandraPartitioner.BOP);
        cassandraConfig.setMaxConnectionsPerHost(Optional.of(60));

        _astyanaxCluster = cassandraConfig.astyanax()
                .metricRegistry(_metricRegistry)
                .cluster();

        _astyanaxCluster.start();
        _keyspace = _astyanaxCluster.connect(_keyspaceName);

        // Only need a storage instance to generate keys
        _storage = new AstyanaxStorage(_uuid, _shards, true, "test",
                () -> { throw new RuntimeException("Placement unavailable"); });
    }

    private void closeCassandra() {
        if (_astyanaxCluster != null) {
            try {
                _astyanaxCluster.stop();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    private void writeAudits() {
        String documentPrefix = UUID.randomUUID().toString();
        int index = 0;

        while (_stopwatch.elapsed(TimeUnit.SECONDS) <= _testSeconds) {
            String docId = String.format("%s-%d", documentPrefix, index++);
            ByteBuffer docKey = _storage.getRowKey(docId);

            _rateLimiter.acquire();

            try {
                _keyspace.prepareColumnMutation(_auditColumnFamily, docKey, TimeUUIDs.newUUID())
                        .setConsistencyLevel(ConsistencyLevel.CL_LOCAL_ONE)
                        .putValue(_protoAudit.duplicate(), null)
                        .execute();

                _auditWrites.mark();
            } catch (Exception e) {
                _auditWriteFailures.mark();
            }
        }
    }
}
