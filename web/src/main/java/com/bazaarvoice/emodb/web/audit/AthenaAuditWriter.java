package com.bazaarvoice.emodb.web.audit;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.audit.AuditWriter;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.util.Size;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class AthenaAuditWriter implements AuditWriter, Managed {

    private final static Logger _log = LoggerFactory.getLogger(AthenaAuditWriter.class);

    private final static DateTimeFormatter LOG_FILE_DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneOffset.UTC);

    private final static String OPEN_FILE_SUFFIX = ".log.tmp";
    private final static String CLOSED_FILE_SUFFIX = ".log";
    private final static String COMPRESSED_FILE_SUFFIX = ".log.gz";

    private final static long DEFAULT_MAX_FILE_SIZE = Size.megabytes(10).toBytes();
    private final static Duration DEFAULT_MAX_BATCH_TIME = Duration.ofMinutes(2);

    private final AmazonS3 _s3;
    private final String _s3Bucket;
    private final String _s3AuditRoot;
    private final long _maxFileSize;
    private final long _maxBatchTimeMs;
    private final File _stagingDir;
    private final String _logFilePrefix;
    private final BlockingQueue<QueuedAudit> _auditQueue;
    private final Clock _clock;
    private final ObjectWriter _objectWriter;
    private final ConcurrentMap<Long, AuditOutput> _openAuditOutputs = Maps.newConcurrentMap();

    private ScheduledExecutorService _auditService;
    private ExecutorService _fileTransferService;
    private AuditOutput _mruAuditOutput;
    private boolean _fileTransfersEnabled = true;

    public AthenaAuditWriter(AmazonS3 s3, URI s3AuditRootUri, long maxFileSize, Duration maxBatchTime,
                             File stagingDir, String logFilePrefix, ObjectMapper objectMapper, Clock clock,
                             LifeCycleRegistry lifeCycleRegistry) {
        _s3 = requireNonNull(s3, "s3");
        requireNonNull(s3AuditRootUri, "s3AuditRoot");
        checkArgument("s3".equals(s3AuditRootUri.getScheme()), "Audit root must be in s3");
        _s3Bucket = s3AuditRootUri.getHost();
        checkArgument(!Strings.isNullOrEmpty(_s3Bucket), "S3 bucket is required");

        String s3AuditRoot = s3AuditRootUri.getPath();
        if (s3AuditRoot.startsWith("/")) {
            s3AuditRoot = s3AuditRoot.substring(1);
        }
        if (s3AuditRoot.endsWith("/")) {
            s3AuditRoot = s3AuditRoot.substring(0, s3AuditRoot.length()-1);
        }
        _s3AuditRoot = s3AuditRoot;

        checkArgument(stagingDir.exists(), "Staging directory must exist");

        _maxFileSize = maxFileSize > 0 ? maxFileSize : DEFAULT_MAX_FILE_SIZE;
        _maxBatchTimeMs = (maxBatchTime != null && maxBatchTime.compareTo(Duration.ZERO) > 0 ? maxBatchTime : DEFAULT_MAX_BATCH_TIME).toMillis();
        _stagingDir = requireNonNull(stagingDir, "stagingDir");
        _logFilePrefix = requireNonNull(logFilePrefix, "logFilePrefix");
        _clock = requireNonNull(clock, "clock");

        _auditQueue = new ArrayBlockingQueue<>(4096);

        _objectWriter = objectMapper.copy()
                .configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false)
                .writer();

        lifeCycleRegistry.manage(this);
    }

    @VisibleForTesting
    AthenaAuditWriter(AmazonS3 s3, URI s3AuditRootUri, long maxFileSize, Duration maxBatchTime,
                      File stagingDir, String logFilePrefix, ObjectMapper objectMapper, Clock clock,
                      LifeCycleRegistry lifeCycleRegistry, ScheduledExecutorService auditService,
                      ExecutorService fileTransferService) {
        this(s3, s3AuditRootUri, maxFileSize, maxBatchTime, stagingDir, logFilePrefix, objectMapper, clock, lifeCycleRegistry);
        _auditService = auditService;
        _fileTransferService = fileTransferService;
    }

    @Override
    public void start() {
        long now = _clock.millis();
        long msToNextBatch = _maxBatchTimeMs - (now % _maxBatchTimeMs);

        // Do a one-time closing of all orphaned log files
        for (final File logFile : _stagingDir.listFiles((dir, name) -> name.startsWith(_logFilePrefix) && name.endsWith(OPEN_FILE_SUFFIX))) {
            if (logFile.length() > 0) {
                try {
                    renameClosedLogFile(logFile);
                } catch (IOException e) {
                    _log.warn("Failed to close orphaned audit log file: {}", logFile, e);
                }
            } else {
                if (!logFile.delete()) {
                    _log.debug("Failed to delete empty orphaned log file: {}", logFile);
                }
            }
        }

        // Two threads for the audit service: once to drain queued audits and one to close audit logs files and submit
        // them for transfer.  Normally these are initially null and locally managed, but unit tests may provide
        // pre-configured instances.
        if (_auditService == null) {
            _auditService = Executors.newScheduledThreadPool(2, new ThreadFactoryBuilder().setNameFormat("audit-log-%d").build());
        }
        if (_fileTransferService == null) {
            _fileTransferService = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("audit-transfer-%d").build());
        }

        _auditService.scheduleWithFixedDelay(this::processQueuedAudits,
                0, 1, TimeUnit.SECONDS);

        _auditService.scheduleAtFixedRate(this::doLogFileMaintenance,
                msToNextBatch, _maxBatchTimeMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() throws Exception {
        _auditService.shutdown();
        _fileTransferService.shutdown();
        
        if (!_auditService.awaitTermination(30, TimeUnit.SECONDS)) {
            _log.warn("Audits still processing unexpectedly after shutdown");
        }
        // Close all complete log files.  Don't transfer them now since we're shutting down; the next time the service
        // starts they'll be transferred.
        closeCompleteLogFiles(true);
    }

    @Override
    public void persist(String table, String key, Audit audit, long auditTime) {
        try {
            _auditQueue.put(new QueuedAudit(table, key, audit, auditTime));
        } catch (InterruptedException e) {
            _log.warn("Interrupted attempting to write audit for {}/{}", table, key);
        }
    }

    public void setFileTransfersEnabled(boolean fileTransfersEnabled) {
        _fileTransfersEnabled = fileTransfersEnabled;
    }

    private void doLogFileMaintenance() {
        // Close all files that have either exceeded their maximum size or age but have not closed due to lack of
        // audit activity.
        closeCompleteLogFiles(false);
        prepareClosedLogFilesForTransfer();
        transferLogFilesToS3();
    }

    private void closeCompleteLogFiles(boolean forceClose) {
        for (AuditOutput auditOutput : ImmutableList.copyOf(_openAuditOutputs.values())) {
            if (forceClose || auditOutput.shouldClose()) {
                auditOutput.close();
            }
        }
    }

    private void prepareClosedLogFilesForTransfer() {
        for (final File logFile : _stagingDir.listFiles((dir, name) -> name.startsWith(_logFilePrefix) && name.endsWith(CLOSED_FILE_SUFFIX))) {
            boolean moved;
            String fileName = logFile.getName().substring(0, logFile.getName().length() - CLOSED_FILE_SUFFIX.length()) + COMPRESSED_FILE_SUFFIX;
            try (FileInputStream fileIn = new FileInputStream(logFile);
                 FileOutputStream fileOut = new FileOutputStream(new File(logFile.getParentFile(), fileName));
                 GzipCompressorOutputStream gzipOut = new GzipCompressorOutputStream(fileOut)) {

                ByteStreams.copy(fileIn, gzipOut);
                moved = true;
            } catch (IOException e) {
                _log.warn("Failed to compress audit log file: {}", logFile, e);
                moved = false;
            }

            if (moved) {
                if (!logFile.delete()) {
                    _log.warn("Failed to delete audit log file: {}", logFile);
                }
            }
        }
    }

    private void transferLogFilesToS3() {
        if (_fileTransfersEnabled) {
            // Find all closed log files in the staging directory and move them to S3
            for (final File logFile : _stagingDir.listFiles((dir, name) -> name.startsWith(_logFilePrefix) && name.endsWith(COMPRESSED_FILE_SUFFIX))) {
                String auditDate = logFile.getName().substring(_logFilePrefix.length() + 1, _logFilePrefix.length() + 9);
                String dest = String.format("%s/date=%s/%s", _s3AuditRoot, auditDate, logFile.getName());

                _fileTransferService.submit(() -> {
                    // Since file transfers are done in a single this thread shouldn't have any concurrency issues,
                    // but verify the same file wasn't submitted previously and is already transferred.
                    if (logFile.exists()) {
                        try {
                            PutObjectResult result = _s3.putObject(_s3Bucket, dest, logFile);
                            _log.debug("Audit log copied: {}, ETag={}", logFile, result.getETag());

                            if (!logFile.delete()) {
                                _log.warn("Failed to delete file after copying to s3: {}", logFile);
                            }
                        } catch (Exception e) {
                            // Log the error, try again on the next iteration
                            _log.warn("Failed to copy log file {}", logFile, e);
                        }
                    }
                });
            }
        }

    }

    private void processQueuedAudits() {
        QueuedAudit audit;
        try {
            while (!_auditService.isShutdown() && (audit = _auditQueue.poll()) != null) {
                boolean written = false;
                while (!written) {
                    AuditOutput auditOutput = getAuditOutputForTime(audit.time);
                    written = auditOutput.writeAudit(audit);
                }
            }
        } catch (Exception e) {
            _log.error("Processing of queued audits failed", e);
        }
    }

    private AuditOutput getAuditOutputForTime(long time) {
        long batchTime = time - (time % _maxBatchTimeMs);
        AuditOutput mruAuditOutput = _mruAuditOutput;
        if (mruAuditOutput != null && batchTime == mruAuditOutput.getBatchTime() && !mruAuditOutput.isClosed()) {
            return mruAuditOutput;
        }

        return _mruAuditOutput = _openAuditOutputs.computeIfAbsent(batchTime, this::createNewAuditLogOut);
    }

    private AuditOutput createNewAuditLogOut(long batchTime) {
        long now = _clock.millis();
        long nextBatchCycleCloseTime = now - (now % _maxBatchTimeMs) + _maxBatchTimeMs;

        return new AuditOutput(LOG_FILE_DATE_FORMATTER.format(Instant.ofEpochMilli(batchTime)), batchTime, nextBatchCycleCloseTime);
    }

    private void renameClosedLogFile(File logFile) throws IOException {
        // Move the file to a new file without the ".tmp" suffix
        String closedFileName = logFile.getName().substring(0, logFile.getName().length() - OPEN_FILE_SUFFIX.length()) + CLOSED_FILE_SUFFIX;
        Files.move(logFile.toPath(), new File(logFile.getParentFile(), closedFileName).toPath());
    }

    private static class QueuedAudit {
        final String table;
        final String key;
        final Audit audit;
        final long time;

        QueuedAudit(String table, String key, Audit audit, long time) {
            this.table = table;
            this.key = key;
            this.audit = audit;
            this.time = time;
        }
    }

    private class AuditOutput {
        private final File _auditLogFile;
        private final long _batchTime;
        private final long _closeTime;
        private final ReentrantLock _lock = new ReentrantLock();
        private volatile boolean _closed;
        private volatile int _auditsWritten = 0;
        private volatile CountingOutputStream _auditLogOut;

        AuditOutput(String datePrefix, long batchTime, long closeTime) {
            String fileName = String.format("%s.%s.%s%s", _logFilePrefix, datePrefix, UUID.randomUUID(), OPEN_FILE_SUFFIX);
            _auditLogFile = new File(_stagingDir, fileName);
            _batchTime = batchTime;
            _closeTime = closeTime;
        }

        void createAuditLogOut() throws IOException {
            FileOutputStream fileOut = new FileOutputStream(_auditLogFile);
            _auditLogOut = new CountingOutputStream(fileOut);
        }

        boolean writeAudit(QueuedAudit audit) {
            Map<String, Object> auditMap = Maps.newLinkedHashMap();
            // This is an intentional break from convention to use "tablename" instead of "table".  This is because
            // "table" is a reserved word in Presto and complicates queries for that column.
            auditMap.put("tablename", audit.table);
            auditMap.put("key", audit.key);
            auditMap.put("time", audit.time);
            // Even though the content of the audit is valid JSON the potential key set is unbounded.  This makes
            // it difficult to define a schema for Presto.  So create values for the conventional keys and store
            // the rest in an opaque blob.
            Map<String, Object> custom = new HashMap<>(audit.audit.getAll());
            if (custom.remove(Audit.COMMENT) != null) {
                auditMap.put("comment", audit.audit.getComment());
            }
            if (custom.remove(Audit.HOST) != null) {
                auditMap.put("host", audit.audit.getHost());
            }
            if (custom.remove(Audit.PROGRAM) != null) {
                auditMap.put("program", audit.audit.getProgram());
            }
            if (custom.remove(Audit.SHA1) != null) {
                auditMap.put("sha1", audit.audit.getCustom(Audit.SHA1));
            }
            if (custom.remove(Audit.TAGS) != null) {
                auditMap.put("tags", audit.audit.getTags());
            }
            if (!custom.isEmpty()) {
                try {
                    auditMap.put("custom", _objectWriter.writeValueAsString(custom));
                } catch (JsonProcessingException e) {
                    _log.info("Failed to write custom audit information", e);
                }
            }

            _lock.lock();
            try {
                if (shouldClose()) {
                    close();
                }

                if (isClosed()) {
                    return false;
                }

                if (_auditLogOut == null) {
                    createAuditLogOut();
                }

                _objectWriter.writeValue(_auditLogOut, auditMap);
                _auditLogOut.write('\n');
                //noinspection NonAtomicOperationOnVolatileField
                _auditsWritten += 1;
            } catch (IOException e) {
                _log.warn("Failed to write audit to logs", e);
            } finally {
                _lock.unlock();
            }

            return true;
        }

        boolean isClosed() {
            return _closed;
        }
        
        void close() {
            _lock.lock();
            try {
                if (!_closed) {
                    _closed = true;

                    if (_auditLogOut != null) {
                        _auditLogOut.close();
                    }

                    if (_auditsWritten != 0) {
                        renameClosedLogFile(_auditLogFile);
                    }
                }
            } catch (IOException e) {
                _log.warn("Failed to close log file", e);
            } finally {
                _openAuditOutputs.remove(_batchTime, this);
                _lock.unlock();
            }
        }

        long getBatchTime() {
            return _batchTime;
        }
        
        boolean isBatchTimedOut() {
            return _clock.millis() >= _closeTime;
        }

        boolean isOversized() {
            return _auditLogOut != null && _auditLogOut.getCount() > _maxFileSize;
        }

        boolean shouldClose() {
            return isBatchTimedOut() || isOversized();
        }
    }
}
