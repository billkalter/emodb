package com.bazaarvoice.emodb.web.scanner.control;

import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatus;

import java.time.Duration;
import java.util.Date;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Simple implementation for {@link ScanCompactionControl}.  It calculates the compaction control and expiration times
 * as a fixed delay from after the scan starts.
 */
public class FixedDelayScanCompactionControl implements ScanCompactionControl {

    private final Duration _delay;
    private final Duration _expiration;

    public FixedDelayScanCompactionControl(Duration delay, Duration expiration) {
        _delay = checkNotNull(delay, "delay");
        _expiration = checkNotNull(expiration, "expiration");
    }

    @Override
    public Date getCompactionControlTimeForScan(ScanStatus scanStatus) {
        return new Date(scanStatus.getStartTime().getTime() + _delay.toMillis());
    }

    @Override
    public Date getCompactionControlExpirationForScan(ScanStatus scanStatus) {
        return new Date(scanStatus.getStartTime().getTime() + _delay.plus(_expiration).toMillis());
    }
}
