package com.bazaarvoice.emodb.web.scanner.control;

import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatus;

import java.util.Date;

/**
 * Helper class for coordinating compaction control while a scan is running.
 */
public interface ScanCompactionControl {
    
    Date getCompactionControlTimeForScan(ScanStatus scanStatus);

    Date getCompactionControlExpirationForScan(ScanStatus scanStatus);
}
