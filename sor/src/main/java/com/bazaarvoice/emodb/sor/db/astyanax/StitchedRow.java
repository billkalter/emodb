package com.bazaarvoice.emodb.sor.db.astyanax;


import com.datastax.driver.core.*;

import java.nio.ByteBuffer;

public class StitchedRow extends AbstractGettableData implements Row {

    private ByteBuffer _content;
    private Row _oldRow;
    private int _contentIndex;
    private int _blockIndex;
    private CodecRegistry _codecRegistry;

    public StitchedRow(ProtocolVersion protocolVersion, CodecRegistry codecRegistry, Row oldRow, ByteBuffer content, int contentIndex, int blockIndex) {
        super(protocolVersion);
        _codecRegistry = codecRegistry;
        _oldRow = oldRow;
        _content = content;
        _contentIndex = contentIndex;
        _blockIndex = blockIndex;
    }

    private int convertIndexFromOldRow(int i) {
        return i >= _blockIndex ?  i - 1 : i;
    }

    private int convertIndexToOldRow(int i) {
        return i >= _blockIndex ?  i + 1 : i;
    }

    @Override
    protected int getIndexOf(String name) {
        int i = _oldRow.getColumnDefinitions().getIndexOf(name);
        return convertIndexFromOldRow(i);
    }

    @Override
    protected DataType getType(int i) {
        return _oldRow.getColumnDefinitions().getType(convertIndexToOldRow(i));
    }

    @Override
    protected String getName(int i) {
        return _oldRow.getColumnDefinitions().getName(convertIndexToOldRow(i));
    }

    @Override
    protected ByteBuffer getValue(int i) {
        if (i == _contentIndex) {
            return _content;
        }
        return _oldRow.getBytesUnsafe(convertIndexToOldRow(i));
    }

    @Override
    protected CodecRegistry getCodecRegistry() {
        return _codecRegistry;
    }

    @Override
    public Token getToken(int i) {
        return _oldRow.getToken(convertIndexToOldRow(i));
    }

    @Override
    public Token getToken(String name) {
        return _oldRow.getToken(name);
    }

    @Override
    public Token getPartitionKeyToken() {
        return _oldRow.getPartitionKeyToken();
    }

    /**
     * This method cannot be overridden since constructing a ColumnDefinitions instance is private.  For our purposes
     * we don't need this anyway, so it throws UnsupportedOperationException
     */
    @Override
    public ColumnDefinitions getColumnDefinitions() {
        throw new UnsupportedOperationException("Cannot get column definitions for a stitched row");
    }
}