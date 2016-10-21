package com.bazaarvoice.emodb.sor.condition;

public enum Comparison {
    GT("gt", false),
    GE("ge", true),
    LT("lt", false),
    LE("le", true);

    private final String _deltaFunction;
    private final boolean _isClosed;

    private Comparison(String deltaFunction, boolean isClosed) {
        _deltaFunction = deltaFunction;
        _isClosed = isClosed;
    }

    public String getDeltaFunction() {
        return _deltaFunction;
    }

    public boolean isClosed() {
        return _isClosed;
    }
}
