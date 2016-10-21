package com.bazaarvoice.emodb.sor.condition;

public interface ComparisonCondition extends Condition {

    Comparison getComparison();

    Object getValue();

    boolean overlaps(ComparisonCondition condition);

    boolean isSubsetOf(ComparisonCondition condition);
}
