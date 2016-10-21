package com.bazaarvoice.emodb.sor.condition;

public interface LikeCondition extends Condition {

    String getCondition();

    boolean matches(String input);

    boolean overlaps(LikeCondition condition);

    boolean isSubsetOf(LikeCondition condition);

    String getPrefix();
}
