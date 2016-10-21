package com.bazaarvoice.emodb.sor.condition.eval;

import com.bazaarvoice.emodb.sor.condition.AndCondition;
import com.bazaarvoice.emodb.sor.condition.ComparisonCondition;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.ConditionVisitor;
import com.bazaarvoice.emodb.sor.condition.ConstantCondition;
import com.bazaarvoice.emodb.sor.condition.ContainsCondition;
import com.bazaarvoice.emodb.sor.condition.EqualCondition;
import com.bazaarvoice.emodb.sor.condition.InCondition;
import com.bazaarvoice.emodb.sor.condition.IntrinsicCondition;
import com.bazaarvoice.emodb.sor.condition.IsCondition;
import com.bazaarvoice.emodb.sor.condition.LikeCondition;
import com.bazaarvoice.emodb.sor.condition.MapCondition;
import com.bazaarvoice.emodb.sor.condition.NotCondition;
import com.bazaarvoice.emodb.sor.condition.OrCondition;
import com.bazaarvoice.emodb.sor.condition.State;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Condition evaluator to determine if two conditions are distinct.  This is true if for all input values at most one
 * of the conditions returns true.
 */
public class DistinctEvaluator implements ConditionVisitor<Condition, Boolean> {

    public static boolean areDistinct(Condition left, Condition right) {
        return new DistinctEvaluator().checkAreDistinct(checkNotNull(left, "left"), checkNotNull(right, "right"));
    }

    private boolean checkAreDistinct(Condition left, Condition right) {
        return left.visit(this, right);
    }

    @Override
    public Boolean visit(LikeCondition left, Condition right) {
        LeftResolvedVisitor<LikeCondition> visitor = new LeftResolvedVisitor<LikeCondition>(left) {
            @Override
            public Boolean visit(IsCondition right, Void context) {
                return right.getState() != State.DEFINED && right.getState() != State.STRING;
            }

            @Override
            public Boolean visit(EqualCondition right, Void context) {
                return !(right.getValue() instanceof String && _left.matches((String) right.getValue()));
            }

            @Override
            public Boolean visit(InCondition right, Void context) {
                for (Object value : right.getValues()) {
                    if (value instanceof String && _left.matches((String) value)) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public Boolean visit(ComparisonCondition right, Void context) {
                if (!(right.getValue() instanceof String)) {
                    return true;
                }

                // If the condition has a prefix then it can be used for comparison.
                String prefix = _left.getPrefix();
                if (prefix == null) {
                    // The condition is something like "*abc", so it cannot be distinct from a comparison.
                    return false;
                }

                String comparisonValue = (String) right.getValue();
                return prefix.length() >= comparisonValue.length() && !ConditionEvaluator.eval(right, prefix, null);
            }

            @Override
            public Boolean visit(LikeCondition right, Void context) {
                return !_left.overlaps(right);
            }
        };

        return right.visit(visitor, null);
    }

    @Override
    public Boolean visit(ConstantCondition left, @Nullable Condition right) {
        return defaultDistinctCheck(left, right);
    }

    @Override
    public Boolean visit(EqualCondition left, Condition right) {
        return defaultDistinctCheck(left, right);
    }

    @Override
    public Boolean visit(InCondition left, Condition right) {
        return defaultDistinctCheck(left, right);
    }

    @Override
    public Boolean visit(IntrinsicCondition left, Condition right) {
        return defaultDistinctCheck(left, right);
    }

    @Override
    public Boolean visit(IsCondition left, Condition right) {
        return defaultDistinctCheck(left, right);
    }

    @Override
    public Boolean visit(ComparisonCondition left, Condition right) {
        return defaultDistinctCheck(left, right);
    }

    @Override
    public Boolean visit(ContainsCondition left, Condition right) {
        return defaultDistinctCheck(left, right);
    }

    @Override
    public Boolean visit(NotCondition left, Condition right) {
        return defaultDistinctCheck(left, right);
    }

    @Override
    public Boolean visit(AndCondition left, Condition right) {
        return defaultDistinctCheck(left, right);
    }

    @Override
    public Boolean visit(OrCondition left, Condition right) {
        return defaultDistinctCheck(left, right);
    }

    @Override
    public Boolean visit(MapCondition left, Condition right) {
        return defaultDistinctCheck(left, right);
    }

    private boolean defaultDistinctCheck(Condition left, Condition right) {
        return right.visit(new LeftResolvedVisitor<>(left), null);    
    }
    
    /**
     * Visitor used to provide typed evaluation of both the left and right conditions.  Default behavior for all
     * combinations of conditions is:
     *
     * !left.subset(right) && !right.subset(left)
     *
     * For the cases where this is insufficient the subclass should override.
     */
    private class LeftResolvedVisitor<T extends Condition> implements ConditionVisitor<Void, Boolean> {

        T _left;

        private LeftResolvedVisitor(T left) {
            _left = left;
        }

        /**
         * Override to use the Like condition on the left since there is custom behavior for this in
         * {@link DistinctEvaluator#visit(LikeCondition, Condition)}.  Also since this call is overridden in that
         * handler there is no need to perform a check for <code>_left instanceof LikeCondition</code>, which otherwise
         * would create an infinite loop.
         */
        @Override
        public Boolean visit(LikeCondition right, Void context) {
            return checkAreDistinct(right, _left);
        }

        @Override
        public Boolean visit(ConstantCondition right, Void context) {
            return haveDistinctSubets(right);
        }

        @Override
        public Boolean visit(EqualCondition right, Void context) {
            return haveDistinctSubets(right);
        }

        @Override
        public Boolean visit(InCondition right, Void context) {
            return haveDistinctSubets(right);
        }

        @Override
        public Boolean visit(IntrinsicCondition right, Void context) {
            return haveDistinctSubets(right);
        }

        @Override
        public Boolean visit(IsCondition right, Void context) {
            return haveDistinctSubets(right);
        }

        @Override
        public Boolean visit(ComparisonCondition right, Void context) {
            return haveDistinctSubets(right);
        }

        @Override
        public Boolean visit(ContainsCondition right, Void context) {
            return haveDistinctSubets(right);
        }

        @Override
        public Boolean visit(MapCondition right, Void context) {
            return haveDistinctSubets(right);
        }

        @Override
        public Boolean visit(AndCondition right, Void context) {
            return haveDistinctSubets(right);
        }

        @Override
        public Boolean visit(OrCondition right, Void context) {
            return haveDistinctSubets(right);
        }

        @Override
        public Boolean visit(NotCondition right, Void context) {
            return haveDistinctSubets(right);
        }

        private boolean haveDistinctSubets(Condition right) {
            return !SubsetEvaluator.isSubset(_left, right) && !SubsetEvaluator.isSubset(right, _left);
        }
    }
}
