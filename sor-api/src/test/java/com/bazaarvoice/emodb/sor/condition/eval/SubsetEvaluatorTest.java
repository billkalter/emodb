package com.bazaarvoice.emodb.sor.condition.eval;

import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.condition.MapCondition;
import com.bazaarvoice.emodb.sor.condition.impl.NotConditionImpl;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class SubsetEvaluatorTest {

    @DataProvider(name = "conditions")
    public Object[][] provideConditions() {
        return new Object[][] {
                // Constant conditions
                new Object[] {Conditions.alwaysTrue(), Conditions.alwaysTrue(), true},
                new Object[] {Conditions.alwaysFalse(), Conditions.alwaysFalse(), true},
                new Object[] {Conditions.alwaysTrue(), Conditions.alwaysFalse(), false},
                new Object[] {Conditions.alwaysFalse(), Conditions.alwaysTrue(), true},

                // Equal conditions
                new Object[] {Conditions.equal("test"), Conditions.alwaysTrue(), true},
                new Object[] {Conditions.equal("test"), Conditions.isString(), true},
                new Object[] {Conditions.equal("test"), Conditions.isDefined(), true},
                new Object[] {Conditions.equal("test"), Conditions.equal("test"), true},
                new Object[] {Conditions.equal("test"), Conditions.in("test", "other"), true},
                new Object[] {Conditions.equal("test"), Conditions.le("toast"), true},
                new Object[] {Conditions.equal("test"), Conditions.like("t*t"), true},
                new Object[] {Conditions.equal("t\\t"), Conditions.like("t\\\\t"), true},
                new Object[] {Conditions.equal("test"), Conditions.not(Conditions.like("z*")), true},
                new Object[] {Conditions.equal(ImmutableList.of("fast", "slow")), Conditions.contains("fast"), true},
                new Object[] {Conditions.equal(ImmutableList.of("fast", "slow")), Conditions.containsOnly("fast", "slow"), true},
                new Object[] {Conditions.equal(ImmutableMap.of("k", "v")), Conditions.mapBuilder().contains("k", "v").build(), true},
                new Object[] {Conditions.equal("test"), Conditions.alwaysFalse(), false},
                new Object[] {Conditions.equal("test"), Conditions.not(Conditions.equal("test")), false},
                new Object[] {Conditions.equal("test"), Conditions.isNumber(), false},
                new Object[] {Conditions.equal("test"), Conditions.isUndefined(), false},
                new Object[] {Conditions.equal("test"), Conditions.equal("rake"), false},
                new Object[] {Conditions.equal("test"), Conditions.in("nope", "nada"), false},
                new Object[] {Conditions.equal("test"), Conditions.gt("zebra"), false},
                new Object[] {Conditions.equal("test"), Conditions.like("z*"), false},
                new Object[] {Conditions.equal("test"), Conditions.not(Conditions.like("t*")), false},
                new Object[] {Conditions.equal("t\\t"), Conditions.not(Conditions.like("t\\\\t")), false},
                new Object[] {Conditions.equal("test"), Conditions.mapBuilder().contains("test", "test").build(), false},
                new Object[] {Conditions.equal("test"), Conditions.contains("test"), false},
                new Object[] {Conditions.equal(ImmutableList.of("test")), Conditions.contains("nope"), false},
                new Object[] {Conditions.equal(ImmutableList.of("fast", "slow")), Conditions.containsOnly("fast"), false},
                new Object[] {Conditions.equal(ImmutableMap.of("k", "v")), Conditions.mapBuilder().contains("k", "x").build(), false},
                new Object[] {Conditions.equal("test"), Conditions.intrinsic(Intrinsic.TABLE, "test"), false},

                // Is conditions
                new Object[] {Conditions.isDefined(), Conditions.alwaysTrue(), true},
                new Object[] {Conditions.isString(), Conditions.isString(), true},
                new Object[] {Conditions.isString(), Conditions.isDefined(), true},
                new Object[] {Conditions.isUndefined(), Conditions.isUndefined(), true},
                new Object[] {Conditions.isUndefined(), Conditions.not(Conditions.isDefined()), true},
                new Object[] {Conditions.isString(), Conditions.not(Conditions.isNull()), true},
                new Object[] {Conditions.isBoolean(), Conditions.not(Conditions.isString()), true},
                new Object[] {Conditions.isString(), Conditions.alwaysFalse(), false},
                new Object[] {Conditions.isDefined(), Conditions.isString(), false},
                new Object[] {Conditions.isUndefined(), Conditions.isString(), false},
                new Object[] {Conditions.isString(), Conditions.isBoolean(), false},
                new Object[] {Conditions.isString(), Conditions.equal("test"), false},
                new Object[] {Conditions.isString(), Conditions.in("a", "b"), false},
                new Object[] {Conditions.isList(), Conditions.contains("a"), false},
                new Object[] {Conditions.isMap(), Conditions.mapBuilder().contains("key", "value").build(), false},
                new Object[] {Conditions.isString(), Conditions.gt("a"), false},
                new Object[] {Conditions.isString(), Conditions.intrinsic(Intrinsic.TABLE, "test"), false},
                new Object[] {Conditions.isString(), Conditions.not(Conditions.isString()), false},
                new Object[] {Conditions.isString(), Conditions.not(Conditions.like("te*")), false},

                // In conditions
                new Object[] {Conditions.in("up", "down"), Conditions.alwaysTrue(), true},
                new Object[] {Conditions.in("up", "down"), Conditions.isDefined(), true},
                new Object[] {Conditions.in("up", "down"), Conditions.isString(), true},
                new Object[] {Conditions.in("up"), Conditions.equal("up"), true},
                new Object[] {Conditions.in("up", "down"), Conditions.gt("c"), true},
                new Object[] {Conditions.in("up", "down"), Conditions.le("up"), true},
                new Object[] {Conditions.in(ImmutableList.of("up"), ImmutableList.of("down")), Conditions.containsAny("up", "down"), true},
                new Object[] {Conditions.in(ImmutableList.of("up", "down", "left"), ImmutableList.of("up", "down", "right")),
                        Conditions.containsAll("up", "down"), true},
                new Object[] {Conditions.in("frog", "flag"), Conditions.like("f*g"), true},
                new Object[] {Conditions.in("frog", "flag"), Conditions.not(Conditions.like("a*")), true},
                new Object[] {Conditions.in(ImmutableMap.of("k1", "v1"), ImmutableMap.of("k1", "v2")),
                        Conditions.mapBuilder().matches("k1", Conditions.in("v1", "v2")).build(), true},
                new Object[] {Conditions.in("up", "down"), Conditions.not(Conditions.equal("left")), true},
                new Object[] {Conditions.in("up", "down"), Conditions.alwaysFalse(), false},
                new Object[] {Conditions.in("up", "down"), Conditions.isUndefined(), false},
                new Object[] {Conditions.in("up", 123), Conditions.isString(), false},
                new Object[] {Conditions.in("up"), Conditions.equal("down"), false},
                new Object[] {Conditions.in("up", "down"), Conditions.gt("e"), false},
                new Object[] {Conditions.in("up", "down"), Conditions.le("e"), false},
                new Object[] {Conditions.in(ImmutableList.of("up"), ImmutableList.of("down")), Conditions.containsAny("up", "left"), false},
                new Object[] {Conditions.in(ImmutableList.of("up", "down", "left"), ImmutableList.of("up", "right")),
                        Conditions.containsAll("up", "down"), false},
                new Object[] {Conditions.in("frog", "toad"), Conditions.like("f*g"), false},
                new Object[] {Conditions.in("frog", "flag"), Conditions.not(Conditions.like("f*g")), false},
                new Object[] {Conditions.in(ImmutableMap.of("k1", "v1"), ImmutableMap.of("k1", "v2")),
                        Conditions.mapBuilder().matches("k1", Conditions.in("v1", "v3")).build(), false},
                new Object[] {Conditions.in("up", "down"), Conditions.not(Conditions.equal("up")), false},
                new Object[] {Conditions.in("up", "down"), Conditions.intrinsic(Intrinsic.TABLE, "test"), false},
                new Object[] {Conditions.in("do","re","mi"), Conditions.not(Conditions.like("*do*")), false},

                // Intrinsic conditions
                new Object[] {Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("table")), Conditions.alwaysTrue(), true},
                new Object[] {Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("table")),
                        Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("table")), true},
                new Object[] {Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("table")),
                        Conditions.intrinsic(Intrinsic.TABLE, Conditions.like("t*")), true},
                new Object[] {Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("table")), Conditions.alwaysFalse(), false},
                new Object[] {Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("table")),
                        Conditions.intrinsic(Intrinsic.TABLE, Conditions.like("x*")), false},
                new Object[] {Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("table")),
                        Conditions.intrinsic(Intrinsic.PLACEMENT, Conditions.equal("table")), false},
                new Object[] {Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("table")), Conditions.equal("table"), false},
                new Object[] {Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("table")), Conditions.in("table", "table2"), false},
                new Object[] {Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("table")), Conditions.isDefined(), false},
                new Object[] {Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("table")), Conditions.isString(), false},
                new Object[] {Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("table")), Conditions.contains("table"), false},
                new Object[] {Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("table")), Conditions.gt("t"), false},
                new Object[] {Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("table")),
                        Conditions.mapBuilder().contains(Intrinsic.TABLE, "table").build(), false},

                // Comparison conditions
                new Object[] {Conditions.gt(5), Conditions.alwaysTrue(), true},
                new Object[] {Conditions.gt(5), Conditions.isDefined(), true},
                new Object[] {Conditions.gt(5), Conditions.isNumber(), true},
                new Object[] {Conditions.gt("test"), Conditions.isString(), true},
                new Object[] {Conditions.gt(5), Conditions.isNumber(), true},
                new Object[] {Conditions.gt(5), Conditions.gt(5), true},
                new Object[] {Conditions.gt(5), Conditions.ge(5), true},
                new Object[] {Conditions.ge(5), Conditions.gt(4.9), true},
                new Object[] {Conditions.ge(5), Conditions.ge(5), true},
                new Object[] {Conditions.lt(5), Conditions.lt(5), true},
                new Object[] {Conditions.lt(5), Conditions.le(5), true},
                new Object[] {Conditions.le(5), Conditions.lt(6.1), true},
                new Object[] {Conditions.le(5), Conditions.le(5), true},
                new Object[] {Conditions.gt(5), Conditions.not(Conditions.le(5)), true},
                new Object[] {Conditions.ge(5), Conditions.not(Conditions.lt(5)), true},
                new Object[] {Conditions.lt(5), Conditions.not(Conditions.ge(5)), true},
                new Object[] {Conditions.le(5), Conditions.not(Conditions.gt(5)), true},
                new Object[] {Conditions.gt(5), Conditions.alwaysFalse(), false},
                new Object[] {Conditions.gt(5), Conditions.isUndefined(), false},
                new Object[] {Conditions.gt(5), Conditions.isString(), false},
                new Object[] {Conditions.gt("test"), Conditions.isNull(), false},
                new Object[] {Conditions.gt(5), Conditions.gt(6), false},
                new Object[] {Conditions.gt(5), Conditions.ge(6), false},
                new Object[] {Conditions.ge(5), Conditions.gt(5), false},
                new Object[] {Conditions.ge(5), Conditions.ge(6), false},
                new Object[] {Conditions.lt(5), Conditions.lt(4), false},
                new Object[] {Conditions.lt(5), Conditions.le(4), false},
                new Object[] {Conditions.le(5), Conditions.lt(5), false},
                new Object[] {Conditions.le(5), Conditions.le(4), false},
                new Object[] {Conditions.le(5), Conditions.ge(20), false},
                new Object[] {Conditions.le(5), Conditions.gt(5), false},
                new Object[] {Conditions.le(5), Conditions.ge(5), false},
                new Object[] {Conditions.gt(5), Conditions.not(Conditions.le(6)), false},
                new Object[] {Conditions.ge(5), Conditions.not(Conditions.lt(6)), false},
                new Object[] {Conditions.lt(5), Conditions.not(Conditions.ge(4)), false},
                new Object[] {Conditions.le(5), Conditions.not(Conditions.gt(4)), false},

                // Contains conditions
                new Object[] {Conditions.contains("up"), Conditions.alwaysTrue(), true},
                new Object[] {Conditions.contains("up"), Conditions.isDefined(), true},
                new Object[] {Conditions.contains("up"), Conditions.isList(), true},
                new Object[] {Conditions.contains("up"), Conditions.contains("up"), true},
                new Object[] {Conditions.contains("up"), Conditions.containsAny("up", "down"), true},
                new Object[] {Conditions.containsAll("up", "left"), Conditions.containsAll("up", "left"), true},
                new Object[] {Conditions.containsAll("up", "down", "left"), Conditions.containsAll("up", "left"), true},
                new Object[] {Conditions.containsAll("up", "left"), Conditions.containsAny("up", "left", "right"), true},
                new Object[] {Conditions.containsAny("up", "left"), Conditions.containsAny("up", "down", "left"), true},
                new Object[] {Conditions.containsOnly("up", "left"), Conditions.containsOnly("up", "left"), true},
                new Object[] {Conditions.contains("up"), Conditions.alwaysFalse(), false},
                new Object[] {Conditions.contains("up"), Conditions.isUndefined(), false},
                new Object[] {Conditions.contains("up"), Conditions.isString(), false},
                new Object[] {Conditions.contains("up"), Conditions.contains("down"), false},
                new Object[] {Conditions.contains("up"), Conditions.containsAny("left", "right"), false},
                new Object[] {Conditions.contains("up"), Conditions.containsAll("up", "down"), false},
                new Object[] {Conditions.containsAll("up", "left"), Conditions.containsAll("up", "right"), false},
                new Object[] {Conditions.containsAll("up", "down"), Conditions.containsAll("up", "down", "left"), false},
                new Object[] {Conditions.containsAll("up", "left"), Conditions.containsAny("down", "right"), false},
                new Object[] {Conditions.containsAny("up", "down"), Conditions.containsAny("left", "right"), false},
                new Object[] {Conditions.containsAny("up", "down"), Conditions.containsAll("left", "right"), false},
                new Object[] {Conditions.containsOnly("up", "down"), Conditions.containsOnly("up"), false},
                new Object[] {Conditions.containsOnly("up"), Conditions.containsOnly("down"), false},
                new Object[] {Conditions.contains("up"), Conditions.equal(ImmutableList.of("up")), false},
                new Object[] {Conditions.contains("up"), Conditions.in(ImmutableList.of("up", "down"), ImmutableList.of("up", "left")), false},
                new Object[] {Conditions.contains("up"), Conditions.equal(ImmutableList.of("down")), false},
                new Object[] {Conditions.contains("up"), Conditions.intrinsic(Intrinsic.TABLE, "up"), false},
                new Object[] {Conditions.contains("up"), Conditions.like("up"), false},
                new Object[] {Conditions.contains("up"), Conditions.mapBuilder().contains("up", "up").build(), false},

                // Like conditions
                new Object[] {Conditions.like("*oa*"), Conditions.alwaysTrue(), true},
                new Object[] {Conditions.like("*oa*"), Conditions.isDefined(), true},
                new Object[] {Conditions.like("*oa*"), Conditions.isString(), true},
                new Object[] {Conditions.like("*oa*"), Conditions.not(Conditions.isNull()), true},
                new Object[] {Conditions.like("test"), Conditions.equal("test"), true},
                new Object[] {Conditions.like("\\\\dev\\\\null"), Conditions.equal("\\dev\\null"), true},
                new Object[] {Conditions.like("*oa*"), Conditions.like("*oa*"), true},
                new Object[] {Conditions.like("bo*t"), Conditions.like("bo*t"), true},
                new Object[] {Conditions.like("boa*"), Conditions.like("*oa*"), true},
                new Object[] {Conditions.like("a*b*c*d*e"), Conditions.like("*b*d*"), true},
                new Object[] {Conditions.like("a*"), Conditions.not(Conditions.like("b*")), true},
                new Object[] {Conditions.like("a*"), Conditions.not(Conditions.like("b*c")), true},
                new Object[] {Conditions.like("*z"), Conditions.not(Conditions.like("*y")), true},
                new Object[] {Conditions.like("*z"), Conditions.not(Conditions.like("x*y")), true},
                new Object[] {Conditions.like("a*az"), Conditions.not(Conditions.like("ab*yz")), true},
                new Object[] {Conditions.like("aa*z"), Conditions.not(Conditions.like("ab*yz")), true},
                new Object[] {Conditions.like("a*b*c"), Conditions.not(Conditions.like("x*y*z")), true},
                new Object[] {Conditions.like("ab*"), Conditions.ge("a"), true},
                new Object[] {Conditions.like("ab*"), Conditions.ge("ab"), true},
                new Object[] {Conditions.like("ab*"), Conditions.gt("aa"), true},
                new Object[] {Conditions.like("ab*"), Conditions.not(Conditions.le("aa")), true},
                new Object[] {Conditions.like("ab*"), Conditions.not(Conditions.le("aa")), true},
                new Object[] {Conditions.like("*oa*"), Conditions.alwaysFalse(), false},
                new Object[] {Conditions.like("*oa*"), Conditions.isUndefined(), false},
                new Object[] {Conditions.like("*oa*"), Conditions.isNumber(), false},
                new Object[] {Conditions.like("test"), Conditions.equal("contest"), false},
                new Object[] {Conditions.like("a*"), Conditions.not(Conditions.like("a*")), false},
                new Object[] {Conditions.like("a*z"), Conditions.not(Conditions.like("a*z")), false},
                new Object[] {Conditions.like("*z"), Conditions.not(Conditions.like("*z")), false},
                new Object[] {Conditions.like("*a*"), Conditions.not(Conditions.like("*e*")), false},
                new Object[] {Conditions.like("a*"), Conditions.not(Conditions.like("*z")), false},
                new Object[] {Conditions.like("*z"), Conditions.not(Conditions.like("a*")), false},
                new Object[] {Conditions.like("*"), Conditions.not(Conditions.like("a*b")), false},
                new Object[] {Conditions.like("a*b*c"), Conditions.not(Conditions.like("*b*c")), false},
                new Object[] {Conditions.like("a*b"), Conditions.not(Conditions.like("*")), false},
                new Object[] {Conditions.like("a*z"), Conditions.not(Conditions.like("ab*yz")), false},
                new Object[] {Conditions.like("ab*yz"), Conditions.not(Conditions.like("a*z")), false},
                new Object[] {Conditions.like("a*b"), Conditions.not(Conditions.isString()), false},
                new Object[] {Conditions.like("ab*c"), Conditions.like("a*bc"), false},
                new Object[] {Conditions.like("a*"), Conditions.in("apple", "ant"), false},
                new Object[] {Conditions.like("a*"), Conditions.contains("apple"), false},
                new Object[] {Conditions.like("a*"), Conditions.intrinsic(Intrinsic.TABLE, "apple"), false},
                new Object[] {Conditions.like("a*"), Conditions.mapBuilder().contains("apple", "apple").build(), false},
                new Object[] {Conditions.like("ab*"), Conditions.ge("c"), false},
                new Object[] {Conditions.like("a*"), Conditions.ge("ab"), false},
                new Object[] {Conditions.like("*a"), Conditions.gt("a"), false},
                new Object[] {Conditions.like("ab*"), Conditions.not(Conditions.ge("aa")), false},

                // Map conditions
                new Object[] {Conditions.mapBuilder().containsKey("k1").build(), Conditions.alwaysTrue(), true},
                new Object[] {Conditions.mapBuilder().containsKey("k1").build(), Conditions.isDefined(), true},
                new Object[] {Conditions.mapBuilder().containsKey("k1").build(), Conditions.isMap(), true},
                new Object[] {
                        Conditions.mapBuilder().containsKey("k1").matches("k2", Conditions.equal("value")).build(),
                        Conditions.mapBuilder().containsKey("k1").matches("k2", Conditions.equal("value")).build(), true},
                new Object[] {
                        Conditions.mapBuilder().contains("k1", 123).matches("k2", Conditions.equal("value")).build(),
                        Conditions.mapBuilder().matches("k1", Conditions.isNumber()).matches("k2", Conditions.like("*al*")).build(), true},
                new Object[] {
                        Conditions.mapBuilder().containsKey("k1").containsKey("k2").build(),
                        Conditions.mapBuilder().containsKey("k1").build(), true},
                new Object[] {
                        Conditions.mapBuilder().contains("k1", "v1").contains("k2", "v2").build(),
                        Conditions.not(Conditions.mapBuilder().contains("k1", "x").contains("k2", "y").build()), true},
                new Object[] {
                        Conditions.mapBuilder().contains("k1", "v1").contains("k2", "v2").build(),
                        Conditions.not(Conditions.mapBuilder().contains("k1", "v1").contains("k2", "y").build()), true},
                new Object[] {
                        Conditions.mapBuilder().contains("k1", "v1").contains("k2", "v2").build(),
                        Conditions.not(Conditions.mapBuilder().contains("k1", "x").contains("k2", "v2").build()), true},
                new Object[] {Conditions.mapBuilder().containsKey("k1").build(), Conditions.alwaysFalse(), false},
                new Object[] {Conditions.mapBuilder().containsKey("k1").build(), Conditions.isUndefined(), false},
                new Object[] {Conditions.mapBuilder().containsKey("k1").build(), Conditions.isString(), false},
                new Object[] {
                        Conditions.mapBuilder().containsKey("k1").matches("k2", Conditions.equal("value1")).build(),
                        Conditions.mapBuilder().containsKey("k1").matches("k2", Conditions.equal("value2")).build(), false},
                new Object[] {
                        Conditions.mapBuilder().contains("k1", 123).matches("k2", Conditions.equal("value")).build(),
                        Conditions.mapBuilder().matches("k1", Conditions.isNumber()).matches("k2", Conditions.equal("nope")).build(), false},
                new Object[] {
                        Conditions.mapBuilder().containsKey("k1").build(),
                        Conditions.mapBuilder().containsKey("k1").containsKey("k2").build(), false},
                new Object[] {
                        Conditions.mapBuilder().contains("k1", "v1").contains("k2", "v2").build(),
                        Conditions.not(Conditions.mapBuilder().contains("k1", "v1").contains("k2", "v2").build()), false},
                new Object[] {
                        Conditions.mapBuilder().contains("k1", "v1").contains("k2", "v2").build(),
                        Conditions.not(Conditions.mapBuilder().contains("k1", "v1").build()), false},
                new Object[] {
                        Conditions.mapBuilder().contains("k1", "v1").contains("k2", "v2").build(),
                        Conditions.not(Conditions.mapBuilder().contains("k2", "v2").build()), false},
                new Object[] {Conditions.mapBuilder().containsKey("k1").build(), Conditions.equal(ImmutableMap.of("k1", "v1")), false},
                new Object[] {Conditions.mapBuilder().containsKey("k1").build(), Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("k1")), false},
                new Object[] {Conditions.mapBuilder().containsKey("k1").build(), Conditions.in("k1"), false},
                new Object[] {Conditions.mapBuilder().containsKey("k1").build(), Conditions.contains("k1"), false},
                new Object[] {Conditions.mapBuilder().containsKey("k1").build(), Conditions.gt("k"), false},
                new Object[] {Conditions.mapBuilder().containsKey("k1").build(), Conditions.like("k*"), false},

                // And conditions
                new Object[] {Conditions.and(Conditions.ge("a"), Conditions.le("z")), Conditions.alwaysTrue(), true},
                new Object[] {Conditions.and(Conditions.ge("a"), Conditions.le("z")), Conditions.isDefined(), true},
                new Object[] {Conditions.and(Conditions.ge("a"), Conditions.le("z")), Conditions.isString(), true},
                new Object[] {Conditions.and(Conditions.ge("a"), Conditions.le("z")), Conditions.and(Conditions.ge("a"), Conditions.le("z")), true},
                new Object[] {Conditions.and(Conditions.ge("a"), Conditions.le("z")), Conditions.and(Conditions.ge("a"), Conditions.le("z")), true},
                new Object[] {Conditions.and(Conditions.ge("b"), Conditions.le("y")), Conditions.and(Conditions.ge("a"), Conditions.le("z")), true},
                new Object[] {Conditions.and(Conditions.ge("b"), Conditions.le("y")), Conditions.and(Conditions.ge("a"), Conditions.le("z"), Conditions.isString()), true},
                new Object[] {Conditions.and(Conditions.ge("a"), Conditions.le("z"), Conditions.like("*g*")), Conditions.and(Conditions.ge("a"), Conditions.le("z")), true},
                new Object[] {Conditions.alwaysFalse(), Conditions.and(Conditions.ge("a"), Conditions.le("z")), true},
                new Object[] {Conditions.equal("g"), Conditions.and(Conditions.ge("a"), Conditions.le("z")), true},
                new Object[] {Conditions.equal("a"), Conditions.and(Conditions.in("a", "b", "c"), Conditions.isDefined()), true},
                new Object[] {Conditions.and(Conditions.ge("a"), Conditions.le("z")), Conditions.or(Conditions.isNull(), Conditions.isString()), true},
                new Object[] {Conditions.and(Conditions.ge("a"), Conditions.le("z")), Conditions.not(Conditions.and(Conditions.isNull(), Conditions.isMap())), true},
                new Object[] {Conditions.and(Conditions.ge("a"), Conditions.le("z")), Conditions.not(Conditions.and(Conditions.isNull(), Conditions.isString())), true},
                new Object[] {Conditions.and(Conditions.gt(5), Conditions.lt(10)), Conditions.not(Conditions.and(Conditions.gt(20), Conditions.lt(30))), true},
                new Object[] {Conditions.and(Conditions.ge("a"), Conditions.le("z")), Conditions.alwaysFalse(), false},
                new Object[] {Conditions.and(Conditions.ge("a"), Conditions.le("z")), Conditions.isUndefined(), false},
                new Object[] {Conditions.and(Conditions.ge("a"), Conditions.le("z")), Conditions.isNumber(), false},
                new Object[] {Conditions.and(Conditions.ge(5), Conditions.le(10)), Conditions.isString(), false},
                new Object[] {Conditions.and(Conditions.ge("a"), Conditions.le("k")), Conditions.and(Conditions.ge("c"), Conditions.le("z")), false},
                new Object[] {Conditions.and(Conditions.ge("c"), Conditions.le("z")), Conditions.and(Conditions.ge("a"), Conditions.le("k")), false},
                new Object[] {Conditions.and(Conditions.ge("a"), Conditions.le("b")), Conditions.and(Conditions.ge("x"), Conditions.le("y")), false},
                new Object[] {Conditions.and(Conditions.ge("a")), Conditions.and(Conditions.ge("a"), Conditions.le("z")), false},
                new Object[] {Conditions.and(Conditions.ge("a"), Conditions.le("z")), Conditions.and(Conditions.ge("a"), Conditions.le("z"), Conditions.like("*g*")), false},
                new Object[] {Conditions.and(Conditions.ge("a"), Conditions.ge("b")), Conditions.and(Conditions.le("y"), Conditions.le("z")), false},
                new Object[] {Conditions.and(Conditions.le("a"), Conditions.le("b")), Conditions.and(Conditions.ge("y"), Conditions.ge("z")), false},
                new Object[] {Conditions.and(Conditions.le("a"), Conditions.le("b")), Conditions.not(Conditions.or(Conditions.lt("y"), Conditions.lt("z"))), false},
                new Object[] {Conditions.alwaysTrue(), Conditions.and(Conditions.ge("a"), Conditions.le("z")), false},
                new Object[] {Conditions.equal("g"), Conditions.and(Conditions.ge("y"), Conditions.le("z")), false},
                new Object[] {Conditions.equal("a"), Conditions.and(Conditions.in("x", "y", "z"), Conditions.isDefined()), false},
                new Object[] {Conditions.and(Conditions.ge("a"), Conditions.le("z")), Conditions.or(Conditions.isList(), Conditions.isMap()), false},
                new Object[] {Conditions.and(Conditions.like("a*"), Conditions.like("*z")), Conditions.and(Conditions.like("b*"), Conditions.like("*y")), false},
                new Object[] {Conditions.and(Conditions.like("*a*"), Conditions.like("*b*")), Conditions.not(Conditions.and(Conditions.like("*c*"), Conditions.like("*d*"))), false},
                new Object[] {Conditions.and(Conditions.gt(5), Conditions.lt(10)), Conditions.not(Conditions.and(Conditions.gt(6), Conditions.lt(9))), false},
                new Object[] {Conditions.and(Conditions.gt(5), Conditions.lt(10)), Conditions.not(Conditions.and(Conditions.gt(8), Conditions.lt(14))), false},


                // Or conditions
                new Object[] {Conditions.or(Conditions.equal("a"), Conditions.equal("b")), Conditions.alwaysTrue(), true},
                new Object[] {Conditions.or(Conditions.equal("a"), Conditions.equal("b")), Conditions.isDefined(), true},
                new Object[] {Conditions.or(Conditions.equal("a"), Conditions.equal("b")), Conditions.isString(), true},
                new Object[] {Conditions.or(Conditions.equal("a"), Conditions.equal("b")), Conditions.or(Conditions.equal("a"), Conditions.equal("b")), true},
                new Object[] {Conditions.or(Conditions.equal("a"), Conditions.equal("b")), Conditions.or(Conditions.equal("a"), Conditions.equal("b"), Conditions.equal("c")), true},
                new Object[] {Conditions.or(Conditions.equal("a"), Conditions.equal("b")), Conditions.and(Conditions.ge("a"), Conditions.le("b")), true},
                new Object[] {Conditions.alwaysFalse(), Conditions.or(Conditions.equal("a"), Conditions.equal("b")), true},
                new Object[] {Conditions.equal("a"), Conditions.or(Conditions.equal("a"), Conditions.equal("b")), true},
                new Object[] {Conditions.in("a", "b"), Conditions.or(Conditions.equal("a"), Conditions.equal("b")), true},
                new Object[] {Conditions.or(Conditions.equal("a"), Conditions.equal("b")), Conditions.alwaysFalse(), false},
                new Object[] {Conditions.or(Conditions.equal("a"), Conditions.equal("b")), Conditions.isUndefined(), false},
                new Object[] {Conditions.or(Conditions.equal("a"), Conditions.equal("b")), Conditions.isNumber(), false},
                new Object[] {Conditions.or(Conditions.equal(12), Conditions.equal("b")), Conditions.isNumber(), false},
                new Object[] {Conditions.or(Conditions.equal("a"), Conditions.equal("b")), Conditions.or(Conditions.equal("a"), Conditions.equal("c")), false},
                new Object[] {Conditions.or(Conditions.equal("a"), Conditions.equal("b"), Conditions.equal("c")), Conditions.or(Conditions.equal("a"), Conditions.equal("b")), false},
                new Object[] {Conditions.or(Conditions.equal("a"), Conditions.equal("c")), Conditions.and(Conditions.ge("b"), Conditions.le("d")), false},
                new Object[] {Conditions.alwaysTrue(), Conditions.or(Conditions.equal("a"), Conditions.equal("b")), false},
                new Object[] {Conditions.equal("c"), Conditions.or(Conditions.equal("a"), Conditions.equal("b")), false},
                new Object[] {Conditions.in("a", "c"), Conditions.or(Conditions.equal("a"), Conditions.equal("b")), false},
                new Object[] {Conditions.isString(), Conditions.or(Conditions.equal("a"), Conditions.equal("b")), false},

                // Not conditions
                new Object[] {Conditions.not(Conditions.alwaysTrue()), Conditions.alwaysFalse(), true},
                new Object[] {Conditions.not(Conditions.alwaysFalse()), Conditions.alwaysTrue(), true},
                new Object[] {Conditions.not(Conditions.alwaysTrue()), Conditions.alwaysTrue(), true},
                new Object[] {Conditions.not(Conditions.isUndefined()), Conditions.isDefined(), true},
                new Object[] {Conditions.not(Conditions.isDefined()), Conditions.isUndefined(), true},
                new Object[] {Conditions.and(Conditions.like("ab*"), Conditions.not(Conditions.equal("abc"))), Conditions.like("a*"), true},
                new Object[] {Conditions.not(Conditions.or(Conditions.isUndefined(), Conditions.equal("v1"))), Conditions.isDefined(), true},
                new Object[] {Conditions.not(Conditions.and(Conditions.equal("a"), Conditions.equal("b"))),
                        Conditions.or(Conditions.not(Conditions.equal("a")), Conditions.not(Conditions.equal("b"))), true},
                new Object[] {Conditions.not(Conditions.or(Conditions.equal("a"), Conditions.equal("b"))),
                        Conditions.and(Conditions.not(Conditions.equal("a")), Conditions.not(Conditions.equal("b"))), true},
                new Object[] {Conditions.not(Conditions.like("a*")), Conditions.not(Conditions.like("a*")), true},
                new Object[] {Conditions.not(Conditions.like("a*")), Conditions.not(Conditions.like("ab*")), true},
                new Object[] {Conditions.not(Conditions.alwaysFalse()), Conditions.alwaysFalse(), false},
                new Object[] {Conditions.not(Conditions.isUndefined()), Conditions.isUndefined(), false},
                new Object[] {Conditions.not(Conditions.isDefined()), Conditions.isDefined(), false},
                new Object[] {Conditions.not(Conditions.isDefined()), Conditions.isString(), false},
                new Object[] {Conditions.not(Conditions.equal("bc")), Conditions.like("a*"), false},
                new Object[] {Conditions.not(Conditions.equal("v1")), Conditions.isDefined(), false},
                new Object[] {Conditions.not(Conditions.and(Conditions.equal("a"), Conditions.equal("b"))),
                        Conditions.or(Conditions.not(Conditions.equal("a")), Conditions.not(Conditions.equal("c"))), false},
                new Object[] {Conditions.not(Conditions.or(Conditions.equal("a"), Conditions.equal("b"))),
                        Conditions.and(Conditions.not(Conditions.equal("a")), Conditions.not(Conditions.equal("c"))), false},
                new Object[] {Conditions.not(Conditions.like("ab*")), Conditions.not(Conditions.like("a*")), false},


                // The following are tests of false-negatives resulting from a "not" condition on the left.  Ideally
                // these would return true but the logic for them to do so is more complicated than the benefit derived
                // from a complete implementation, especially considering it is always trivial to rewrite the condition
                // in a way that can be detected as a subset correctly.

                // False-negatives
                new Object[] {Conditions.and(Conditions.isNumber(), Conditions.not(Conditions.gt(1))), Conditions.le(5), false},
                new Object[] {Conditions.and(
                        Conditions.isMap(),
                        Conditions.not(Conditions.mapBuilder().matches("k1", Conditions.or(Conditions.isUndefined(), Conditions.equal("v1"))).build())),
                        Conditions.mapBuilder().matches("k1", Conditions.isDefined()).build(), false},
                // Equivalent expressions
                new Object[] {Conditions.le(1), Conditions.le(5), true},
                new Object[] {Conditions.mapBuilder().matches("k1", Conditions.not(Conditions.or(Conditions.isUndefined(), Conditions.equal("v1")))).build(),
                        Conditions.mapBuilder().matches("k1", Conditions.isDefined()).build(), true},
        };
    }

    @Test(dataProvider = "conditions")
    public void testSubset(Condition left, Condition right, boolean isSubset) {
        assertEquals(SubsetEvaluator.isSubset(left, right), isSubset);
    }
}