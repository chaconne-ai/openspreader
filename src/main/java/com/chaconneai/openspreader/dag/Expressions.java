/*
 * Copyright 2026 ChaconneAI
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.chaconneai.openspreader.dag;

import org.springframework.expression.EvaluationException;
import org.springframework.expression.Expression;
import org.springframework.expression.ParseException;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Turns a SpEL string into a condition the graph can use.
 *
 * <p>So that a branch can be written the way it would be said:
 *
 * <pre>{@code
 * .from(Score.class)
 *     .when("#risk > 80").to(HumanReview.class)
 *     .when("#risk > 40").to(SecondLook.class)
 *     .otherwise(AutoApprove.class)
 *
 * .from(Classify.class)
 *     .switchOn("#kind")                       // the channel's value is the branch key
 *     .caseOf("book",  ShipMedia.class)
 *     .caseOf("fresh", ShipCold.class)
 * }</pre>
 *
 * <h2>Channels are variables</h2>
 * Every channel is bound as a SpEL variable of its own name, so {@code #risk} is the
 * {@code risk} channel. The whole state is bound as {@code #state} for the times a method on
 * it is wanted, such as {@code #state.getList('items').size()}.
 *
 * <h2>An absent channel is null, and null is false</h2>
 * A channel that was never written is bound as {@code null}. SpEL <b>does not throw</b> when
 * such a value is compared: {@code #missing > 10} is simply false, and a branch guarded by it
 * is not taken. That is the behaviour wanted here, and it is worth stating because the
 * opposite is the natural guess for anyone thinking in Java, where the same comparison would
 * be a NullPointerException.
 *
 * <p>What does throw is <b>reaching into</b> a null: {@code #missing.length()}. Use SpEL's
 * safe navigation ({@code #order?.total}) or an explicit guard
 * ({@code #order != null and #order.total > 10}) for a channel that may be absent.
 *
 * <h2>Parsed at build time, evaluated at run time</h2>
 * A malformed expression fails as the graph is built, not part-way through the first run.
 * That is the whole reason this class exists rather than a lambda calling the parser inline.
 *
 * <h2>When to use a lambda instead</h2>
 * A lambda is checked by the compiler and refactors with the code; SpEL is a string. Use SpEL
 * where the condition belongs to configuration rather than to the code, or where a graph is
 * assembled at run time from something a person edited. Use a lambda everywhere else.
 *
 * <p>Note also that SpEL can reach beyond the state, and an expression built by joining
 * user input into a string is an injection waiting to happen. Expressions here should come
 * from the application, not from its requests.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 12/09/2026
 */
public class Expressions {

    /** Stateless and thread-safe, so one is enough. */
    private static final SpelExpressionParser PARSER = new SpelExpressionParser();

    private Expressions() {
    }

    /** A condition that must evaluate to a boolean. */
    public static Predicate<GraphState> predicate(String spel) {
        Expression expression = parse(spel);
        return state -> {
            Object value = evaluate(expression, spel, state);
            if (value == null) {
                // A condition that is neither true nor false is false. Failing the run for it
                // would be harsh: an absent channel making a branch not apply is ordinary.
                // Note that SpEL reaches here rarely, because it already answers false for a
                // comparison against an unset variable rather than returning null
                return false;
            }
            if (!(value instanceof Boolean)) {
                throw new DagException("the condition \"" + spel + "\" evaluated to a "
                        + value.getClass().getSimpleName() + " rather than a boolean. For a "
                        + "switch on a value, use switchOn(...) instead of when(...)");
            }
            return (Boolean) value;
        };
    }

    /** A router whose result is the branch key. A null result matches no case. */
    public static Function<GraphState, String> router(String spel) {
        Expression expression = parse(spel);
        return state -> {
            Object value = evaluate(expression, spel, state);
            return value == null ? null : String.valueOf(value);
        };
    }

    private static Expression parse(String spel) {
        if (spel == null || spel.isBlank()) {
            throw new IllegalArgumentException("an expression must not be blank");
        }
        try {
            return PARSER.parseExpression(spel);
        } catch (ParseException e) {
            throw new DagException("the expression \"" + spel + "\" does not parse", e);
        }
    }

    private static Object evaluate(Expression expression, String spel, GraphState state) {
        StandardEvaluationContext context = new StandardEvaluationContext();
        context.setVariable("state", state);
        for (Map.Entry<String, Object> channel : state.asMap().entrySet()) {
            context.setVariable(channel.getKey(), channel.getValue());
        }
        try {
            return expression.getValue(context);
        } catch (EvaluationException e) {
            // Reaching into a null is the usual cause: #order.total where nothing wrote
            // "order". A comparison against an absent channel does not come here, it is
            // simply false
            throw new DagException("the expression \"" + spel + "\" failed against the state "
                    + state.channels() + ". Reaching into a channel that may be absent needs "
                    + "a guard, such as \"#order?.total\" or \"#order != null and "
                    + "#order.total > 10\"", e);
        }
    }
}
