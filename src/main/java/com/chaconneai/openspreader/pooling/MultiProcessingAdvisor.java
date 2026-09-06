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
package com.chaconneai.openspreader.pooling;

import org.aopalliance.aop.Advice;
import org.springframework.aop.Pointcut;
import org.springframework.aop.support.AbstractPointcutAdvisor;
import org.springframework.aop.support.annotation.AnnotationMatchingPointcut;
import org.springframework.core.Ordered;

/**
 * Attaches {@link MultiProcessingInterceptor} to every method annotated
 * {@link MultiProcessingCall}.
 *
 * <h2>Why its order is near the outermost</h2>
 * Dispatching means the call <b>may not run in this process at all</b>, so wrapping other
 * aspects on this side -- transactions, caching, retries -- around it achieves nothing:
 * there is no transaction to open locally. The side that needs those aspects is <b>the one
 * executing</b>, and that side calls a proxy, where they apply as usual.
 *
 * <p>Hence an early order: decide <i>where</i> it runs first, and let the side running it
 * decide <i>how</i>. {@code @Async} is the one exception -- either order works with
 * dispatch; see the class documentation on {@link MultiProcessingInterceptor}.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class MultiProcessingAdvisor extends AbstractPointcutAdvisor {

    /**
     * Ahead of the default aspects, while leaving room for a user's own aspects to sit
     * further out.
     *
     * <p>Not {@link Ordered#HIGHEST_PRECEDENCE}: that would let nothing sit outside it, and
     * "do something before dispatching" -- instrumentation, authorisation -- is a reasonable
     * thing to want.
     */
    public static final int ORDER = Ordered.HIGHEST_PRECEDENCE + 100;

    private final Advice advice;

    public MultiProcessingAdvisor(MultiProcessingInterceptor interceptor) {
        this.advice = interceptor;
        setOrder(ORDER);
    }

    @Override
    public Pointcut getPointcut() {
        // Matches the annotation on methods only, not on the class: dispatch is opened per
        // method and should not be opened for a whole class at once
        return new AnnotationMatchingPointcut(null, MultiProcessingCall.class, true);
    }

    @Override
    public Advice getAdvice() {
        return advice;
    }
}
