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
package com.chaconneai.openspreader.rpc;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.annotation.AnnotatedElementUtils;

/**
 * Turns an interface annotated {@link RpcClient} into an injectable bean.
 *
 * <p>An interface cannot be {@code new}ed, and Spring will not implement it for you --
 * something in between has to produce an implementation. That is what a
 * {@link FactoryBean} is for: the container registers the return value of
 * {@link #getObject()} rather than the factory itself.
 *
 * <p>{@link RpcService} is fetched <b>at use time</b> rather than injected in the
 * constructor. RPC client bean definitions must be registered very early, before the
 * dependency chain has been built at all, and requiring the service at construction time
 * would leave the startup order stretched taut -- one circular dependency and it breaks.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class RpcClientFactoryBean implements FactoryBean<Object>, ApplicationContextAware {

    private final Class<?> apiType;

    private ApplicationContext context;

    public RpcClientFactoryBean(Class<?> apiType) {
        this.apiType = apiType;
    }

    @Override
    public void setApplicationContext(ApplicationContext context) throws BeansException {
        this.context = context;
    }

    @Override
    public Object getObject() {
        RpcClient config = AnnotatedElementUtils.findMergedAnnotation(apiType, RpcClient.class);
        if (config == null) {
            throw new RpcException(apiType.getName() + " is not annotated @RpcClient");
        }
        // The bean factory rather than the ApplicationContext: serviceId's ${...} and #{...}
        // are resolved through it, and the resolvers hang only off ConfigurableBeanFactory
        Object factory = context instanceof ConfigurableApplicationContext c
                ? c.getBeanFactory()
                : context;
        return RpcClientProxy.create(apiType, config, context.getBean(RpcService.class),
                (BeanFactory) factory, context.getBean(RpcDefaults.class));
    }

    @Override
    public Class<?> getObjectType() {
        return apiType;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }
}
