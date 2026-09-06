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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ClassPathBeanDefinitionScanner;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.util.ClassUtils;

import java.util.Set;

/**
 * Scans the classpath for interfaces annotated {@link RpcClient} and registers each as an
 * injectable proxy bean.
 *
 * <h2>Why a scanner of its own is needed</h2>
 * Spring's own component scan <b>skips interfaces</b> -- an interface cannot be
 * instantiated, so finding one achieves nothing. And {@code @RpcClient} appears on nothing
 * but interfaces. Two things are therefore overridden:
 * <ul>
 *   <li>{@link #isCandidateComponent} -- to admit independent interfaces, the exact opposite
 *       of the default</li>
 *   <li>{@link #doScan} -- a scanned bean definition points at the interface itself and is
 *       unusable as it stands, so it is changed to a {@link RpcClientFactoryBean}, which
 *       builds the proxy</li>
 * </ul>
 *
 * <p>This is the standard "the interface is the client" approach in the Spring ecosystem;
 * MyBatis's {@code ClassPathMapperScanner} and OpenFeign's scanner do exactly the same.
 *
 * <h2>It can be used on its own</h2>
 * Auto-configuration scans the main class's package by default (see
 * {@link RpcClientRegistrar}). Where the interfaces live elsewhere, or the scan scope needs
 * precise control, use {@code @EnableRpcClients(basePackages = "...")} -- which ends up
 * here too.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class ClassPathRpcClientScanner extends ClassPathBeanDefinitionScanner {

    private static final Logger log = LoggerFactory.getLogger(ClassPathRpcClientScanner.class);

    /** The attribute Spring reads to learn what a FactoryBean will produce <b>without
     *  instantiating it</b>. */
    private static final String FACTORY_BEAN_OBJECT_TYPE = "factoryBeanObjectType";

    public ClassPathRpcClientScanner(BeanDefinitionRegistry registry) {
        // useDefaultFilters=false: only what is annotated @RpcClient, not the whole
        // @Component family
        super(registry, false);
        addIncludeFilter(new AnnotationTypeFilter(RpcClient.class));
    }

    /**
     * Accepts interfaces only, and only top-level or statically nested ones.
     *
     * <p>The independence condition cannot be dropped: an inner class cannot be loaded
     * without an enclosing instance, and an RPC interface has no business being one
     * anyway.
     */
    @Override
    protected boolean isCandidateComponent(AnnotatedBeanDefinition beanDefinition) {
        return beanDefinition.getMetadata().isInterface()
                && beanDefinition.getMetadata().isIndependent();
    }

    /**
     * Scans, then turns each bean definition into the factory that builds the proxy.
     *
     * @return those successfully registered
     */
    @Override
    protected Set<BeanDefinitionHolder> doScan(String... basePackages) {
        Set<BeanDefinitionHolder> holders = super.doScan(basePackages);
        if (holders.isEmpty()) {
            log.debug("No @RpcClient interfaces were found under {}", String.join(", ", basePackages));
            return holders;
        }
        for (BeanDefinitionHolder holder : holders) {
            convertToFactoryBean(holder);
        }
        log.info("Registered {} @RpcClient remote interface(s)", holders.size());
        return holders;
    }

    /** Replaces a bean definition that points at the interface with one that points at the factory. */
    private void convertToFactoryBean(BeanDefinitionHolder holder) {
        AbstractBeanDefinition definition = (AbstractBeanDefinition) holder.getBeanDefinition();
        String apiClassName = definition.getBeanClassName();
        if (apiClassName == null) {
            return;
        }
        Class<?> apiType;
        try {
            apiType = ClassUtils.forName(apiClassName, getClass().getClassLoader());
        } catch (ClassNotFoundException | LinkageError e) {
            log.warn("Found {} but could not load it; skipping: {}", apiClassName, e.toString());
            return;
        }

        // The interface type goes in as a constructor argument, so the factory knows what to proxy
        definition.getConstructorArgumentValues().addGenericArgumentValue(apiType);
        definition.setBeanClass(RpcClientFactoryBean.class);
        // So that injection by interface type matches this, rather than matching the
        // FactoryBean itself. The value must be a Class and not a class name string: Spring
        // validates this attribute's type, and a string produces "Invalid value type for
        // attribute" at container startup
        definition.setAttribute(FACTORY_BEAN_OBJECT_TYPE, apiType);
        definition.setPrimary(true);
        log.debug("Registered RPC client: {} -> {}", holder.getBeanName(), apiClassName);
    }
}
