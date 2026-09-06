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
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ResourceLoader;

import java.util.List;

/**
 * Runs {@link ClassPathRpcClientScanner} early in the container's startup.
 *
 * <h2>Why a BeanDefinitionRegistryPostProcessor</h2>
 * RPC client bean definitions have to be registered <b>before anything else starts
 * depending on them</b>. When a business service injects {@code OrderApi} in its
 * constructor, the container either already knows of that bean or reports it missing --
 * there is no state in between. This extension point falls exactly at the moment when all
 * bean definitions have been read and none has been instantiated.
 *
 * <h2>Which packages are scanned</h2>
 * By default the application's main class package and its subpackages, matching
 * {@code @SpringBootApplication} -- put the interfaces in the usual place and no extra
 * configuration is needed. To customise it, use
 * {@link EnableRpcClients @EnableRpcClients(basePackages = "…")}。
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class RpcClientRegistrar implements BeanDefinitionRegistryPostProcessor,
        ResourceLoaderAware, EnvironmentAware {

    private static final Logger log = LoggerFactory.getLogger(RpcClientRegistrar.class);

    private final List<String> basePackages;

    private ResourceLoader resourceLoader;
    private Environment environment;

    public RpcClientRegistrar(List<String> basePackages) {
        this.basePackages = basePackages;
    }

    @Override
    public void setResourceLoader(ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
    }

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) {
        if (basePackages == null || basePackages.isEmpty()) {
            log.debug("No packages to scan; skipping the @RpcClient scan");
            return;
        }
        ClassPathRpcClientScanner scanner = new ClassPathRpcClientScanner(registry);
        if (resourceLoader != null) {
            scanner.setResourceLoader(resourceLoader);
        }
        if (environment != null) {
            scanner.setEnvironment(environment);
        }
        scanner.scan(basePackages.toArray(new String[0]));
    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) {
        // The bean factory itself needs no changes; all the work was done in the step above
    }
}
