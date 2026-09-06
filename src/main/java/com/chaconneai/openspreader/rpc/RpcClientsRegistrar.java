package com.chaconneai.openspreader.rpc;

import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.util.ClassUtils;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Handles {@link EnableRpcClients}: scans the packages named on the annotation.
 *
 * <p>The only difference from {@link RpcClientRegistrar}, the one auto-configuration uses,
 * is where the scan scope comes from: that one takes it from
 * {@code AutoConfigurationPackages}, this one from the annotation. The scanning itself is
 * the same
 * {@link ClassPathRpcClientScanner}。
 *
 * <p>The two <b>can both be active</b>, and an interface scanned twice is not registered
 * twice: the scanner skips it on finding a bean definition of that name already there.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class RpcClientsRegistrar implements ImportBeanDefinitionRegistrar,
        ResourceLoaderAware, EnvironmentAware {

    private ResourceLoader resourceLoader;
    private Environment environment;

    @Override
    public void setResourceLoader(ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
    }

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void registerBeanDefinitions(AnnotationMetadata metadata,
                                        BeanDefinitionRegistry registry) {
        AnnotationAttributes attributes = AnnotationAttributes.fromMap(
                metadata.getAnnotationAttributes(EnableRpcClients.class.getName()));
        if (attributes == null) {
            return;
        }

        Set<String> packages = resolvePackages(attributes, metadata);
        if (packages.isEmpty()) {
            return;
        }

        ClassPathRpcClientScanner scanner = new ClassPathRpcClientScanner(registry);
        if (resourceLoader != null) {
            scanner.setResourceLoader(resourceLoader);
        }
        if (environment != null) {
            scanner.setEnvironment(environment);
        }
        scanner.scan(packages.toArray(new String[0]));
    }

    /**
     * The three attributes together make up the scan scope.
     *
     * <p>With none of them set it degenerates to the package of the annotated class --
     * usually the application's main class, which matches the default behaviour when the
     * annotation is absent altogether.
     */
    private Set<String> resolvePackages(AnnotationAttributes attributes,
                                        AnnotationMetadata metadata) {
        Set<String> packages = new LinkedHashSet<>();
        for (String p : attributes.getStringArray("value")) {
            addIfPresent(packages, p);
        }
        for (String p : attributes.getStringArray("basePackages")) {
            addIfPresent(packages, p);
        }
        for (Class<?> clazz : attributes.getClassArray("basePackageClasses")) {
            packages.add(ClassUtils.getPackageName(clazz));
        }
        if (packages.isEmpty()) {
            packages.add(ClassUtils.getPackageName(metadata.getClassName()));
        }
        return packages;
    }

    /** Package names accept {@code ${...}} placeholders, so the scan scope can be
     *  configured per environment too. */
    private void addIfPresent(Set<String> packages, String value) {
        if (value == null || value.isBlank()) {
            return;
        }
        packages.add(environment == null ? value.trim()
                : environment.resolvePlaceholders(value.trim()));
    }
}
