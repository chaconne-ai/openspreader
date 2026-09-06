package com.chaconneai.openspreader.rpc;

import org.springframework.context.annotation.Import;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Names the packages to search for {@link RpcClient} interfaces.
 *
 * <pre>{@code
 * @SpringBootApplication
 * @EnableRpcClients(basePackages = "com.example.remote.api")
 * public class Application { ... }
 * }</pre>
 *
 * <h2>Usually you do not need it</h2>
 * The default scans the application's main class package and its subpackages, matching
 * {@code @SpringBootApplication}. An interface in its usual place in the project is
 * already found, with nothing to configure.
 *
 * <p>There is one case that genuinely needs it: <b>the interface comes from an external
 * dependency</b> -- someone published the API interfaces as their own jar, under a package
 * outside your application's, where the default scan does not reach.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(RpcClientsRegistrar.class)
public @interface EnableRpcClients {

    /** An alias for {@link #basePackages()}, so the package names can be given on their own. */
    String[] value() default {};

    /**
     * The packages to scan.
     *
     * <p>Setting this scans <b>these and nothing else</b>; the default main-class package is
     * no longer added automatically -- so if your own interfaces need scanning too, list
     * them as well.
     */
    String[] basePackages() default {};

    /**
     * Names packages by class: the packages these classes live in.
     *
     * <p>Safer than strings -- rename a package or move a class and the compiler says so,
     * whereas a string simply scans nothing at runtime, silently.
     */
    Class<?>[] basePackageClasses() default {};
}
