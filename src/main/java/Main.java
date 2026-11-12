import org.apache.catalina.Context;
import org.apache.catalina.WebResourceRoot;
import org.apache.catalina.core.StandardContext;
import org.apache.catalina.startup.Tomcat;
import org.apache.catalina.webresources.DirResourceSet;
import org.apache.catalina.webresources.StandardRoot;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Standalone launcher for the existing JSP/servlet portal. It starts the Kafka
 * runtime (producer, 20 partition workers, scheduler) and then embedded Tomcat.
 *
 * <p>Run from IntelliJ or with {@code mvn exec:java}. External WAR deployment
 * remains supported through {@link ListenerClass}.</p>
 */
public class Main {

    private static final Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) throws Exception {
        AppConfig config = AppConfig.getInstance();
        KafkaRuntime runtime = KafkaRuntime.getInstance();
        Tomcat tomcat = null;

        try {
            runtime.start();
            Tomcat server = createTomcat(config);
            tomcat = server;
            server.start();

            Context context = (Context) server.getHost().findChild("");
            if (context != null && context.getServletContext() != null) {
                context.getServletContext().setAttribute("messageProducer", runtime.getProducer());
            } else {
                logger.warning("ServletContext is not available; MessageServlet will use KafkaRuntime directly");
            }

            logger.info("Portal available at http://localhost:" + config.getHttpPort() + "/");
            Runtime.getRuntime().addShutdownHook(new Thread(
                    () -> shutdown(server), "application-shutdown"));

            server.getServer().await();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Application startup failed", e);
            shutdown(tomcat);
            throw e;
        }
    }

    private static Tomcat createTomcat(AppConfig config) throws Exception {
        Tomcat tomcat = new Tomcat();
        Path baseDir = Files.createTempDirectory("embedded-tomcat-");
        tomcat.setBaseDir(baseDir.toString());
        tomcat.setPort(config.getHttpPort());
        tomcat.getConnector();

        File webappDir = new File("src/main/webapp");
        if (!webappDir.isDirectory()) {
            throw new IllegalStateException("Web application directory not found: "
                    + webappDir.getAbsolutePath());
        }

        Context context = tomcat.addWebapp("", webappDir.getAbsolutePath());
        // Main and the webapp must share one KafkaRuntime singleton. Without
        // parent-first delegation the webapp loads its own copy of every app
        // class, so ListenerClass would start a second producer/consumer set.
        if (context instanceof StandardContext standardContext) {
            standardContext.setDelegate(true);
        }
        File classesDir = resolveClassesDirectory();
        if (classesDir.isDirectory()) {
            WebResourceRoot resources = new StandardRoot(context);
            resources.addPreResources(new DirResourceSet(
                    resources,
                    "/WEB-INF/classes",
                    classesDir.getAbsolutePath(),
                    "/"));
            context.setResources(resources);
            logger.info("Embedded Tomcat classes directory: " + classesDir.getAbsolutePath());
        } else {
            logger.warning("Classes directory was not found; annotation scanning may be incomplete");
        }

        return tomcat;
    }

    private static File resolveClassesDirectory() {
        try {
            File codeSource = new File(Main.class.getProtectionDomain()
                    .getCodeSource()
                    .getLocation()
                    .toURI());
            if (codeSource.isDirectory()) {
                return codeSource;
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "Unable to resolve the code source directory", e);
        }

        File targetClasses = new File("target/classes");
        if (targetClasses.isDirectory()) {
            return targetClasses;
        }
        File ideaOutput = new File("out/production/Testing");
        if (ideaOutput.isDirectory()) {
            return ideaOutput;
        }
        return new File("target/classes");
    }

    private static void shutdown(Tomcat tomcat) {
        try {
            KafkaRuntime.getInstance().stop();
        } catch (RuntimeException e) {
            logger.log(Level.WARNING, "Failed to stop Kafka runtime", e);
        }

        if (tomcat != null) {
            try {
                tomcat.stop();
            } catch (Exception e) {
                logger.log(Level.WARNING, "Failed to stop embedded Tomcat", e);
            }
            try {
                tomcat.destroy();
            } catch (Exception e) {
                logger.log(Level.WARNING, "Failed to destroy embedded Tomcat", e);
            }
        }
    }
}
