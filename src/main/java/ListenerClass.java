import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;
import jakarta.servlet.annotation.WebListener;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * WAR lifecycle bridge. External Tomcat deployments start the same Kafka
 * runtime that Main starts for embedded Tomcat.
 */
@WebListener
public class ListenerClass implements ServletContextListener {

    private static final Logger logger = Logger.getLogger(ListenerClass.class.getName());

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        configureLogging();

        KafkaRuntime runtime = KafkaRuntime.getInstance();
        runtime.start();
        sce.getServletContext().setAttribute("messageProducer", runtime.getProducer());
        logger.info("Message producer is available to the portal");
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        KafkaRuntime.getInstance().stop();
    }

    private void configureLogging() {
        InputStream input = getClass().getClassLoader().getResourceAsStream("application.properties");
        if (input == null) {
            return;
        }
        try (InputStream stream = input) {
            LogManager.getLogManager().readConfiguration(stream);
            logger.info("Log manager configured successfully");
        } catch (IOException e) {
            logger.warning("Unable to configure java.util.logging: " + e.getMessage());
        }
    }
}
