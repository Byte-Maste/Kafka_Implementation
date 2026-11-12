import jakarta.servlet.ServletException;
import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

@WebServlet("/message")
public class MessageServlet extends HttpServlet {

    private static final Logger logger = Logger.getLogger(MessageServlet.class.getName());

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        // Tomcat defaults form bodies to ISO-8859-1. Without this, non-ASCII
        // content is decoded as Latin-1 and measured at up to 2x its real size.
        req.setCharacterEncoding("UTF-8");

        HttpSession session = req.getSession(false);
        if (session == null || session.getAttribute("username") == null) {
            resp.sendRedirect(req.getContextPath() + "/login.jsp");
            return;
        }

        MessageProducer producer = resolveProducer();
        if (producer == null) {
            logger.severe("MessageProducer is not available");
            resp.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Message queue is not available");
            return;
        }

        int userId = resolveUserId(session);
        if (userId <= 0) {
            logger.warning("Unable to resolve a valid user id for session");
            resp.sendRedirect(req.getContextPath() + "/login.jsp");
            return;
        }

        String content = req.getParameter("Content");
        String type = req.getParameter("Type");
        String priorityParameter = req.getParameter("priority");
        boolean priority = "true".equalsIgnoreCase(priorityParameter)
                || "yes".equalsIgnoreCase(priorityParameter)
                || "on".equalsIgnoreCase(priorityParameter);

        try {
            MessageProducer.SendResult result = producer.send(userId, type, content, priority);
            logger.info("Queued message msgId=" + result.msgId()
                    + " topic=" + result.topic()
                    + " partition=" + result.partition()
                    + " offset=" + result.offset());
            resp.sendRedirect(req.getContextPath() + "/message.jsp?posted=true");
        } catch (IllegalArgumentException e) {
            logger.log(Level.WARNING, "Message rejected: " + e.getMessage(), e);
            req.setAttribute("error", e.getMessage());
            req.getRequestDispatcher("/message.jsp").forward(req, resp);
        } catch (RuntimeException e) {
            logger.log(Level.SEVERE, "Failed to publish message", e);
            req.setAttribute("error", "Message could not be queued. Please try again.");
            req.getRequestDispatcher("/message.jsp").forward(req, resp);
        }
    }

    private MessageProducer resolveProducer() {
        Object attribute = getServletContext().getAttribute("messageProducer");
        if (attribute instanceof MessageProducer producer) {
            return producer;
        }
        try {
            return KafkaRuntime.getInstance().getProducer();
        } catch (IllegalStateException e) {
            return null;
        }
    }

    private int resolveUserId(HttpSession session) {
        Object sessionUserId = session.getAttribute("user_id");
        if (sessionUserId instanceof Integer userId && userId > 0) {
            return userId;
        }
        String username = (String) session.getAttribute("username");
        if (username == null) {
            return -1;
        }
        return new UserDAO().getUserID(username);
    }
}
