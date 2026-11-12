<%@ page contentType="text/html;charset=UTF-8" language="java" isELIgnored="false" %>
<html>
<head>
  <title>Message Dashboard</title>
</head>
<body>
<h2>Post a Message</h2>

<%
  Object error = request.getAttribute("error");
  if (error != null) {
%>
<p style="color:#b00020;"><strong>Error:</strong> <%= error %></p>
<%
  }
  if ("true".equals(request.getParameter("posted"))) {
%>
<p style="color:#0a7a2f;"><strong>Message queued successfully.</strong></p>
<%
  }
%>

<form action="${pageContext.request.contextPath}/message" method="post">
  <label for="content">Content</label><br>
  <textarea id="content" name="Content" rows="8" cols="60"></textarea><br><br>

  <label for="type">Type</label>
  <select id="type" name="Type">
    <option value="INFO">INFO</option>
    <option value="WARN">WARN</option>
    <option value="FINE">FINE</option>
    <option value="SEVERE">SEVERE</option>
  </select><br><br>

  <label for="priority">Priority</label>
  <input id="priority" type="checkbox" name="priority" value="true"><br><br>

  <button type="submit">Submit</button>
</form>
</body>
</html>
