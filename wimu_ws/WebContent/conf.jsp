<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Where is my URI</title>
<%
	if (request.getParameter("dirInd") != null) {
		System.setProperty("dirInd", request.getParameter("dirInd"));
		response.sendRedirect("index.jsp");
		return;
	}
%>
</head>
<body>
	<h1>Where is my URI?</h1>
	Configurations:
	<br>
	<br>

	<form action="conf.jsp">
		Dir index: <input type="text" name="dirInd" size="100">
		<br> <input type="submit" value="Send">
	</form>
</body>
</html>