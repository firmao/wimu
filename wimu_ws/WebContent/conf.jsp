<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Where is my URI</title>
<%
	out.println(request.getParameter("dirHDT"));
	out.println(request.getParameter("dirEndpoints"));
	out.println(request.getParameter("dirDumps"));
	if ((request.getParameter("dirHDT") != null) && (request.getParameter("dirEndpoints") != null)
			&& (request.getParameter("dirDumps") != null)) {
		request.getSession().setAttribute("dirHDT", request.getParameter("dirHDT"));
		request.getSession().setAttribute("dirEndpoints", request.getParameter("dirEndpoints"));
		request.getSession().setAttribute("dirDumps", request.getParameter("dirDumps"));

// 		try {
// 			HDTQueryMan.loadFileMap("md5HDT.csv");
// 		} catch (Exception e) {
// 			e.printStackTrace();
// 		}
		
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
		Dir HDT: <input type="text" name="dirHDT" size="100"> <br>
		Dir Lucene endpoints: <input type="text" name="dirEndpoints" size="100">
		<br> Dir Lucene dumps: <input type="text" name="dirDumps"
			size="100"> <br> <input type="submit" value="Send">
	</form>
</body>
</html>