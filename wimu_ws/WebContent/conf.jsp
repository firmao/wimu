<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Insert title here</title>
</head>
<body>
<h1>Where is my URI?</h1>
	<input type="text" id="dirconf" size="100">
	<button onclick="setConf()">Set Conf</button>
<script>
		function setConf() {
			var dirHDT = document.getElementById("dirconf").value;
			window.location.assign("Find?dirHDT=" + dirHDT);
		}
	</script>
</body>
</html>