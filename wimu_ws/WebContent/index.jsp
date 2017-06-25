<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html;charset=utf-8" />
<title property="dc:title">Where is my URI?</title>
<link rel="stylesheet" href="style.css" type="text/css" />
<script src="jquery.js" type="text/javascript"></script>
<script src="zeroclipboard.js" type="text/javascript"></script>
<script src="script.js" type="text/javascript"></script>
</head>
<body>
<style>
div {
position:absolute;
        top: 50%;
        left: 28%;
        width:50em;
        height:8em;
        margin-top: -5em; /*set to a negative number 1/2 of your height*/
        margin-left: -5em; /*set to a negative number 1/2 of your width*/
        border: 1px solid #ccc;
        border:  2px solid black;
        z-index:100; 
        background-color: white;
}

.hider{
position:absolute;
        top: 30%;
        left: 30%;
        width:1490px;
        height:1045px;
        margin-top: -800px; /*set to a negative number 1/2 of your height*/
        margin-left: -500px; /*set to a negative number 1/2 of your width*/
        /*
        z- index must be lower than pop up box
       */
        z-index: 99;
       background-color:Black;
       opacity:0.6;
}

</style>

	<h1>Where is my URI?</h1>
	<input type="text" id="uri" size="80">
	<button onclick="findEndPoint()">Find Datasets</button>
	<button onclick="findURI()">Find the URIs</button>
	<button onclick="findSQL()">SQL query</button>
	<br>Examples: 
	<br>URI: http://semanticscience.org/resource/SIO_000272
	<br>EndPoint: http://crm.rkbexplorer.com/sparql
	<div id="loadingDiv" style="display: none;">
		<h1>Please wait, Loading...</h1>
		<img src="img/comi.gif">
	</div>
<div id="divhider" class="hider" style="display: none;">
		
	</div>
	<script>
		function findEndPoint() {
			document.getElementById('loadingDiv').style.display = "block";
			document.getElementById('divhider').style.display = "block";
			var url = document.getElementById("uri").value.replace("#",
					"123nada");
			window.location.assign("Find?uri1=" + url);
		}
		function findURI() {
			document.getElementById('loadingDiv').style.display = "block";
			document.getElementById('divhider').style.display = "block";
			var url = document.getElementById("uri").value.replace("#",
					"123nada");
			window.location.assign("Find?endpoint=" + url);
		}
		function findSQL() {
			document.getElementById('loadingDiv').style.display = "block";
			document.getElementById('divhider').style.display = "block";
			var url = document.getElementById("uri").value.replace("#",
					"123nada");
			window.location.assign("Find?SQL=" + url);
		}
	</script>
</body>
</html>