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
<a href="https://github.com/firmao/wimu"><img style="position: absolute; top: 0; left: 0; border: 0;" src="https://camo.githubusercontent.com/82b228a3648bf44fc1163ef44c62fcc60081495e/68747470733a2f2f73332e616d617a6f6e6177732e636f6d2f6769746875622f726962626f6e732f666f726b6d655f6c6566745f7265645f6161303030302e706e67" alt="Fork me on GitHub" data-canonical-src="https://s3.amazonaws.com/github/ribbons/forkme_left_red_aa0000.png"></a>
	<h1>Where is my URI?</h1>
	<input type="text" id="uri" size="80">
	<button onclick="findEndPoint()">Find Datasets</button>
	<button onclick="getStatistics()">Statistics</button>
	<button onclick="findSQL()">SQL query</button>
	<br>Examples: 
	<br>URI: http://dbpedia.org/resource/Leipzig
	
<table style="margin-left: auto; margin-right: auto;" border="1">
<tbody>
<tr>
<td><strong>Dataset</strong></td>
<td><strong>Triples</strong></td>
<td><strong>Datatypes/Literals</strong></td>
<td><strong>Type</strong></td>
</tr>



</tbody>
</table>
	
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
			var url = document.getElementById("uri").value;
			url=encodeURIComponent(url);
			//window.location.assign("Find?uri1=" + url);
			window.location.assign("Find?urihdt=" + url);
		}
		function findURI() {
			document.getElementById('loadingDiv').style.display = "block";
			document.getElementById('divhider').style.display = "block";
			var url = document.getElementById("uri").value;
			url=encodeURIComponent(url);
			window.location.assign("Find?endpoint=" + url);
		}
		function findSQL() {
			document.getElementById('loadingDiv').style.display = "block";
			document.getElementById('divhider').style.display = "block";
			var url = document.getElementById("uri").value;
			url=encodeURIComponent(url);
			window.location.assign("Find?SQL=" + url);
		}
		function getStatistics() {
			document.getElementById('loadingDiv').style.display = "block";
			document.getElementById('divhider').style.display = "block";
			var url = document.getElementById("uri").value;
			url=encodeURIComponent(url);
			window.location.assign("Find?statistics=" + url);
		}
		
	</script>
</body>
</html>