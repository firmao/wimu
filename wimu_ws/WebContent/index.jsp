<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>

<%
// if((session.getAttribute("dirHDT") == null) 
// 		|| (session.getAttribute("dirEndpoints") == null) 
// 		|| (session.getAttribute("dirDumps") == null)){
// 	response.sendRedirect("conf.jsp");
// 	//out.close();
// }
if(System.getProperty("dirInd") == null){
	response.sendRedirect("conf.jsp");
	//out.close();
}
%>

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
        top: 60%;
        left: 32%;
        width:50em;
        height:8em;
        margin-top: -5em; /*set to a negative number 1/2 of your height*/
        margin-left: -5em; /*set to a negative number 1/2 of your width*/
        border: 0px solid #ccc;
        border:  0px solid black;
        z-index:100; 
        background-color: white;
        opacity:0.1;
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
	<input type="text" id="top" size="5" value="10" alt="OPTIONAL: Top datasets">
	<button onclick="findEndPoint()">Find Datasets</button>
<!-- 	<button onclick="getStatistics()">Statistics</button> -->
<!-- 	<button onclick="uploadHDT()">Upload HDT dataset</button> -->
	<br>Examples: 
	<br>URI: http://dbpedia.org/resource/Leipzig
	<br>URI: http://citeseer.rkbexplorer.com/id/resource-CS116606
<center>	
<table dir="ltr" border="1" cellspacing="0" cellpadding="0"><colgroup><col width="132" /><col width="133" /><col width="136" /></colgroup>
<tbody>
<tr>
<td><strong>&nbsp;</strong></td>
<td data-sheets-value="{&quot;1&quot;:2,&quot;2&quot;:&quot;LODLaundromat&quot;}"><strong>LODLaundromat</strong></td>
<td data-sheets-value="{&quot;1&quot;:2,&quot;2&quot;:&quot;LODStats&quot;}"><strong>LODStats</strong></td>
</tr>
<tr>
<td data-sheets-value="{&quot;1&quot;:2,&quot;2&quot;:&quot;URIs processed&quot;}"><strong>URIs processed</strong></td>
<td data-sheets-value="{&quot;1&quot;:3,&quot;3&quot;:4185133445}" data-sheets-numberformat="{&quot;1&quot;:2,&quot;2&quot;:&quot;#,##0&quot;,&quot;3&quot;:1}" data-sheets-formula="=(SUM(R[-1]C[-3]:R[2]C[-3]))">4,185,133,445</td>
<td data-sheets-value="{&quot;1&quot;:3,&quot;3&quot;:31121342}" data-sheets-numberformat="{&quot;1&quot;:2,&quot;2&quot;:&quot;#,##0&quot;,&quot;3&quot;:1}" data-sheets-formula="=SUM(R[3]C[-4]:R[4]C[-4])">31,121,342</td>
</tr>
<tr>
<td data-sheets-value="{&quot;1&quot;:2,&quot;2&quot;:&quot;Datasets&quot;}"><strong>Datasets</strong></td>
<td data-sheets-value="{&quot;1&quot;:3,&quot;3&quot;:658206}" data-sheets-numberformat="{&quot;1&quot;:2,&quot;2&quot;:&quot;#,##0&quot;,&quot;3&quot;:1}">658,206</td>
<td data-sheets-value="{&quot;1&quot;:3,&quot;3&quot;:9960}" data-sheets-numberformat="{&quot;1&quot;:2,&quot;2&quot;:&quot;#,##0&quot;,&quot;3&quot;:1}">9,960</td>
</tr>
<tr>
<td data-sheets-value="{&quot;1&quot;:2,&quot;2&quot;:&quot;Triples&quot;}"><strong>Triples</strong></td>
<td data-sheets-value="{&quot;1&quot;:3,&quot;3&quot;:38606408854}" data-sheets-numberformat="{&quot;1&quot;:2,&quot;2&quot;:&quot;#,##0&quot;,&quot;3&quot;:1}">38,606,408,854</td>
<td data-sheets-value="{&quot;1&quot;:3,&quot;3&quot;:149423660620}" data-sheets-numberformat="{&quot;1&quot;:2,&quot;2&quot;:&quot;#,##0&quot;,&quot;3&quot;:1}">149,423,660,620</td>
</tr>
</tbody>
</table>
<br>
<br>Example of API service usage:
<br>- URI:
<br>https://w3id.org/where-is-my-uri/Find?uri=http://dbpedia.org/resource/Leipzig
<br>
<br>Many URIs (An example of a linkset file from http://linklion.org):
<br>https://w3id.org/where-is-my-uri/Find?link=http://www.linklion.org/download/mapping/citeseer.rkbexplorer.com---ibm.rkbexplorer.com.nt
	</center>
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
			var top = document.getElementById("top").value;
			//window.location.assign("Find?uri1=" + url);
			window.location.assign("Find?top="+top+"&urihdt=" + url);
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
		function uploadHDT() {
			document.getElementById('loadingDiv').style.display = "block";
			document.getElementById('divhider').style.display = "block";
			window.location.assign('uploadHDT.jsp');
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