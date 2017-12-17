<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Datasets</title>
<script src="https://www.w3schools.com/lib/w3.js"></script>
</head>
<body>
<table id="myTable">
  <tr>
    <th onclick="w3.sortHTML('#myTable', '.item', 'td:nth-child(1)')" style="cursor:pointer">Dataset</th>
    <th onclick="w3.sortHTML('#myTable', '.item', 'td:nth-child(2)')" style="cursor:pointer">Triples</th>
  </tr>
  <tr class="item">
    <td>Berglunds snabbkop</td>
    <td>Sweden</td>
  </tr>
  <tr class="item">
    <td>North/South</td>
    <td>UK</td>
  </tr>
  <tr class="item">
    <td>Alfreds Futterkiste</td>
    <td>Germany</td>
  </tr>
  <tr class="item">
    <td>Koniglich Essen</td>
    <td>Germany</td>
  </tr>
  <tr class="item">
    <td>Magazzini Alimentari Riuniti</td>
    <td>Italy</td>
  </tr>
  <tr class="item">
    <td>Paris specialites</td>
    <td>France</td>
  </tr>
  <tr class="item">
    <td>Island Trading</td>
    <td>UK</td>
  </tr>
  <tr class="item">
    <td>Laughing Bacchus Winecellars</td>
    <td>Canada</td>
  </tr>
</table>
</body>
</html>