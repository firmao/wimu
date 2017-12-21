<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Upload HDT files</title>
</head>
<body>
    <form action="UploadHDTfile" method="post" enctype="multipart/form-data">
        <ul>
            <h3>File Upload:</h3>
            Select a file to upload: <br>
            <input type="file" name="file" size="50" accept=".hdt"  multiple> <br>
            <br>
            Metadata about the dataset (Example: URL, URI, location, name, etc...):
            <br>
            <input type="text" id="txtMetadata" />
            <br>
            <input type="submit" value="Upload File" />
        </ul>
    </form>
</body>
</html>