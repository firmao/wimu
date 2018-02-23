

public class WIMUri {
	String datasetS, uriS, hdtS, datasetT, uriT, hdtT;
	int cDatatypesS, cDatatypesT;
	public WIMUri(String pUriS, String pUriT) {
		setUriS(pUriS);
		setUriT(pUriT);
	}
	public String getDatasetS() {
		return datasetS;
	}
	public void setDatasetS(String datasetS) {
		this.datasetS = datasetS;
	}
	public String getUriS() {
		return uriS;
	}
	public void setUriS(String uriS) {
		this.uriS = uriS;
	}
	public String getHdtS() {
		return hdtS;
	}
	public void setHdtS(String hdtS) {
		this.hdtS = hdtS;
	}
	public String getDatasetT() {
		return datasetT;
	}
	public void setDatasetT(String datasetT) {
		this.datasetT = datasetT;
	}
	public String getUriT() {
		return uriT;
	}
	public void setUriT(String uriT) {
		this.uriT = uriT;
	}
	public String getHdtT() {
		return hdtT;
	}
	public void setHdtT(String hdtT) {
		this.hdtT = hdtT;
	}
	public int getcDatatypesS() {
		return cDatatypesS;
	}
	public void setcDatatypesS(int cDatatypesS) {
		this.cDatatypesS = cDatatypesS;
	}
	public int getcDatatypesT() {
		return cDatatypesT;
	}
	public void setcDatatypesT(int cDatatypesT) {
		this.cDatatypesT = cDatatypesT;
	}
	@Override
	public String toString() {
		return "Source(" + getUriS() + "," + ((getDatasetS() != null) ? getDatasetS() : getHdtS())  + "),Target(" + getUriT() + "," + ((getDatasetT() != null) ? getDatasetT() : getHdtT()) + ")";
	}
}
