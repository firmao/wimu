import java.io.File;

public class FileWIMU extends File {

	private String dataset;

	public String getDataset() {
		return dataset;
	}

	public void setDataset(String pDataset) {
		this.dataset = pDataset;
	}

	public FileWIMU(String fName) {
		super(fName);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

}
