package br.lehmann.aggregator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

public class Server {

	private File folder;
	private int fileIndex;

	public Server(String folder) {
		this.folder = new File(folder);
	}

	public InputStream nextLogFile() {
		try {
			File file = new File(folder, ++fileIndex + ".txt");
			if(!file.exists()) {
				return null;
			}
			return new FileInputStream(file);
		} catch (FileNotFoundException e) {
			throw new RuntimeException();
		}
	}

	public File getDestinFolder() {
		return new File("./destin");
	}

}
