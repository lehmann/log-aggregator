package br.lehmann.aggregator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

/**
 * Descritor do servidor
 * 
 * @author André Lehmann
 * 
 */
public class Server {

	private File folder;
	private int fileIndex;
	private String name;

	public Server(String folder) {
		name = folder;
		this.folder = new File(folder);
	}

	public InputStream nextLogFile() {
		try {
			File file = new File(folder, ++fileIndex + ".txt");
			if (!file.exists()) {
				return null;
			}
			return new FileInputStream(file);
		} catch (FileNotFoundException e) {
			throw new RuntimeException();
		}
	}

	public File getDestinFolder() {
		File file = new File("./destin");
		file.mkdirs();
		return file;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Server other = (Server) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}

}
