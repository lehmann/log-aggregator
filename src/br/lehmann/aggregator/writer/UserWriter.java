package br.lehmann.aggregator.writer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.concurrent.LinkedTransferQueue;

public class UserWriter implements Runnable {

	private Path destin;
	// fila para transferência entre a Thread de leitura e escrita
	private LinkedTransferQueue<String> unwrited;

	public UserWriter() {
		this.unwrited = new LinkedTransferQueue<>();
	}

	public void setDestin(Path destin) {
		this.destin = destin;
	}

	public void send(Collection<String> collection) {
		unwrited.addAll(collection);
	}

	public void run() {
		while (true) {
			try {
				String logLine = unwrited.take();
				try {
					write(logLine);
				} catch (Exception e) {
					e.printStackTrace();
				}
			} catch (InterruptedException e1) {
				//
				e1.printStackTrace();
			}
		}
	}

	private void write(String logLine) throws Exception {
		FileChannel channel = FileChannel.open(destin,
				StandardOpenOption.WRITE, StandardOpenOption.CREATE,
				StandardOpenOption.APPEND);
		channel.write(ByteBuffer.wrap(logLine.getBytes()));
	}
}
