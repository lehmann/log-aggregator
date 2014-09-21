package br.lehmann.aggregator.writer;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.concurrent.LinkedTransferQueue;

/**
 * Endpoint do barramento de mensagens, responsável por armazenar todas as
 * entradas de log previamente processadas
 * 
 * @author André Lehmann
 * 
 */
public class UserWriter implements Runnable {

	private static final byte[] NEW_LINE = System.getProperty("line.separator").getBytes();

	private Path destination;
	// fila para transferência entre a Thread de leitura e escrita
	private LinkedTransferQueue<String> unwrited;

	private static final String FINISHED = "FINISHED";

	public UserWriter() {
		this.unwrited = new LinkedTransferQueue<>();
	}

	public void setDestination(Path destin) {
		this.destination = destin;
	}

	public void send(Collection<String> collection) {
		unwrited.addAll(collection);
	}

	public void run() {
		while (true) {
			try {
				String logLine = unwrited.take();
				if (logLine == FINISHED) {
					break;
				}
				try {
					write(logLine);
				} catch (Exception e) {
					// se houver qualquer erro na escrita de uma linha do log,
					// registra na saída padrão e continua o processamento
					e.printStackTrace();
				}
			} catch (InterruptedException e1) {
				//
				e1.printStackTrace();
			}
		}
	}

	private void write(String logLine) throws Exception {
		FileChannel channel = FileChannel.open(destination, StandardOpenOption.WRITE, StandardOpenOption.CREATE,
				StandardOpenOption.APPEND);
		byte[] bytes = logLine.getBytes();
		ByteBuffer buffer = ByteBuffer.allocate(bytes.length + NEW_LINE.length);
		buffer.put(bytes);
		buffer.put(NEW_LINE);
		buffer.flip();
		channel.write(buffer);
		channel.close();
	}

	public void finish() {
		unwrited.add(FINISHED);
	}
}
