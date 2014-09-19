package br.lehmann.aggregator.writer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.LinkedTransferQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;

public class WriteLogPipeline {

	private Aggregator aggregator;
	// chave baseada em hash e valores armazenados em lista encadeada
	private Multimap<String, LogEntry> entries = MultimapBuilder.hashKeys().linkedListValues().build();
	// fila para transferência entre a Thread de leitura e escrita
	private LinkedTransferQueue<LogEntry> unwrited = new LinkedTransferQueue<>();
	private File destinFolder;

	private static final Pattern TIME_LOG_PATTERN = Pattern.compile(".*\\[(.*)\\].*");
	private static final SimpleDateFormat FULL_DATE_FORMAT = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss z");

	public WriteLogPipeline(Aggregator aggregator, File destinFolder) {
		this.aggregator = aggregator;
		this.destinFolder = destinFolder;
	}

	public void addEntry(String date, String userId, String logLine) {
		entries.put(userId, new LogEntry(date, userId, logLine));
	}

	public void aggregate() {
		Set<String> keySet = entries.keySet();
		for (Iterator iterator = keySet.iterator(); iterator.hasNext();) {
			String userId = (String) iterator.next();
			if (aggregator.exclusivity(userId, this)) {
				pleaseWrite(entries.get(userId));
			} else {
				aggregator.write(userId, entries.get(userId));
			}
		}
		entries.clear();
	}

	void pleaseWrite(Collection<LogEntry> collection) {
		unwrited.addAll(collection);
	}

	void flush() throws InterruptedException, IOException {
		while (true) {
			LogEntry entry = unwrited.take();
			File destinFile = new File(destinFolder, entry.userId);
			if (!destinFile.exists()) {
				destinFile.createNewFile();
			}
			writeLogEntryOrdered(destinFile.toPath(), entry);
		}
	}

	private void writeLogEntryOrdered(Path destinPath, LogEntry entry) throws IOException {
		FileChannel channel = FileChannel.open(destinPath, StandardOpenOption.READ, StandardOpenOption.WRITE);
		String line = null;
		try(Scanner scan = new Scanner(channel)) {
			long previousPosition = channel.position();
			while((line = scan.nextLine()) != null) {
				if("".equals(line)) {
					channel.write(ByteBuffer.wrap(entry.logLine.getBytes()));
					return;
				}
				Matcher matcher = TIME_LOG_PATTERN.matcher(line);
				if (matcher.find()) {
					try {
						Date timeLog = FULL_DATE_FORMAT.parse(matcher.group(1));
						if (timeLog.compareTo(FULL_DATE_FORMAT.parse(entry.date)) < 0) {
							//encontrei o ponto onde devo escrever o novo log
							insert(destinPath, previousPosition, entry.logLine.getBytes());
							return;
						}
					} catch (ParseException e) {
						// ignora a entrada do log e continua a vida
					}
				}
				previousPosition = channel.position();
			}
		}
	}

	public void insert(Path destinPath, long offset, byte[] content) throws IOException {
		RandomAccessFile r = new RandomAccessFile(destinPath.toFile(), "rw");
		RandomAccessFile rtemp = new RandomAccessFile(new File(destinPath.toFile().getAbsolutePath() + "~"), "rw");
		long fileSize = r.length();
		FileChannel sourceChannel = r.getChannel();
		FileChannel targetChannel = rtemp.getChannel();
		sourceChannel.transferTo(offset, (fileSize - offset), targetChannel);
		sourceChannel.truncate(offset);
		r.seek(offset);
		r.write(content);
		r.writeChar(File.separatorChar);
		long newOffset = r.getFilePointer();
		targetChannel.position(0L);
		sourceChannel.transferFrom(targetChannel, newOffset, (fileSize - offset));
		sourceChannel.close();
		targetChannel.close();
		r.close();
		rtemp.close();
	}
}
