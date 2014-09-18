package br.lehmann.aggregator.writer;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;

public class WriteLogPipeline {

	private Aggregator aggregator;
	private Multimap<String, LogEntry> entries = MultimapBuilder.hashKeys().hashSetValues().build();
	private LinkedList<LogEntry> unwrited = new LinkedList<>();
	private File destinFolder;
	
	private static final Pattern TIME_LOG_PATTERN = Pattern.compile(".*\\[(.*)\\].*");

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
			if(aggregator.exclusivity(userId, this)) {
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
	
	void flush() {
		for (Iterator iterator = unwrited.iterator(); iterator.hasNext();) {
			LogEntry entry = (LogEntry) iterator.next();
			File destinFile = new File(destinFolder, entry.userId);
			if(!destinFile.exists()) {
				destinFile.createNewFile();
			}
			writeLogEntryOrdered(destinFile, entry);
		}
	}

	private void writeLogEntryOrdered(File destinFile, LogEntry entry) throws FileNotFoundException {
		String line = null;
		try (Scanner scan = new Scanner(destinFile)){
			while((line = scan.nextLine()) != null) {
				Matcher matcher = TIME_LOG_PATTERN.matcher(line);
				if(matcher.find()) {
					String timeLog = matcher.group(1);
					if(timeLog.compareTo(entry.date) < 0) {
						//TODO escrever neste ponto do arquivo de log
					}
				}
			}
		}
	}
}
