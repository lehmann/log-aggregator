package br.lehmann.aggregator;

import java.io.InputStream;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import br.lehmann.aggregator.writer.LogFileAggregator;

/**
 * Realiza o processamento de um arquivo de log e posta para o barramento de
 * mensagens
 * 
 * @author André Lehmann
 * 
 */
public class LogFileProcessor implements Runnable {

	private LogFileAggregator aggregator;
	private InputStream logFile;
	private static final Pattern LOG_PATTERN = Pattern.compile(".*\"userid=(.*)\"");
	private int processorId;
	private ServerLogProcessor server;

	public LogFileProcessor(int processorId, InputStream logFile, LogFileAggregator pipeline, ServerLogProcessor server) {
		this.processorId = processorId;
		this.logFile = logFile;
		this.aggregator = pipeline;
		this.server = server;
	}

	@Override
	public void run() {
		try {
			try (Scanner scan = new Scanner(logFile)) {
				while (scan.hasNextLine()) {
					String logLine = scan.nextLine();
					Matcher matcher = LOG_PATTERN.matcher(logLine);
					if (matcher.find()) {
						aggregator.addEntry(matcher.group(1), logLine);
					}
				}
			}
			aggregator.flush();
		} catch (Exception e) {
			e.printStackTrace();
		}
		server.finish(this);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + processorId;
		result = prime * result + ((server == null) ? 0 : server.hashCode());
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
		LogFileProcessor other = (LogFileProcessor) obj;
		if (processorId != other.processorId)
			return false;
		if (server == null) {
			if (other.server != null)
				return false;
		} else if (!server.equals(other.server))
			return false;
		return true;
	}

}
