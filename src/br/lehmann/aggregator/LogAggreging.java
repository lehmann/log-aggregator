package br.lehmann.aggregator;

import java.io.InputStream;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import br.lehmann.aggregator.writer.Aggregator;
import br.lehmann.aggregator.writer.WriteLogPipeline;

public class LogAggreging implements Runnable {

	private Server server;
	private WriteLogPipeline pipeline;
	private static final Pattern LOG_PATTERN = Pattern.compile(".*--\\[(.*)\\].*\"userid=(.*)\"");

	public LogAggreging(Server server, WriteLogPipeline pipeline) {
		this.server = server;
		this.pipeline = pipeline;
	}

	public static void main(String[] args) {
		Server[] servers = null;
		Aggregator aggregator = null;
		ExecutorService poolExecutor = Executors.newCachedThreadPool();
		for (int i = 0; i < servers.length; i++) {
			poolExecutor.submit(new LogAggreging(servers[i], aggregator.pipeline(servers[i].getDestinFolder())));
		}
	}

	@Override
	public void run() {
		InputStream logFile = null;
		while((logFile = server.nextLogFile()) != null) {
			try (Scanner scan = new Scanner(logFile)) {
				String logLine = scan.nextLine();
				Matcher matcher = LOG_PATTERN.matcher(logLine);
				if(matcher.find()) {
					pipeline.addEntry(matcher.group(1), matcher.group(2), logLine);
				}
			}
		}
	}
}
