package br.lehmann.aggregator;

import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import br.lehmann.aggregator.writer.Writer;

/**
 * Responsável por delegar o processamento de cada arquivo por servidor de log.
 * @author André Lehmann
 *
 */
public class ServerLogProcessor implements Runnable {

	private Server server;
	private Writer aggregator;

	public ServerLogProcessor(Server server, Writer pipeline) {
		this.server = server;
		this.aggregator = pipeline;
	}

	@Override
	public void run() {
		InputStream logFile = null;
		List<LogFileProcessor> processors = new LinkedList<>();
		for(int i = 0; (logFile = server.nextLogFile()) != null; i++) {
			LogFileProcessor fileProcessor = new LogFileProcessor(i, logFile, aggregator
					.pipeline(server), this);
			aggregator.putProcessor(fileProcessor);
			processors.add(fileProcessor);
		}
		//aloca uma Thread por núcleo do processador para otimizar o uso de CPU
		ExecutorService logFileExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		for (LogFileProcessor fileProcessor : processors) {
			//para cada arquivo, posto o processamento em um executor
			logFileExecutor.submit(fileProcessor);
		}
		logFileExecutor.shutdown();
	}
	
	public void finish(LogFileProcessor processor) {
		aggregator.removeProcessor(processor);
	}
}
