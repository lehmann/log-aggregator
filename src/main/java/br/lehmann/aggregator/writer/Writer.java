package br.lehmann.aggregator.writer;

import java.io.File;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import br.lehmann.aggregator.LogFileProcessor;
import br.lehmann.aggregator.Server;

/**
 * Objeto distribuído que conhece todos os {@link UserWriter writers} existentes
 * e garante que somente um arquivo é gerado por usuário
 * 
 * @author André Lehmann
 * 
 */
public class Writer {

	private ConcurrentHashMap<String, UserWriter> exclusivities = new ConcurrentHashMap<>();
	private ExecutorService poolExecutor = Executors.newCachedThreadPool();
	private Set<LogFileProcessor> processors = new LinkedHashSet<>();
	private boolean started = false;

	public LogFileAggregator pipeline(Server server) {
		return new LogFileAggregator(this, server);
	}

	public void putProcessor(LogFileProcessor task) {
		this.processors.add(task);
		started = true;
	}

	public void removeProcessor(LogFileProcessor processor) {
		processors.remove(processor);
	}

	public void write(String userId, Server server, Collection<String> collection) {
		UserWriter writer = new UserWriter();
		if (exclusivities.putIfAbsent(userId, writer) == null) {
			//para cada usuário distinto é criado um consumidor de mensagens no barramento
			writer.setDestination(new File(server.getDestinFolder(), userId).toPath());
			poolExecutor.submit(writer);
		} else {
			writer = exclusivities.get(userId);
		}
		writer.send(collection);
	}

	public void finish() {
		for (UserWriter writer : exclusivities.values()) {
			writer.finish();
		}
		poolExecutor.shutdown();
	}

	public Runnable watchDog() {
		// Thread que é a responsável por finalizar todos os escritores de
		// arquivos por usuário
		return new Runnable() {

			@Override
			public void run() {
				while (!started || !processors.isEmpty()) {
					try {
						Thread.sleep(50);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				finish();
			}
		};
	}

}
