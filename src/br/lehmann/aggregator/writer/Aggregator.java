package br.lehmann.aggregator.writer;

import java.io.File;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import br.lehmann.aggregator.Server;

public class Aggregator {
	
	ConcurrentHashMap<String, UserWriter> exclusivities = new ConcurrentHashMap<>();
	ExecutorService poolExecutor = Executors.newCachedThreadPool();

	public WriteLogPipeline pipeline(Server server) {
		return new WriteLogPipeline(this, server);
	}

	public void write(String userId, Server server, Collection<String> collection) {
		UserWriter writer = new UserWriter();
		if(exclusivities.putIfAbsent(userId, writer) == null) {
			writer.setDestin(new File(server.getDestinFolder(), userId).toPath());
			poolExecutor.submit(writer);
		} else {
			writer = exclusivities.get(userId);
		}
		writer.send(collection);
	}

}
