package br.lehmann.aggregator.writer;

import java.util.Iterator;
import java.util.Set;

import br.lehmann.aggregator.Server;

import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;

public class WriteLogPipeline {

	private Aggregator aggregator;
	// chave baseada em hash e valores armazenados em lista encadeada
	private Multimap<String, String> entries = MultimapBuilder.hashKeys()
			.linkedListValues().build();
	private Server server;

	public WriteLogPipeline(Aggregator aggregator, Server server) {
		this.aggregator = aggregator;
		this.server = server;
	}

	public void addEntry(String userId, String logLine) {
		entries.put(userId, logLine);
	}

	public void aggregate() {
		Set<String> keySet = entries.keySet();
		for (Iterator iterator = keySet.iterator(); iterator.hasNext();) {
			String userId = (String) iterator.next();
			aggregator.write(userId, server, entries.get(userId));
		}
		entries.clear();
	}

}
