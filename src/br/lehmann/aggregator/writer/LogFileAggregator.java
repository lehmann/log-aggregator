package br.lehmann.aggregator.writer;

import java.util.Iterator;
import java.util.Set;

import br.lehmann.aggregator.Server;

import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;

/**
 * Agrega todas as mensagem de log por usuário de um arquivo de log, para
 * otimizar a escrita no arquivo de log quando as mensagens forem postadas no
 * barramento.
 * 
 * @author André Lehmann
 * 
 */
public class LogFileAggregator {

	private Writer writer;
	// chave baseada em hash e valores armazenados em lista encadeada, para
	// manter a ordem de inserção
	private Multimap<String, String> entries = MultimapBuilder.hashKeys().linkedListValues().build();
	private Server server;

	public LogFileAggregator(Writer aggregator, Server server) {
		this.writer = aggregator;
		this.server = server;
	}

	public void addEntry(String userId, String logLine) {
		entries.put(userId, logLine);
	}

	public void flush() {
		Set<String> keySet = entries.keySet();
		for (Iterator iterator = keySet.iterator(); iterator.hasNext();) {
			String userId = (String) iterator.next();
			if (entries.containsKey(userId)) {
				writer.write(userId, server, entries.get(userId));
			}
		}
		entries.clear();
	}
}
