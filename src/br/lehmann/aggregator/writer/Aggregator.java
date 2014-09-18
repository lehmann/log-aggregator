package br.lehmann.aggregator.writer;

import java.io.File;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Aggregator {
	
	Map<String, WriteLogPipeline> exclusivities = new ConcurrentHashMap<>();

	public WriteLogPipeline pipeline(File destinFolder) {
		return new WriteLogPipeline(this, destinFolder);
	}

	public boolean exclusivity(String userId, WriteLogPipeline applicantOwner) {
		return exclusivities.putIfAbsent(userId, applicantOwner) == null;
	}

	public void write(String userId, Collection<LogEntry> collection) {
		WriteLogPipeline pipeline = exclusivities.get(userId);
		assert pipeline != null;
		pipeline.pleaseWrite(collection);
	}

}
