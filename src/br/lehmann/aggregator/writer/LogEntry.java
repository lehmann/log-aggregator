package br.lehmann.aggregator.writer;

class LogEntry {

	String date;
	String userId;
	String logLine;

	LogEntry(String date, String userId, String logLine) {
		this.date = date;
		this.userId = userId;
		this.logLine = logLine;
	}
	
}