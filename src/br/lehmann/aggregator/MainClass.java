package br.lehmann.aggregator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import br.lehmann.aggregator.writer.Writer;

/**
 * Classe principal, que contém as configurações de todos os servidores.
 * 
 * @author André Lehmann
 * 
 */
public class MainClass {

	public static void main(String[] args) {
		Server[] servers = new Server[] { new Server("./teste-1"), new Server("./teste-2") };
		Writer aggregator = new Writer();
		// crio uma Thread para cada 'servidor' ser processado paralelamente +
		// uma Thread para finalizar os consumidores de log
		ExecutorService serverExecutor = Executors.newFixedThreadPool(servers.length + 1);
		for (int i = 0; i < servers.length; i++) {
			ServerLogProcessor task = new ServerLogProcessor(servers[i], aggregator);
			serverExecutor.submit(task);
		}
		serverExecutor.submit(aggregator.watchDog());
		serverExecutor.shutdown();
	}
}
