package br.lehmann.aggregator;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class TesteChannel {

	public static void main(String[] args) throws IOException {
		File file = new File("./testinho.txt");
		file.delete();
		file.createNewFile();
		FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);

		ByteBuffer buffer = ByteBuffer.wrap("Meu teste lindo\nContinuando a conversa".getBytes());
		channel.write(buffer);
		channel.close();

		insert(file.toPath(), "Meu teste lindo\n".getBytes().length, ("Hackeado" + System.getProperty("line.separator")).getBytes());
	}

	public static void insert(Path destinPath, long offset, byte[] content) throws IOException {
		RandomAccessFile r = new RandomAccessFile(destinPath.toFile(), "rw");
		File tempFile = new File(destinPath.toFile().getAbsolutePath() + "~");
		RandomAccessFile rtemp = new RandomAccessFile(tempFile, "rw");
		long fileSize = r.length();
		FileChannel sourceChannel = r.getChannel();
		FileChannel targetChannel = rtemp.getChannel();
		sourceChannel.transferTo(offset, (fileSize - offset), targetChannel);
		sourceChannel.truncate(offset);
		r.seek(offset);
		r.write(content);
		long newOffset = r.getFilePointer();
		targetChannel.position(0L);
		sourceChannel.transferFrom(targetChannel, newOffset, (fileSize - offset));
		sourceChannel.close();
		targetChannel.close();
		r.close();
		rtemp.close();
		tempFile.delete();
	}
}
