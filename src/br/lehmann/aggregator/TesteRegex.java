package br.lehmann.aggregator;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TesteRegex {

	private static final Pattern LOG_PATTERN = Pattern.compile(".*--\\[(.*)\\].*\"userid=(.*)\"");

	public static void main(String[] args) {
		Matcher matcher = LOG_PATTERN.matcher("1515151--[4564564564]564564564\"userid=321231231231\"");
		if (matcher.find()) {
			System.out.println("Uhuuuuul");
			System.out.println(matcher.group(1));
			System.out.println(matcher.group(2));
		} else {
			System.out.println("Sad :(");
		}
	}
}
