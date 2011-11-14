package util;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringTools {

	public static ArrayList<String> split(String content) {
		return split(content, "\\B+");
	}

	public static ArrayList<String> split(String content, String pattern) {
		Matcher m = Pattern.compile(pattern).matcher(content);
		ArrayList<String> words = new ArrayList<String>();
		while (m.find()) {
			words.add(m.group());
		}
		return words;
	}
}
