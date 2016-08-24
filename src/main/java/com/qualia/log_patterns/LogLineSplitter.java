package com.qualia.log_patterns;


import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;


public class LogLineSplitter {

    public static CharMatcher CHAR_NUM_MATCHER = CharMatcher.JAVA_LETTER.or(CharMatcher.DIGIT)
            .or(CharMatcher.anyOf("/"));
    public static CharMatcher HEX_MATCHER = CharMatcher.DIGIT.or(CharMatcher.anyOf("abcdefABCDEF"));
    public static Splitter SPLITTER = Splitter.on(CHAR_NUM_MATCHER.negate());


    public static LogLineSplitterResult findBestSplit(List<String> lines) {

        if (lines.size() > 20000) {
            System.out.println("Splitting lines into words.  Lines = " + lines.size());
        }

        Map<String, Integer> wordCounts = new HashMap<String, Integer>();
        int num = 0;
        for (String line : lines) {
            List<String> words = SPLITTER.splitToList(line);
            Set<String> uniqueWords = new HashSet<String>(words);

            for (String word : uniqueWords) {
                if (CharMatcher.DIGIT.matchesAnyOf(word)) {
                    // Don't allow using a number as a split word, or any word with numbers in it
                    continue;
                }
                if (HEX_MATCHER.matchesAllOf(word)) {
                    // Don't allow using a hex number as a split word
                    continue;
                }
                if (word.contains("/")) {
                    // Don't allow using a path (a/b/c) as a split word
                    continue;
                }
                if (word.length() == 0) {
                    continue;
                }
                Integer count = wordCounts.get(word);
                if (count == null) {
                    count = 0;
                }
                wordCounts.put(word, count + 1);
            }

            num++;
            if (num % 10000 == 0) {
                // System.out.println("Curline " + num + " Max lines " + lines.size());
            }
        }

        // Find best "split" word
        int size = lines.size();
        double bestDiff = Double.MAX_VALUE;
        String bestWord = null;
        for (String word : wordCounts.keySet()) {
            Integer count = wordCounts.get(word);
            double percent = (count + 0.0) / size;
            double diff = Math.abs(0.5 - percent);
            if (diff < bestDiff) {
                bestDiff = diff;
                bestWord = word;
            }
        }
        System.out.println(String.format("Lines = %,8d   # Unique Words = %,6d   Best Word = %-20s   Score = %5.3f",
                lines.size(), wordCounts.size(), bestWord, bestDiff));

        LogLineSplitterResult result = new LogLineSplitterResult();
        result.word = bestWord;
        result.score = bestDiff;

        return result;
    }
}
