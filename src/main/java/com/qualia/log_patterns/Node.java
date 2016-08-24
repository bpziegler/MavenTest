package com.qualia.log_patterns;


import java.util.ArrayList;
import java.util.List;


public class Node {
    private static final int MIN_LINES_TO_SPLIT = 10;

    // Branch Fields
    public String splitWord;
    public Node presentSet;
    public Node notPresentSet;
    public Node parent;

    // Leaf Fields
    public List<String> lines = new ArrayList<String>();


    public void checkSplitNode() {
        if (lines.size() < MIN_LINES_TO_SPLIT) {
            return;
        }

        LogLineSplitterResult splitResult = LogLineSplitter.findBestSplit(lines);

        if (splitResult.word != null) {
            splitNode(splitResult);
        }
    }


    public void splitNode(LogLineSplitterResult splitResult) {
        splitWord = splitResult.word;

        presentSet = new Node();
        presentSet.parent = this;
        notPresentSet = new Node();
        notPresentSet.parent = this;

        for (String line : lines) {
            List<String> words = LogLineSplitter.SPLITTER.splitToList(line);
            boolean isPresent = words.contains(splitWord);
            if (isPresent) {
                presentSet.lines.add(line);
            } else {
                notPresentSet.lines.add(line);
            }
        }

        boolean isValidSplit = presentSet.lines.size() >= MIN_LINES_TO_SPLIT && notPresentSet.lines.size() >= MIN_LINES_TO_SPLIT;

        if (isValidSplit) {
            // Check recursively if we can split more
            lines.clear();
            presentSet.checkSplitNode();
            notPresentSet.checkSplitNode();
        } else {
            // This split wasn't valid, roll it back
            splitWord = null;
            if (lines.size() > 1000) {
                System.out.println("Unable to split with word:  " + splitResult.word + "   lines = " + lines.size());
            }
            presentSet = null;
            notPresentSet = null;
        }
    }


    public void visitNodes(IVisitCallback callback) {
        if (presentSet != null) {
            presentSet.visitNodes(callback);
        }
        callback.visitNode(this);
        if (notPresentSet != null) {
            notPresentSet.visitNodes(callback);
        }
    }


    public boolean isLeafNode() {
        return lines.size() > 0;
    }


    public int numLines() {
        return lines.size();
    }


    public String conditionString() {
        StringBuilder sb = new StringBuilder();

        getRecurConditionString(sb);

        return sb.toString();
    }


    private void getRecurConditionString(StringBuilder sb) {
        if (parent != null) {
            if (parent.notPresentSet == this) {
                sb.append("!");
            }
            sb.append(parent.splitWord);
            sb.append(" ");
            parent.getRecurConditionString(sb);
        }
    }


    @Override
    public String toString() {
        return "Node [splitWord=" + splitWord + ", numLines()=" + numLines() + ", conditionString()="
                + conditionString() + "]";
    }
}
