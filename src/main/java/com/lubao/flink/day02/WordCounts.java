package com.lubao.flink.day02;


import java.io.Serializable;

public class WordCounts implements Serializable {

    private String word;

    private Long counts;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Long getCounts() {
        return counts;
    }

    public void setCount(Long counts) {
        this.counts = counts;
    }

    public WordCounts() {
    }

    public WordCounts(String word, Long counts) {
        this.word = word;
        this.counts = counts;
    }

    public static WordCounts of(String word, Long counts) {
        return new WordCounts(word,counts);
    }
    @Override
    public String toString() {
        return "WordCounts{" +
                "word='" + word + '\'' +
                ", counts=" + counts +
                '}';
    }
}
