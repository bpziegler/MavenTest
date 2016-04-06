package com.qualia.keystore_graph;


import java.util.Random;


public class ProbabilityTest {

    private final double prob;
    private final Random rand = new Random();


    public ProbabilityTest(double prob) {
        this.prob = prob;
    }


    public int runSimulation(int count) {
        int result = 0;

        for (int i = 0; i < count; i++) {
            if (rand.nextDouble() < prob) {
                result++;
            }
        }

        return result;
    }


    public static void compareSim() {
        ProbabilityTest test1 = new ProbabilityTest(0.45);
        ProbabilityTest test2 = new ProbabilityTest(0.50); // 4% better!

        final int NUM_SIM = 1000;
        int numLess = 0;
        int numGreater = 0;
        int numEqual = 0;

        for (int i = 0; i < NUM_SIM; i++) {
            int count1 = test1.runSimulation(100);
            int count2 = test2.runSimulation(100);
            if (count1 < count2) {
                numLess++;
            } else if (count1 > count2) {
                numGreater++;
            } else {
                numEqual++;
            }
        }

        System.out.println(String.format("NUM_SIM = %d   numLess = %d   numEqual = %d   numGreater = %d", NUM_SIM, numLess, numEqual, numGreater));
    }


    public static void main(String[] args) {
        compareSim();
    }

}
