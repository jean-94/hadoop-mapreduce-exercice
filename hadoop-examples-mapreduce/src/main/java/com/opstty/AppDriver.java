package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");

            programDriver.addClass("districttrees", DistrictTrees.class,
                    "A map/reduce program that display the list of district that contains trees.");

            programDriver.addClass("treespecies", TreeSpecies.class,
                    "A map/reduce program that display the list of different species trees.");

            programDriver.addClass("treespeciescount", TreeSpeciesCount.class,
                    " a map/reduce program that count the number of trees of each species.");

            programDriver.addClass("tallesttreespecies", TallestTreeSpecies.class,
                    " a map/reduce program that calculates the height of the tallest tree of each species.");

            programDriver.addClass("sortheight", SortHeight.class,
                    "a map/reduce program that the trees height from smallest to largest.");

            programDriver.addClass("districtoldesttree", DistrictOldestTree.class,
                    " a map/reduce program that display the district where the oldest tree is.");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
