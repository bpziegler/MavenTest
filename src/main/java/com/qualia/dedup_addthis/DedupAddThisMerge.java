package com.qualia.dedup_addthis;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import com.google.common.base.Charsets;


/**
 * Process AddThisMapping files. The goal is to remove dups that come in over time, and reduce the load on the graph
 * database.
 * 
 * Each addThisMapping file is processed by being merged with the "database" file. The database file continues to grow
 * as more AddThisMapping files are merged into it. Note it could be pruned by removing lines that have a timestamp > 30
 * days, etc)
 * 
 * addThisMapping file + database file => new database file + file for new mappings + file for new last_seen values. 
 * The database file has the mappings + tab + lastseen (timestamp). The mappings is the column with 6=1234,9=22345,1172=3223
 * NOTE the addThisMapping file must be pre-processed to have mapping in first column, tab, then timestamp in second
 * column. Also must be SORTED!
 * 
 * LocalResponse cookies are ignored.
 * 
 * Note to preprocess the addThisMapping file, these commands can be used:
 * # This command Moves column 3 to the front, then column 1
 * cat "$1" | gunzip | awk '{ print $3"\t"$1 }' > mappings_"$1"
 * # This command sorts the file
 * java -jar ~/externalsortinginjava-0.1.10-SNAPSHOT.jar -v mappings_"$1" mappings_sorted_"$1"
 * # This command runs the merge:
 * java -jar ../MavenTest-0.0.1-SNAPSHOT-jar-with-dependencies.jar DedupAddThisMerge mappings_sorted_"$1"
 * 
 */
public class DedupAddThisMerge {

    private static final String DB_FILE_PATH = "database.txt";
    private static final String DB_OUTPUT_FILE_PATH = "database_output_temp.txt";
    private static final String NEW_MAPPINGS_ROOT = "new_mappings_";
    private static final String NEW_TIMESTAMPS_ROOT = "new_timestamps_";


    private void run(String addThisFilePath) throws IOException {
        File dbFile = new File(DB_FILE_PATH);
        File dbOutputFile = new File(DB_OUTPUT_FILE_PATH);

        if (!dbFile.exists()) {
            // Create an empty one
            dbFile.createNewFile();
        }

        BufferedReader databaseReader = createReader(dbFile);
        BufferedReader addThisReader = createReader(new File(addThisFilePath));
        BufferedWriter databaseWriter = createWriter(dbOutputFile);
        BufferedWriter newMappingsWriter = createWriter(new File(NEW_MAPPINGS_ROOT + addThisFilePath));
        BufferedWriter newTimestampsWriter = createWriter(new File(NEW_TIMESTAMPS_ROOT + addThisFilePath));

        AddThisLine dbLine;
        AddThisLine addThisLine;

        dbLine = new AddThisLine(databaseReader.readLine());
        addThisLine = new AddThisLine(addThisReader.readLine());

        // Begin processing Loop
        while ((dbLine.line != null) || (addThisLine.line != null)) {
            int compare = AddThisLine.compareLines(dbLine, addThisLine);
            if (compare < 0) {
                // Copy from database
                // TODO - Filter out lines that are older than 30 days
                databaseWriter.write(dbLine.line);
                databaseWriter.newLine();
                dbLine = new AddThisLine(databaseReader.readLine());
            } else if (compare > 0) {
                // Copy from addThis
                databaseWriter.write(addThisLine.line);
                databaseWriter.newLine();
                newMappingsWriter.write(addThisLine.line);
                newMappingsWriter.newLine();
                newTimestampsWriter.write(addThisLine.line);
                newTimestampsWriter.newLine();
                addThisLine = new AddThisLine(addThisReader.readLine());
            } else {
                // Need to merge
                // Keep the mapping from one (they are both the same), and keep the largest (most recent) timestamp
                Long dbTimestamp = Long.valueOf(dbLine.parts.get(1));
                Long addThisTimestamp = Long.valueOf(addThisLine.parts.get(1));
                long maxTimestamp = Math.max(dbTimestamp, addThisTimestamp);
                String newLine = dbLine.parts.get(0) + "\t" + Long.toString(maxTimestamp);
                databaseWriter.write(newLine);
                databaseWriter.newLine();
                if (addThisTimestamp > dbTimestamp) {
                    newTimestampsWriter.write(addThisLine.line);
                    newTimestampsWriter.newLine();
                }
                dbLine = new AddThisLine(databaseReader.readLine());
                addThisLine = new AddThisLine(addThisReader.readLine());
            }
        }

        databaseReader.close();
        addThisReader.close();
        databaseWriter.close();
        newMappingsWriter.close();
        newTimestampsWriter.close();

        dbFile.delete();
        dbOutputFile.renameTo(dbFile);
    }


    private static BufferedReader createReader(File file) throws FileNotFoundException {
        FileInputStream fis = new FileInputStream(file);
        InputStreamReader isr = new InputStreamReader(fis, Charsets.UTF_8);
        BufferedReader br = new BufferedReader(isr);
        return br;
    }


    private static BufferedWriter createWriter(File file) throws FileNotFoundException {
        FileOutputStream fos = new FileOutputStream(file);
        OutputStreamWriter osw = new OutputStreamWriter(fos);
        BufferedWriter bw = new BufferedWriter(osw);
        return bw;
    }


    public static void main(String[] args) throws IOException {
        String addThisFilePath = args[0];
        DedupAddThisMerge program = new DedupAddThisMerge();
        program.run(addThisFilePath);
    }
}
