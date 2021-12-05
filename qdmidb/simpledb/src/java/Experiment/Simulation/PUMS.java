package Experiment.Simulation;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;

public class PUMS {

    int columnSize = 33;
    private final String dirtyACSPath = "";
    private final String cleanACSPath = "";
    BufferedWriter cleanACSWriter, dirtyACSWriter;

    public PUMS(){
        openWriters();
        getCleanACS();
        getDirtyACS();
        closeWriters();
    }

    public void openWriters(){
        try {
            String cleanACS = Paths.get(cleanACSPath).toAbsolutePath().toString();
            FileWriter cleanACSFile = new FileWriter(cleanACS);
            cleanACSWriter = new BufferedWriter(cleanACSFile);

            String dirtyACS = Paths.get(dirtyACSPath).toAbsolutePath().toString();
            FileWriter dirtyACSFile = new FileWriter(dirtyACS);
            dirtyACSWriter = new BufferedWriter(dirtyACSFile);
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void closeWriters(){
        try{
            cleanACSWriter.close();
            dirtyACSWriter.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void getCleanACS(){

    }

    public void getDirtyACS(){

    }
}
