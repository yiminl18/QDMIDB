package Experiment.Service;

import Experiment.CDCData.*;
import Experiment.Simulation.PUMS;
import Experiment.Simulation.WiFi;

import java.io.IOException;

public class main {
    public static void main(String args[])throws IOException {
        //WiFi wifi = new WiFi("wifi");
        Metadata md = new Metadata();
        md.getSchema();
    }
}
