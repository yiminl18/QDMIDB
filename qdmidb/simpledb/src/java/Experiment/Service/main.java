package Experiment.Service;

import Experiment.CDCData.*;
import Experiment.Simulation.WiFi;

import java.io.IOException;

public class main {
    public static void main(String args[])throws IOException {
        Metadata md = new Metadata();
        md.getSchema();
        //WiFi wifi = new WiFi("occupancy");
    }
}
