package org.gridkit.lab.gridbeans.monadic.builder;

import org.gridkit.lab.gridbeans.monadic.ExecutionGraph;
import org.gridkit.lab.gridbeans.monadic.ExecutionGraph.CallDescription;

public class PrintObserver implements ExecutionGraph.ExecutionObserver {

    @Override
    public void onFire(CallDescription call) {
        System.out.println("FIRE -> " + call);
    }

    @Override
    public void onComplete(CallDescription call) {
        System.out.println("DONE <- " + call);
    }

    @Override
    public void onCheckpoint(ExecutionGraph.Checkpoint checkpoint) {
        if (checkpoint.host() == null) {
            System.out.println("CHECKPOINT " + checkpoint);
        }
        else {
            System.out.println("SYNC [" + checkpoint.host() + "] " + checkpoint);
        }
    }

    @Override
    public void onFailure(Exception error) {
        System.out.println("EXECUTION FAILURE " + error);
    }

    @Override
    public void onFinish() {
        System.out.println("EXECUTION COMPLETE");
    }
}
