package org.gridkit.lab.gridbeans.monadic.builder;

import org.gridkit.lab.gridbeans.monadic.ExecutionGraph.CallDescription;
import org.gridkit.lab.gridbeans.monadic.ExecutionGraph.CheckpointDescription;
import org.gridkit.lab.gridbeans.monadic.ExecutionGraph.ExecutionObserver;

public class PrintObserver implements ExecutionObserver {

    @Override
    public void onFire(CallDescription call) {
        System.out.println("FIRE -> " + call);
    }

    @Override
    public void onComplete(CallDescription call) {
        System.out.println("DONE <- " + call);
    }

    @Override
    public void onCheckpoint(CheckpointDescription checkpoint) {
        if (checkpoint.isGlobal()) {
            System.out.println("CHECKPOINT " + checkpoint);
        }
        else {
            System.out.println("SYNC [" + checkpoint.getExecutionHost() + "] " + checkpoint);
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
