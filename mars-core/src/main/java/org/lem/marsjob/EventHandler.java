package org.lem.marsjob;

import org.lem.marsjob.pojo.JobParam;

public interface EventHandler {
    void handleEvent(JobParam jobParam);
}

