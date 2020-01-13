package org.lem.marsjob.service;


import org.lem.marsjob.pojo.JobParam;

public abstract class JobEvenetListener {

    abstract void listen(JobParam jobParam);

}
