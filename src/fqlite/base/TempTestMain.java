package fqlite.base;

import java.util.concurrent.ExecutionException;

import fqlite.descriptor.TableDescriptor;

public class TempTestMain {

    public static void main(String [] args) throws InterruptedException, ExecutionException {
        Job job = new Job();
        
        job.readWAL = false;
        job.readRollbackJournal = false;
        job.path = "T:\\tmp\\msgstore.db";
        Global.LOGLEVEL = Base.ERROR;
        Job.LOGLEVEL = Base.ERROR;
        Global.CONVERT_DATETIME = false;
        
        job.processDB();
        
        Global.LOGLEVEL = Base.ALL;
        Job.LOGLEVEL = Base.ALL;
        for (TableDescriptor td : job.headers) {
            td.printTableDefinition();
            for (SqliteRow row : job.getRowsForTable(td.getName())) {
                System.out.println(row.toString());
            }
        }        
    }
}
