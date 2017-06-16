package org.kepler.spark.mllib;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.kepler.spark.actor.SparkBaseActor;

import ptolemy.actor.TypedIOPort;
import ptolemy.data.LongToken;
import ptolemy.data.ObjectToken;
import ptolemy.data.type.BaseType;
import ptolemy.data.type.ObjectType;
import ptolemy.kernel.CompositeEntity;
import ptolemy.kernel.util.IllegalActionException;
import ptolemy.kernel.util.NameDuplicationException; import ptolemy.kernel.util.SingletonAttribute;

/* 
 * Author: Dylan Uys
 * Returns a new DataFrame, removing all Rows with any null values from the input DataFrame
 */
public class RemoveRowsWithNulls extends SparkBaseActor {

    public RemoveRowsWithNulls(CompositeEntity container, String name)
            throws IllegalActionException, NameDuplicationException {
        super(container, name);
        
        inData = new TypedIOPort(this, "inData", true, false);
        inData.setTypeEquals(new ObjectType(Dataset.class));
        new SingletonAttribute(inData, "_showName");

        outData = new TypedIOPort(this, "outData", false, true);
        outData.setTypeEquals(new ObjectType(Dataset.class));
        new SingletonAttribute(outData, "_showName");

        
        count = new TypedIOPort(this, "count", false, true);
        count.setTypeEquals(BaseType.LONG);
        new SingletonAttribute(count, "_showName");

    }

    @Override
    public void fire() throws IllegalActionException {
        
        super.fire();

        Dataset<Row> df = (Dataset<Row>)((ObjectToken)inData.get(0)).getValue();
        
        // Drop rows with null values
        Dataset<Row> filtered = df.na().drop();
        
        outData.broadcast(new ObjectToken(filtered, Dataset.class));
	
	    if (count.numberOfSinks() > 0) {
	        count.broadcast(new LongToken(filtered.count()));
	    }
    }
        
    /** Incoming DataFrame */
    public TypedIOPort inData;
    
    /** New DataFrame without null rows */
    public TypedIOPort outData;
    
    /** Number of rows of the filtered dataset. */
    public TypedIOPort count;
   
    
}

