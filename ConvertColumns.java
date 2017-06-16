package org.kepler.spark.mllib;


import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.kepler.spark.actor.SparkSQLActor;

import ptolemy.actor.TypedIOPort;
import ptolemy.data.ObjectToken;
import ptolemy.data.expr.StringParameter;
import ptolemy.data.type.ObjectType;
import ptolemy.kernel.CompositeEntity;
import ptolemy.kernel.util.IllegalActionException;
import ptolemy.kernel.util.NameDuplicationException;
import ptolemy.kernel.util.SingletonAttribute;

/*
 * Author: Dylan Uys
 * This actor converts the values in specified columns from one unit
 * of measurement to another. Accomplished by performing element wise multiplication on
 * the specified columns by a factor determined by what input units and output units
 * are selected (currently only supports meters/second and miles/hour) 
 */
public class ConvertColumns extends SparkSQLActor {

	public ConvertColumns(CompositeEntity container, String name)
            throws IllegalActionException, NameDuplicationException {
        super(container, name);
        
       	// initialize ports and parameters
        inData = new TypedIOPort(this, "inData", true, false);
        inData.setTypeEquals(new ObjectType(Dataset.class));
        new SingletonAttribute(inData, "_showName");
        
        outData = new TypedIOPort(this, "outData", false, true);
        outData.setTypeEquals(new ObjectType(Dataset.class));
        new SingletonAttribute(outData, "_showName");
        
        first = new TypedIOPort(this, "first", false, true);
        first.setTypeEquals(new ObjectType(Row.class));
        new SingletonAttribute(first, "_showName");
        
        columnNamesParam = new StringParameter(this, "Column Names");
        columnNamesParam.setToken("avg_wind_speed,max_wind_speed");
        
        inUnitsParam = new StringParameter(this, "Input Units");
        inUnitsParam.addChoice("miles per hour");
        inUnitsParam.addChoice("meters per second");
        inUnitsParam.setToken("meters per second");
        
        outUnitsParam = new StringParameter(this, "Output Units");
        outUnitsParam.addChoice("miles per hour");
        outUnitsParam.addChoice("meters per second");
        outUnitsParam.setToken("miles per hour");
        
	}
   
	
	@Override
    public void preinitialize() throws IllegalActionException {
    	super.preinitialize();
    
    	// Determine the conversion factor by getting the inUnits and outUnits values from
    	// the string parameters
    	convertFactor = getConvertFactor(inUnitsParam.stringValue(),
                                         outUnitsParam.stringValue());
	
	}
	
    @Override public void fire() throws IllegalActionException { 
    	super.fire();

    	// Read data
        Dataset<Row> df = (Dataset<Row>)((ObjectToken)inData.get(0)).getValue();
        
        // Get names of columns to convert
        String colNames = columnNamesParam.getValueAsString();

        for (String colName: colNames.split(",")) {
	        // element wise multiplication, make sure to get rid of possible whitespace
            Column convertedCol = df.col(colName.trim()).multiply(convertFactor);

            // newly converted one
    	    df = df.withColumn(colName.trim(), convertedCol);
        } 
    	
    	// Broadcast results
        outData.broadcast(new ObjectToken(df, Dataset.class));

        if (first.numberOfSinks() > 0) {
            first.broadcast(new ObjectToken(df.first(), Row.class));
        }
       
    }
	
    // Used to determine the factor by which to multiply the incoming value 
    private double getConvertFactor(String inUnits, String outUnits) {
    	if (inUnits.equalsIgnoreCase("meters per second")) {
    		if (outUnits.equalsIgnoreCase("miles per hour")) {
    			return 2.23694;
            } 
        } else if (inUnits.equalsIgnoreCase("miles per hour")) {
            if (outUnits.equalsIgnoreCase("meters per second")) {
                return 0.44704;
           } 
        }
        return 1.0;
    }

    
    /** The input DataFrame */ 
    public TypedIOPort inData;
        
    /** The output DataFrame */
    public TypedIOPort outData;
    
    /** First row of dataframe for debug purposes */ 
    public TypedIOPort first;
     
    /** incoming units of value to convert */
    public StringParameter inUnitsParam;

    /** outgoing units of value to convert */
    public StringParameter outUnitsParam;
    
    /** outgoing units of value to convert */
    public StringParameter columnNamesParam;
    
    /** value determined by getConvertFactor() based on incoming units and outgoing units */
    private double convertFactor;
    
   
}


