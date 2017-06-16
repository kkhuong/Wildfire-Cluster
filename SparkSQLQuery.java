package org.kepler.spark.mllib;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
//import org.postgresql.Driver;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.kepler.spark.actor.SparkSQLActor;

import ptolemy.actor.TypedIOPort;
import ptolemy.data.LongToken;
import ptolemy.data.ObjectToken;
import ptolemy.data.expr.StringParameter;
import ptolemy.data.type.BaseType;
import ptolemy.data.type.ObjectType;
import ptolemy.kernel.CompositeEntity;
import ptolemy.kernel.util.IllegalActionException;
import ptolemy.kernel.util.NameDuplicationException;
import ptolemy.kernel.util.SingletonAttribute;

/*
 * Author: Dylan Uys
 * This actor performs a query from the Postgres database that stores HPWREN weather
 * data using SparkSQL's distributed query engine. Query options, such as the server, database,
 * table name, and more, are configured in the HashMap<String, String> options member of the class.
 */
public class SparkSQLQuery extends SparkSQLActor {

    public SparkSQLQuery(CompositeEntity container, String name)
            throws IllegalActionException, NameDuplicationException {
        super(container, name);

        // initialize ports and parameters
        first = new TypedIOPort(this, "first", false, true);
        first.setTypeEquals(new ObjectType(Row.class));
        new SingletonAttribute(first, "_showName");
  
        out = new TypedIOPort(this, "out", false, true);
        out.setTypeEquals(new ObjectType(Dataset.class));
        new SingletonAttribute(out, "_showName");
        
        count = new TypedIOPort(this, "count", false, true);
        count.setTypeEquals(BaseType.LONG);
        new SingletonAttribute(count, "_showName");
        

        // Set the default value for all the query option parameters
        // Note that changes to the structure of this query may affect data processing
        // downstream in the workflow
        dbColsParam = new StringParameter(this, "Columns");
        dbColsParam.setToken("air_pressure,"
				      + "air_temp,"
				      + "avg_wind_direction,"
					  + "avg_wind_speed,"
				      + "max_wind_direction,"
					  + "max_wind_speed,"
					  + "relative_humidity");

        dbTableParam = new StringParameter(this, "Table");
        dbTableParam.setToken("kmeans_actor_sample_set");

        stationIdParam = new StringParameter(this, "Station ID");
        stationIdParam.setToken("92"); 

        withRowNumParam = new StringParameter(this, "With row_number");
        withRowNumParam.setToken("true");

        usernameParam = new StringParameter(this, "Username");
        usernameParam.setToken("duys");
        
        passwordParam = new StringParameter(this, "Password");
        passwordParam.setToken("w1fir32016");
        
    }

    
    @Override
    public void preinitialize() throws IllegalActionException {
    	super.preinitialize();
    	
    	SQL_CONNECTION_URL = "jdbc:postgresql://florian.sdsc.edu/wifire?user=" +
    			     usernameParam.getValueAsString() +
    			     "&password=" +
    			     passwordParam.getValueAsString();
    	
	
    	options = new HashMap<String, String>();

        String query = "(SELECT " + dbColsParam.getValueAsString();
        
        if (withRowNumParam.getValueAsString().equalsIgnoreCase("true")) {
            query += ", ROW_NUMBER() OVER (ORDER BY hpwren_timestamp) AS row_number";
        }
        query += " FROM " + dbTableParam.getValueAsString();
        query += " WHERE station_id=" + stationIdParam.getValueAsString();
        query += ") AS weather_data";

    	// initialize the options HashMap from the actor's parameter values
        options.put("driver", SQL_DRIVER);
        options.put("url", SQL_CONNECTION_URL);
        options.put("dbtable", query);
    } 

    /** 
     *  Perform the sql query that is specified by the "dbtable" field of options 
     *  The results are stored in a Dataset<Row> and broadcasted by the out port.   
     */ 

    @Override 
    public void fire() throws IllegalActionException { 

       	super.fire();
        
        try {
            // perform the query
            Dataset<Row> df = _sqlContext.read().format("jdbc").options(options).load();
        	
            // broadcasdt results
	        out.broadcast(new ObjectToken(df, Dataset.class)); 

        // only broadcast debug information if an actor is hooked up to the port
	    if (first.numberOfSinks() > 0) {
		    first.broadcast(new ObjectToken(df.first(), Row.class)); 
	    }

	    if (count.numberOfSinks() > 0) {
		    count.broadcast(new LongToken(df.count())); 
        }

	    } catch (OutOfMemoryError e) { 
	        System.out.println(e.getMessage()); 
	        System.out.println("ERROR: SparkSQLQuery: Out of Java Heap Space!"); 
	    } catch (IllegalActionException e) {
	        System.out.println("ERROR: SparkSQLQuery: Failed to read from database.");
        }
    }

    
    /** The first element of the DataFrame. Debug */
    public TypedIOPort first;
    
    /** The DataFrame resulting from the SparkSQL query */
    public TypedIOPort out;
    
    /** Number of Rows in the DataFrame */
    public TypedIOPort count;
    
    /** Options map for SQL config */
    protected Map<String, String> options;
    
    public StringParameter dbTableParam;
    public StringParameter dbColsParam;
    public StringParameter stationIdParam;
    public StringParameter withRowNumParam;
    public StringParameter usernameParam;
    public StringParameter passwordParam;
    
    private static final String SQL_DRIVER = "org.postgresql.Driver";
    private static String SQL_CONNECTION_URL;
}
