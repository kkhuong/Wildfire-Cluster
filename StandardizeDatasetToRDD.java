/* 
 * Copyright (c) 2015 The Regents of the University of California.
 * All rights reserved.
 *
 * '$Author: crawl $'
 * '$Date: 2015-06-04 15:39:55 -0700 (Thu, 04 Jun 2015) $' 
 * '$Revision: 33462 $'
 * 
 * Permission is hereby granted, without written agreement and without
 * license or royalty fees, to use, copy, modify, and distribute this
 * software and its documentation for any purpose, provided that the above
 * copyright notice and the following two paragraphs appear in all copies
 * of this software.
 *
 * IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY
 * FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES
 * ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF
 * THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 * THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE
 * PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, AND THE UNIVERSITY OF
 * CALIFORNIA HAS NO OBLIGATION TO PROVIDE MAINTENANCE, SUPPORT, UPDATES,
 * ENHANCEMENTS, OR MODIFICATIONS.
 *
 */
package org.kepler.spark.mllib;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.feature.StandardScaler;
import org.apache.spark.mllib.feature.StandardScalerModel;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import ptolemy.actor.TypedAtomicActor;
import ptolemy.actor.TypedIOPort;
import ptolemy.actor.parameters.PortParameter;
import ptolemy.data.ArrayToken;
import ptolemy.data.DoubleToken;
import ptolemy.data.IntToken;
import ptolemy.data.LongToken;
import ptolemy.data.ObjectToken;
import ptolemy.data.StringToken;
import ptolemy.data.Token;
import ptolemy.data.type.ArrayType;
import ptolemy.data.type.BaseType;
import ptolemy.data.type.ObjectType;
import ptolemy.kernel.CompositeEntity;
import ptolemy.kernel.util.IllegalActionException;
import ptolemy.kernel.util.NameDuplicationException;
import ptolemy.kernel.util.SingletonAttribute;

public class StandardizeDatasetToRDD extends TypedAtomicActor {

    public StandardizeDatasetToRDD (CompositeEntity container, String name)
            throws IllegalActionException, NameDuplicationException {
        super(container, name);
        
        inData = new TypedIOPort(this, "inData", true, false);
        inData.setTypeEquals(new ObjectType(Dataset.class));
        new SingletonAttribute(inData, "_showName");
        
        outData = new TypedIOPort(this, "outData", false, true);
        outData.setTypeEquals(new ObjectType(JavaRDD.class));
        new SingletonAttribute(outData, "_showName");
        
        meanVec = new TypedIOPort(this, "meanVec", false, true);
        meanVec.setTypeEquals (new ObjectType(Vector.class));
        new SingletonAttribute(meanVec, "_showName");
        
        stdevVec = new TypedIOPort(this, "stdevVec", false, true);
        stdevVec.setTypeEquals(new ObjectType(Vector.class));
        new SingletonAttribute(stdevVec, "_showName");

        test =  new TypedIOPort(this, "test", false, true);
        test.setTypeEquals(new ObjectType(Vector.class));
        new SingletonAttribute(test, "_showName");

    }

    /* This actor standardizes the values in a Dataset, and outputs an RDD containing
     * the standardized data. Used if actors downstream fo this actor use RDDs and MLlib 
     * rather than Datasetse and ML.
     */
    @Override
    public void fire() throws IllegalActionException {
        
        super.fire();
        
        // Read data
        Dataset<Row> ds = (Dataset<Row>) ((ObjectToken)inData.get(0)).getValue();
	    JavaRDD<Vector> javaRDD = ds.toJavaRDD().map(new DsConverter());

        RDD<Vector> data = javaRDD.rdd();

        // Perform standardization by subtracting mean and dividing by stdev
        final StandardScaler scaler = new StandardScaler(true,true);
        StandardScalerModel scalerModel = scaler.fit(data.cache());
        
        // Get values for scaled data, mean vector, and stdev vector
        // for output parameters
        RDD<Vector> scaledData = scalerModel.transform(data);

        JavaRDD<Vector> scData = (JavaRDD<Vector>) javaRDD.wrapRDD(scaledData);
        outData.broadcast(new ObjectToken(scData, JavaRDD.class));
                
        if (meanVec.numberOfSinks() > 0) {
            meanVec.broadcast (new ObjectToken(scalerModel.mean(), Vector.class));
        }

        if (stdevVec.numberOfSinks() > 0) {
            stdevVec.broadcast(new ObjectToken(scalerModel.std(), Vector.class));
        }

        if (test.numberOfSinks() > 0) {
            test.broadcast(new ObjectToken(scData.first(),Vector.class));
        }
            
    }  
    
    // Used to convert a Dataset to an RDD. Used with map to convert each
    // Row of the Dataset to a Vector in the output RDD
    private static class DsConverter implements Function<Row, Vector> {
        @Override
        public Vector call(Row in) {
            double arr[] = new double[in.size()];
	        for (int i = 0; i < in.size(); i++) {
		        try {
			        arr[i] = (double)in.get(i);
		        } catch (java.lang.ClassCastException e) {
			        arr[i] = (double)in.getInt(i);
		        }
 	        }
	        return Vectors.dense(arr);
       }
    }

    /** Input data set as an RDD of Vectors. */
    public TypedIOPort inData;
    
    /** Standardized data set as an RDD of Vectors. */
    public TypedIOPort outData;
    
    /** Mean value used in standardization.*/
    public TypedIOPort meanVec;
  
    /** Variance value used in standardization.*/
    public TypedIOPort stdevVec;
    
    /** Outputs the first row of the new dataset only for test purpose*/
    public TypedIOPort test;
    
    
}
