/* Train a KMeans model in MLlib.
 * 
 * Copyright (c) 2014 The Regents of the University of California.
 * All rights reserved.
 *
 * '$Author: crawl $'
 * '$Date: 2015-11-23 12:02:01 -0800 (Mon, 23 Nov 2015) $' 
 * '$Revision: 34243 $'
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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;

import ptolemy.actor.TypedAtomicActor;
import ptolemy.actor.TypedIOPort;
import ptolemy.data.expr.StringParameter;
import ptolemy.actor.parameters.PortParameter;
import ptolemy.data.ArrayToken;
import ptolemy.data.DoubleToken;
import ptolemy.data.IntToken;
import ptolemy.data.LongToken;
import ptolemy.data.ObjectToken;
import ptolemy.data.Token;
import ptolemy.data.type.ArrayType;
import ptolemy.data.type.BaseType;
import ptolemy.data.type.ObjectType;
import ptolemy.kernel.CompositeEntity;
import ptolemy.kernel.util.IllegalActionException;
import ptolemy.kernel.util.NameDuplicationException;
import ptolemy.kernel.util.SingletonAttribute;

/** Train a KMeans model in Spark MLlib.
 * 
 *  @author Daniel Crawl
 *  @version $Id: KMeansModel.java 34243 2015-11-23 20:02:01Z crawl $
 * 
 */
public class KMeansModel extends TypedAtomicActor {
    
    public KMeansModel(CompositeEntity container, String name)
            throws IllegalActionException, NameDuplicationException {
        super(container, name);
        
        data = new TypedIOPort(this, "data", true, false);
        data.setTypeEquals(new ObjectType(JavaRDD.class));
        new SingletonAttribute(data, "_showName");
        
        numClusters = new PortParameter(this, "numClusters");
        numClusters.setTypeEquals(BaseType.INT);
        numClusters.getPort().setTypeEquals(BaseType.INT);
        new SingletonAttribute(numClusters.getPort(), "_showName");

        numRuns = new PortParameter(this, "numRuns");
        numRuns.setTypeEquals(BaseType.INT);
        numRuns.getPort().setTypeEquals(BaseType.INT);
        new SingletonAttribute(numRuns.getPort(), "_showName");
        numRuns.setToken(IntToken.ONE);

        iterations = new PortParameter(this, "iterations");
        iterations.setTypeEquals(BaseType.INT);
        iterations.getPort().setTypeEquals(BaseType.INT);
        new SingletonAttribute(iterations.getPort(), "_showName");
        iterations.setToken(new IntToken(10));
        
        seed = new PortParameter(this, "seed");
        seed.setTypeEquals(BaseType.LONG);
        seed.getPort().setTypeEquals(BaseType.LONG);
        new SingletonAttribute(seed.getPort(), "_showName");
        
        error = new TypedIOPort(this, "error", false, true);
        error.setTypeEquals(BaseType.DOUBLE);
        new SingletonAttribute(error, "_showName");
        
        centers = new TypedIOPort(this, "centers", false, true);
        centers.setTypeEquals(new ArrayType(new ArrayType(BaseType.DOUBLE)));
        new SingletonAttribute(centers, "_showName");

        clusterSizes = new TypedIOPort(this, "clusterSizes", false, true);
        clusterSizes.setTypeEquals(new ArrayType(BaseType.LONG));
        new SingletonAttribute(clusterSizes, "_showName");

	initSteps = new PortParameter(this, "initSteps");
	initSteps.setTypeEquals(BaseType.INT);	
	initSteps.getPort().setTypeEquals(BaseType.INT);
	new SingletonAttribute(initSteps.getPort(), "_showName");
	initSteps.setToken(new IntToken(5));

	initializationMode = new StringParameter(this, "initializationMode");
    }
    
    @Override
    public void fire() throws IllegalActionException {

        super.fire();
        
        iterations.update();
        final int numIterations = ((IntToken)iterations.getToken()).intValue();
        
        numClusters.update();
        final int numClustersVal = ((IntToken)numClusters.getToken()).intValue();

        numRuns.update();
        final int numRunsVal = ((IntToken)numRuns.getToken()).intValue();
        
        seed.update();
        final long seedVal = ((LongToken)seed.getToken()).longValue();

	initSteps.update();
	final int numInitSteps = ((IntToken)initSteps.getToken()).intValue();
	
	final String initMode = initializationMode.getValueAsString();
        
        final KMeans kmeans = new KMeans();
        kmeans.setMaxIterations(numIterations);
        kmeans.setK(numClustersVal);
        kmeans.setSeed(seedVal);
	    kmeans.setInitializationMode(initMode);
	    kmeans.setInitializationSteps(numInitSteps);

        final JavaRDD<Vector> javaRDD = (JavaRDD<Vector>) ((ObjectToken)data.get(0)).getValue();
        final RDD<Vector> rdd = javaRDD.rdd();
        
        _model = kmeans.run(rdd.cache());
        
        final double errorVal = _model.computeCost(rdd);        
        error.broadcast(new DoubleToken(errorVal));

        Vector[] centerVectors = _model.clusterCenters();
        ArrayList<ArrayToken> centersArray = new ArrayList<ArrayToken>(centerVectors.length);
        for(Vector vector : centerVectors) {
            ArrayList<Token> center = new ArrayList<Token>(vector.size());
            for(double val : vector.toArray()) {
                center.add(new DoubleToken(val));
            }
            centersArray.add(new ArrayToken(center.toArray(new Token[vector.size()])));
        }
        centers.broadcast(new ArrayToken(centersArray.toArray(new ArrayToken[centerVectors.length])));
        
        JavaRDD<Integer> prediction =_model.predict(javaRDD);
        Map<Integer,Long> counts = prediction.countByValue();
        List<Token> countsArray = new ArrayList<Token>();
        for(Long count : counts.values()) {
            countsArray.add(new LongToken(count.longValue()));
        }
        clusterSizes.broadcast(new ArrayToken(countsArray.toArray(new Token[countsArray.size()])));
        
    }
    
    /** The input vectors. */ 
    public TypedIOPort data;
        
    /** The number of clusters. */
    public PortParameter numClusters;
    
    /** The number of runs of the algorithm to execute in parallel. */
    public PortParameter numRuns;
    
    /** The maximum number of iterations to run. */
    public PortParameter iterations;
    
    /** The random seed value to use for cluster initialization . */
    public PortParameter seed;

    public StringParameter initializationMode;	

    public PortParameter initSteps; 
  
    /** The sum of squared distances to their nearest center. */
    public TypedIOPort error;
    
    /** The center of the clusters. */
    public TypedIOPort centers;
    
    /** The size of each cluster. */
    public TypedIOPort clusterSizes;
    

    /** The MLlib KMeans model. */
    private org.apache.spark.mllib.clustering.KMeansModel _model;

}
