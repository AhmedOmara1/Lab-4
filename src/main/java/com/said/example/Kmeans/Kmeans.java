package com.said.example.Kmeans;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.collections.IteratorUtils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.util.Vector;

import scala.Tuple2;

public final class Kmeans {

    private static final Pattern COMMA = Pattern.compile(",");
    private static JavaSparkContext context;
    public static void main(String[] args) throws Exception {

        String path = args[0];        // input file on HDFS
        String outputPath = args[1];  // output dir on HDFS
        int k = Integer.parseInt(args[2]);

        int maxIterations = Integer.MAX_VALUE;
        double convergenceEpslon = 0.0;

        if (args.length > 3) {
            if (!"MAX".equals(args[3])) {
                maxIterations = Integer.parseInt(args[3]);
            }
        }

        if (args.length > 4) {
            convergenceEpslon = Double.parseDouble(args[4]);
        }

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("kmeans");

        context = new JavaSparkContext(conf);

        JavaRDD<Vector> data = context.textFile(path)
                .map(new Function<String, Vector>() {
 @Override
                    public Vector call(String line) {
                        return parseVector(line);
                    }
                }).cache();

        List<Vector> finalCentroids =
                kmeans(data, k, maxIterations, convergenceEpslon);

        List<String> outputLines = new ArrayList<String>();
        for (Vector c : finalCentroids) {
            outputLines.add(c.toString());
        }
   JavaRDD<String> output = context.parallelize(outputLines, 1);
        output.saveAsTextFile(outputPath);
    }
public static List<Vector> kmeans(JavaRDD<Vector> data,
                                      int k,
                                      int maxIterations,
                                      double convergenceEpslon) {

        long count = data.count();

        if (k > count) {
            throw new IllegalArgumentException(
                    "Dataset has fewer points than the number of clusters.");
        } else if (k == 0 || count == 0 || count == 1) {
 throw new IllegalArgumentException(
                    "At least 2 points are required to build clusters.");
        }          else if (k == 1) {
            List<Vector> allPoints = new ArrayList<Vector>(data.collect());
            List<Vector> list = new ArrayList<Vector>();
            list.add(average(allPoints));
            return list;
        }

 // final عشان Java 7 يسمح نستخدمه جوه الـ anonymous class
        final List<Vector> centroids = new ArrayList<Vector>();

                List<Vector> allPoints = new ArrayList<Vector>(data.collect());
        Collections.shuffle(allPoints);
        for (int i = 0; i < k; i++) {
            centroids.add(allPoints.get(i));
        }

        double tempDist;
 long counter = 0;

        long start = System.currentTimeMillis();

        do {
            JavaPairRDD<Integer, Vector> closest = data
                    .mapToPair(new PairFunction<Vector, Integer, Vector>() {
                        @Override
  public Tuple2<Integer, Vector> call(Vector vector) {
                            int idx = closestPoint(vector, centroids);
                            return new Tuple2<Integer, Vector>(idx, vector);
                        }
                    });
  JavaPairRDD<Integer, Iterable<Vector>> pointsGroup =
                    closest.groupByKey();

            Map<Integer, Vector> newCentroids = pointsGroup.mapValues(
                    new Function<Iterable<Vector>, Vector>() {
                        @Override
                        public Vector call(Iterable<Vector> ps) {
                            @SuppressWarnings("unchecked")
List<Vector> list =
                                    (List<Vector>) IteratorUtils.toList(ps.iterator());
                            return average(list);
                        }
                    }
            ).collectAsMap();
tempDist = 0.0;
            for (int j = 0; j < k; j++) {
                Vector oldC = centroids.get(j);
                Vector newC = newCentroids.get(j);
                tempDist += oldC.squaredDist(newC);
            }

            for (Map.Entry<Integer, Vector> t : newCentroids.entrySet()) {
                centroids.set(t.getKey(), t.getValue());
            }
 counter++;

        } while (tempDist > convergenceEpslon && counter < maxIterations);

        long end = System.currentTimeMillis();
        long timeElapsed = end - start;
        System.out.println("Time taken: " + timeElapsed + " milliseconds");

        return centroids;
    }
  static Vector parseVector(String line) {
        String[] splits = COMMA.split(line);
        double[] data = new double[splits.length];
        for (int i = 0; i < splits.length; i++) {
            data[i] = Double.parseDouble(splits[i]);
        }
        return new Vector(data);
    }
static int closestPoint(Vector p, List<Vector> centers) {
        int bestIndex = 0;
        double closest = Double.POSITIVE_INFINITY;
        for (int i = 0; i < centers.size(); i++) {
            double tempDist = p.squaredDist(centers.get(i));
            if (tempDist < closest) {
                closest = tempDist;
                bestIndex = i;
            }
        }
        return bestIndex;
    }
    static Vector average(List<Vector> ps) {
        int numVectors = ps.size();
        Vector out = new Vector(ps.get(0).elements());
        for (int i = 1; i < numVectors; i++) {
            out.addInPlace(ps.get(i));
        }
        return out.divide(numVectors);
    }
}
