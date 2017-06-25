

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.hp.hpl.jena.query.ResultSet;


public class TimeOut { 

	static ResultSet result = null;
	public static void main(String[] args) throws Exception {
		String endPoint = "http://ring.ciard.net/sparql1";
		analyseSparqlComplex(endPoint, 1);
    }

	public static void analyseSparqlComplex(String endPoint, int timeOutSeconds) throws InterruptedException, ExecutionException {
		ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<String> future = executor.submit(new VerifyEndPoint(endPoint));

        try { 
            System.out.println("Started..");
            System.out.println(future.get(timeOutSeconds, TimeUnit.SECONDS));
            System.out.println("Finished!");
        } catch (TimeoutException e) {
            //Without the below cancel the thread will continue to live 
            // even though the timeout exception thrown.
            future.cancel(true);
            System.out.println("Time out: " + endPoint);
        } 
        executor.shutdownNow();
	} 
	
	public static ResultSet isGood(String endPoint, int timeOutSeconds) throws InterruptedException, ExecutionException{
		ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<String> future = executor.submit(new VerifyEndPoint(endPoint));

        try { 
            System.out.println("Started..");
            System.out.println(future.get(timeOutSeconds, TimeUnit.SECONDS));
            System.out.println("Finished!");
            return result;
        } catch (TimeoutException e) {
            //Without the below cancel the thread will continue to live 
            // even though the timeout exception thrown.
            future.cancel(true);
            System.out.println("Time out: " + endPoint);
        } 
        executor.shutdownNow();
        return result;
	}
} 

class VerifyEndPoint implements Callable<String> {
	String sEndPoint = null;
	public VerifyEndPoint(String pEndPoint) {
		sEndPoint = pEndPoint;
	}
	
    @Override 
    public String call() throws Exception {
    	//AnalyseComplexSparql.analyseSparqlComplex(sEndPoint);
    	TimeOut.result = AnalyseComplexSparql.isGood(sEndPoint);
    	return null;
    }
} 
