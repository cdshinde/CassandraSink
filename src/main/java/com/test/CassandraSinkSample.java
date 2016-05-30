package com.test;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraSinkSample extends AbstractSink implements Configurable {
	
	 private static String hosts = " ";    
	 private static String keyspaceName = " ";    
	 
	 private static Cluster cluster = null;
	 private static Session session = null;
	 
	 public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss.SSS");

	public Status process() throws EventDeliveryException {
		// TODO Auto-generated method stub
		Status status = null;
		
		System.out.println("************** Into the Process method ************** ");
		
		Channel ch = getChannel();		
		Transaction txn = ch.getTransaction();
		txn.begin();
		try{
			
			Event event = ch.take();
			String eventBody = new String(event.getBody());
			CassandraDataIngestor.storeInCassandra(session, new String(event.getBody()));
			txn.commit();
			status = Status.READY;
		}catch(Throwable t){
			txn.rollback();
			status = Status.BACKOFF;
		}finally{
			txn.close();
		}
		
		return status;
	}

	public void configure(Context arg0) {
		// TODO Auto-generated method stub
		hosts = "172.27.44.76";
		keyspaceName = "gfkpoc";
		
	}
	
	public void start(){
		super.start();
		cluster = Cluster.builder().addContactPoint(hosts).build();
        session = cluster.connect(keyspaceName);
        
        System.out.println("Test started "+ sdf.format(Calendar.getInstance().getTime()));
	}
	
	public void stop(){
		super.stop();
		cluster.close();
		
		System.out.println("Test ended "+ sdf.format(Calendar.getInstance().getTime()));
	}

}
