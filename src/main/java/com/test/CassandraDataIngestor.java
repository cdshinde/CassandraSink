package com.test;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class CassandraDataIngestor {
	
	private static final String RESPONDENT_INSERT = "INSERT INTO "+
					" respondent (respondent_id,ida,idb) values (?,?,?)";
	
	
	public static void storeInCassandra(Session session, String eventBody){
		String localBody = eventBody.trim();
		System.out.println("-------- " + localBody);
		if (localBody.contains("|")) {
            try {
            	
            	System.out.println("Into the if block ");
                int respId = Integer.parseInt(localBody.substring(0, localBody.indexOf("|")));
                long ida = Long.parseLong(localBody.substring(localBody.indexOf("|") + 1, localBody.lastIndexOf("|")));
                //System.out.println("Special Print "+Long.parseLong(localBody.substring(localBody.lastIndexOf("|") + 1, localBody.length())));
                long idb = Long.parseLong(localBody.substring(localBody.lastIndexOf("|") + 1, localBody.length()));
                
                System.out.println("************ The value to be logged is  " + respId + ", " + ida + ", " + idb);
                
                PreparedStatement insertStatement = session.prepare(RESPONDENT_INSERT);
                BoundStatement boundStatement = new BoundStatement(insertStatement);
                session.execute(boundStatement.bind(respId, ida, idb));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
	}

}
