package com.example;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.datastax.oss.driver.api.core.ConsistencyLevel;

import java.nio.file.Paths;

public class AstraDbTest {

    private static final String TABLE_NAME = "locatestorebystorenumber";  
    private static final String keyspace = "store_v5"; 

    public static void main(String[] args) {
        // Replace with your Astra DB secure connect bundle path and credentials
        String bundlePath = "";
        String username = "";
        String password = "";

        try (CqlSession session = CqlSession.builder()
                .withCloudSecureConnectBundle(Paths.get(bundlePath))
                .withAuthCredentials(username, password)
                .build()) {

        
            SelectFrom selectFrom = QueryBuilder.selectFrom(TABLE_NAME);
            Select query = selectFrom.column("storenbr");

            // Convert the query to a SimpleStatement, setting the keyspace and consistency level
            SimpleStatement statement = createReadSimple(query);

            // Execute the statement and print the results
            ResultSet rs = session.execute(statement);
            rs.forEach(row -> System.out.println(row.getFormattedContents()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

  
    public static SimpleStatement createReadSimple(Select query) {
        return query.build()
                .setKeyspace(keyspace)
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM); // Adjust this as needed
    }
}
