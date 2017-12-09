/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hala.cis450;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;



// This client will default to US West (Oregon)
/**
 *
 * @author arelin
 */
public class DynamoDBQuery {
 
    public String query() {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
            .withRegion(Regions.US_EAST_1).build();
        DynamoDB dynamoDB = new DynamoDB(client);

    Table table = dynamoDB.getTable("okcupidsmall");

    Map<String, AttributeValue> expressionAttributeValues = 
        new HashMap<String, AttributeValue>();
    expressionAttributeValues.put(":val", new AttributeValue().withS("f")); 
        
    ScanRequest scanRequest = new ScanRequest()
        .withTableName("okcupidsmall")
        .withProjectionExpression("city, sex")
        .withFilterExpression("sex = :val")
        .withExpressionAttributeValues(expressionAttributeValues);


    ScanResult result = client.scan(scanRequest);
    return result.toString();
    }
    
    public String genderQuery() {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
            .withRegion(Regions.US_EAST_1).build();
    	StringBuilder sb = new StringBuilder();
    	sb.append("count,descriptor\n");
		Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>();
		expressionAttributeValues.put(":valerie", new AttributeValue().withS("f")); 
		expressionAttributeValues.put(":orientation", new AttributeValue().withS("straight")); 
		ScanRequest scanRequest = new ScanRequest()
		   // .withTableName("OkCupid")
			.withTableName("OkCupid")
		    .withFilterExpression("sex = :valerie and orientation = :orientation")
		    .withExpressionAttributeValues(expressionAttributeValues);
		
		ScanResult result = client.scan(scanRequest);
		sb.append(result.getCount().toString() + ","+"straight females\n");
		
		ScanRequest scanRequest2 = new ScanRequest()
				   // .withTableName("OkCupid")
					.withTableName("OkCupid")
				    .withFilterExpression("sex = :valerie and orientation <> :orientation")
				    .withExpressionAttributeValues(expressionAttributeValues);
				
		ScanResult result2 = client.scan(scanRequest2);
		sb.append(result2.getCount().toString() + ","+"gay females\n");
		
		ScanRequest scanRequest3 = new ScanRequest()
				   // .withTableName("OkCupid")
			.withTableName("OkCupid")
			.withFilterExpression("sex = :valerie and orientation = :orientation")
		    .withExpressionAttributeValues(expressionAttributeValues);
				
			ScanResult result3 = client.scan(scanRequest3);
			sb.append(result3.getCount().toString() + ","+"straight males\n");
				
		ScanRequest scanRequest4 = new ScanRequest()
						   // .withTableName("OkCupid")
			.withTableName("OkCupid")
			.withFilterExpression("sex = :valerie and orientation <> :orientation")
			.withExpressionAttributeValues(expressionAttributeValues);
						
		ScanResult result4 = client.scan(scanRequest4);
		sb.append(result4.getCount().toString() + ","+"gay males");

		return sb.toString();
    
        }
}
