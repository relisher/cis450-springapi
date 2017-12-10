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
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import java.util.HashMap;
import java.util.Map;



// This client will default to US West (Oregon)
/**
 *
 * @author arelin
 */
public class DynamoDBQuery {
    

    	private AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
    			.withRegion(Regions.US_EAST_1).build();
    			private DynamoDB dynamoDB = new DynamoDB(client);
 
    public String query() {
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
    
        public String genderOrientation(){
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
		sb.append(result2.getCount().toString() + ","+"LGBTQ females\n");
		
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
		sb.append(result4.getCount().toString() + ","+"LGBTQ males\n");
		
		System.out.println(sb.toString());
		return sb.toString();
    }
    
   
    
       public String marriedGender(){
    	StringBuilder sb = new StringBuilder();
    	sb.append("count,descriptor\n");
		Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>(); 
		expressionAttributeValues.put(":married", new AttributeValue().withS("married")); 
		expressionAttributeValues.put(":gender", new AttributeValue().withS("f")); 
		ScanRequest scanRequest = new ScanRequest()
		   // .withTableName("OkCupid")
			.withTableName("OkCupid")
		    .withFilterExpression("sex = :gender AND cupidstatus = :married")
		    .withExpressionAttributeValues(expressionAttributeValues);
		
		ScanResult result = client.scan(scanRequest);
		sb.append(result.getCount().toString() + ","+"married women\n");
		
		ScanRequest scanRequest2 = new ScanRequest()
				   // .withTableName("OkCupid")
			.withTableName("OkCupid")
			.withFilterExpression("sex <> :gender AND cupidstatus = :married")
			.withExpressionAttributeValues(expressionAttributeValues);
				
		ScanResult result2 = client.scan(scanRequest2);
		sb.append(result2.getCount().toString() + ","+"married men\n");
		
		ScanRequest scanRequest3 = new ScanRequest()
			// .withTableName("OkCupid")
			.withTableName("OkCupid")
			.withFilterExpression("sex = :gender AND cupidstatus <> :married")
			.withExpressionAttributeValues(expressionAttributeValues);
						
		ScanResult result3 = client.scan(scanRequest3);
		sb.append(result3.getCount().toString() + ","+"unmarried women\n");
		ScanRequest scanRequest4 = new ScanRequest()
				// .withTableName("OkCupid")
				.withTableName("OkCupid")
				.withFilterExpression("sex <> :gender AND cupidstatus <> :married")
				.withExpressionAttributeValues(expressionAttributeValues);
							
		ScanResult result4 = client.scan(scanRequest4);
		sb.append(result4.getCount().toString() + ","+"unmarried men\n");
		
		System.out.println(sb.toString());
		return sb.toString();
    }
    
    public String married(){
    	StringBuilder sb = new StringBuilder();
    	sb.append("count,descriptor\n");
		Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>();
		expressionAttributeValues.put(":married", new AttributeValue().withS("married"));  
		ScanRequest scanRequest = new ScanRequest()
		   // .withTableName("OkCupid")
			.withTableName("OkCupid")
		    .withFilterExpression("cupidstatus = :married")
		    .withExpressionAttributeValues(expressionAttributeValues);
		
		ScanResult result = client.scan(scanRequest);
		sb.append(result.getCount().toString() + ","+"married\n");
		System.out.println(sb.toString());
		return sb.toString();
    }
    
    public String marriedLocation(){
    	StringBuilder sb = new StringBuilder();
    	sb.append("count,descriptor\n");
		Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>();
		expressionAttributeValues.put(":married", new AttributeValue().withS("married"));  
		expressionAttributeValues.put(":sanfran", new AttributeValue().withS("san francisco, california"));  
		expressionAttributeValues.put(":sanjose", new AttributeValue().withS("vallejo, california"));
		expressionAttributeValues.put(":oakland", new AttributeValue().withS("oakland, california"));
		expressionAttributeValues.put(":sanrafael", new AttributeValue().withS("san rafael, california"));
		expressionAttributeValues.put(":walnutcreek", new AttributeValue().withS("walnut creek, california"));
		ScanRequest scanRequest = new ScanRequest()
		  //.withTableName("OkCupid")
			.withTableName("OkCupid")
		    .withFilterExpression("cupidstatus = :married AND cupidlocation = :sanfran "
		    		+ "AND cupidlocation <> :sanjose AND cupidlocation <> :oakland "
		    		+ "AND cupidlocation <> :sanrafael AND cupidlocation <> :walnutcreek")
		    .withExpressionAttributeValues(expressionAttributeValues);
		ScanResult result = client.scan(scanRequest);
		sb.append(result.getCount().toString() + ","+"married in san francisco\n");
		//san francisco, oakland, vallejo, san rafael, walnut creek
		ScanRequest scanRequest2 = new ScanRequest()
				   // .withTableName("OkCupid")
			.withTableName("OkCupid")
			.withFilterExpression("cupidstatus = :married AND cupidlocation <> :sanfran "
		    		+ "AND cupidlocation = :sanjose AND cupidlocation <> :oakland "
		    		+ "AND cupidlocation <> :sanrafael AND cupidlocation <> :walnutcreek")
			.withExpressionAttributeValues(expressionAttributeValues);
				
		ScanResult result2 = client.scan(scanRequest2);
		sb.append(result2.getCount().toString() + ","+"married in vallejo\n");
		
		ScanRequest scanRequest3 = new ScanRequest()
			// .withTableName("OkCupid")
			.withTableName("OkCupid")
			.withFilterExpression("cupidstatus = :married AND cupidlocation <> :sanfran "
		    		+ "AND cupidlocation <> :sanjose AND cupidlocation = :oakland "
		    		+ "AND cupidlocation <> :sanrafael AND cupidlocation <> :walnutcreek")			
			.withExpressionAttributeValues(expressionAttributeValues);
						
		ScanResult result3 = client.scan(scanRequest3);
		sb.append(result3.getCount().toString() + ","+"married in oakland\n");
		ScanRequest scanRequest4 = new ScanRequest()
				// .withTableName("OkCupid")
				.withTableName("OkCupid")
				.withFilterExpression("cupidstatus = :married AND cupidlocation <> :sanfran "
			    		+ "AND cupidlocation <> :sanjose AND cupidlocation <> :oakland "
			    		+ "AND cupidlocation = :sanrafael AND cupidlocation <> :walnutcreek")				
				.withExpressionAttributeValues(expressionAttributeValues);
							
		ScanResult result4 = client.scan(scanRequest4);
		sb.append(result4.getCount().toString() + ","+"married in san rafael\n");
		
		ScanRequest scanRequest5 = new ScanRequest()
				// .withTableName("OkCupid")
				.withTableName("OkCupid")
				.withFilterExpression("cupidstatus = :married AND cupidlocation <> :sanfran "
			    		+ "AND cupidlocation <> :sanjose AND cupidlocation <> :oakland "
			    		+ "AND cupidlocation = :sanrafael AND cupidlocation <> :walnutcreek")				
				.withExpressionAttributeValues(expressionAttributeValues);
							
		ScanResult result5 = client.scan(scanRequest5);
		sb.append(result5.getCount().toString() + ","+"married in walnut creek\n");
		
		System.out.println(sb.toString());
		return sb.toString();
    }
    
    public String genderQuery() {
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
    
        public String youngUsers(){
    	StringBuilder sb = new StringBuilder();
    	sb.append("cityname,zillow20,okcupid20pop\n");
		Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>();
		expressionAttributeValues.put(":age", new AttributeValue().withN("30")); 
		expressionAttributeValues.put(":sanfran", new AttributeValue().withS("san francisco, california"));  
		expressionAttributeValues.put(":sanjose", new AttributeValue().withS("vallejo, california"));
		expressionAttributeValues.put(":oakland", new AttributeValue().withS("oakland, california"));
		expressionAttributeValues.put(":sanrafael", new AttributeValue().withS("san rafael, california"));
		expressionAttributeValues.put(":walnutcreek", new AttributeValue().withS("walnut creek, california"));
		ScanRequest scanRequest1 = new ScanRequest()
		  //.withTableName("OkCupid")
			.withTableName("OkCupid")
		    .withFilterExpression("age < :age AND cupidlocation = :sanfran "
		    		+ "AND cupidlocation <> :sanjose AND cupidlocation <> :oakland "
		    		+ "AND cupidlocation <> :sanrafael AND cupidlocation <> :walnutcreek")
		    .withExpressionAttributeValues(expressionAttributeValues);
		ScanResult result1 = client.scan(scanRequest1);
		//san francisco, oakland, vallejo, san rafael, walnut creek
		ScanRequest scanRequest2 = new ScanRequest()
				   // .withTableName("OkCupid")
			.withTableName("OkCupid")
			.withFilterExpression("age < :age AND cupidlocation <> :sanfran "
		    		+ "AND cupidlocation = :sanjose AND cupidlocation <> :oakland "
		    		+ "AND cupidlocation <> :sanrafael AND cupidlocation <> :walnutcreek")
			.withExpressionAttributeValues(expressionAttributeValues);
				
		ScanResult result2 = client.scan(scanRequest2);
		
		ScanRequest scanRequest3 = new ScanRequest()
			//.withTableName("OkCupid")
			.withTableName("OkCupid")
			.withFilterExpression("age < :age AND cupidlocation <> :sanfran "
		    		+ "AND cupidlocation <> :sanjose AND cupidlocation = :oakland "
		    		+ "AND cupidlocation <> :sanrafael AND cupidlocation <> :walnutcreek")			
			.withExpressionAttributeValues(expressionAttributeValues);
						
		ScanResult result3 = client.scan(scanRequest3);
		
		ScanRequest scanRequest4 = new ScanRequest()
				// .withTableName("OkCupid")
			.withTableName("OkCupid")
			.withFilterExpression("age < :age AND cupidlocation <> :sanfran "
			    	+ "AND cupidlocation <> :sanjose AND cupidlocation <> :oakland "
			    	+ "AND cupidlocation = :sanrafael AND cupidlocation <> :walnutcreek")				
			.withExpressionAttributeValues(expressionAttributeValues);
							
		ScanResult result4 = client.scan(scanRequest4);
		
		ScanRequest scanRequest5 = new ScanRequest()
				// .withTableName("OkCupid")
				.withTableName("OkCupid")
				.withFilterExpression("age < :age AND cupidlocation <> :sanfran "
			    		+ "AND cupidlocation <> :sanjose AND cupidlocation <> :oakland "
			    		+ "AND cupidlocation = :sanrafael AND cupidlocation <> :walnutcreek")				
				.withExpressionAttributeValues(expressionAttributeValues);
							
		ScanResult result5 = client.scan(scanRequest5);
		
		Map<String, AttributeValue> expressionAttributeValuesZillow = new HashMap<String, AttributeValue>(); 
		expressionAttributeValuesZillow.put(":sanfran", new AttributeValue().withS("san francisco, california"));  
		expressionAttributeValuesZillow.put(":sanjose", new AttributeValue().withS("vallejo, california"));
		expressionAttributeValuesZillow.put(":oakland", new AttributeValue().withS("oakland, california"));
		expressionAttributeValuesZillow.put(":sanrafael", new AttributeValue().withS("san rafael, california"));
		expressionAttributeValuesZillow.put(":walnutcreek", new AttributeValue().withS("walnut creek, california"));
		ScanRequest scanRequest11 = new ScanRequest()
				.withTableName("Zillow")
			    .withFilterExpression("city = :sanfran "
			    		+ "AND city <> :sanjose AND city <> :oakland "
			    		+ "AND city <> :sanrafael AND city <> :walnutcreek")
			    .withExpressionAttributeValues(expressionAttributeValuesZillow);
			ScanResult result11 = client.scan(scanRequest11);
			//san francisco, oakland, vallejo, san rafael, walnut creek
			ScanRequest scanRequest12 = new ScanRequest()
					   // .withTableName("Zillow")
				.withTableName("Zillow")
				.withFilterExpression("city <> :sanfran "
			    		+ "AND city = :sanjose AND city <> :oakland "
			    		+ "AND city <> :sanrafael AND city <> :walnutcreek")
				.withExpressionAttributeValues(expressionAttributeValuesZillow);
					
			ScanResult result12 = client.scan(scanRequest12);
			
			ScanRequest scanRequest13 = new ScanRequest()
				//.withTableName("Zillow")
				.withTableName("Zillow")
				.withFilterExpression("city <> :sanfran "
			    		+ "AND city <> :sanjose AND city = :oakland "
			    		+ "AND city <> :sanrafael AND city <> :walnutcreek")			
				.withExpressionAttributeValues(expressionAttributeValuesZillow);
							
			ScanResult result13 = client.scan(scanRequest13);
			
			ScanRequest scanRequest14 = new ScanRequest()
					// .withTableName("Zillow")
				.withTableName("Zillow")
				.withFilterExpression("city <> :sanfran "
				    	+ "AND city <> :sanjose AND city <> :oakland "
				    	+ "AND city = :sanrafael AND city <> :walnutcreek")				
				.withExpressionAttributeValues(expressionAttributeValuesZillow);
								
			ScanResult result14 = client.scan(scanRequest14);
			
			ScanRequest scanRequest15 = new ScanRequest()
					// .withTableName("Zillow")
					.withTableName("Zillow")
					.withFilterExpression("city <> :sanfran "
				    		+ "AND city <> :sanjose AND city <> :oakland "
				    		+ "AND city <> :sanrafael AND city = :walnutcreek")				
					.withExpressionAttributeValues(expressionAttributeValuesZillow);
								
			ScanResult result15 = client.scan(scanRequest15);
			
				sb.append("san francisco" + ","+ (Double.toString(Double.parseDouble(result11.getItems().get(0).get("per20s").getN().toString())*100)) +"," + (double)(result1.getCount())*100/864800 + "\n");
				sb.append("vallejo" + ","+ (Double.toString(Double.parseDouble(result12.getItems().get(0).get("per20s").getN().toString())*100)) +"," + (double)(result2.getCount())*100/122300 + "\n");
				sb.append("oakland" + ","+ (Double.toString(Double.parseDouble(result13.getItems().get(0).get("per20s").getN().toString())*100)) +"," + (double)(result3.getCount())*100/420000 + "\n");
				sb.append("san rafael" + ","+ (Double.toString(Double.parseDouble(result14.getItems().get(0).get("per20s").getN().toString())*100)) +"," + (double)(result4.getCount())*100/59000 + "\n");
				sb.append("walnut creek" + ","+ (Double.toString(Double.parseDouble(result15.getItems().get(0).get("per20s").getN().toString())*100)) +"," + (double)(result5.getCount())*100/69000 + "\n");

		System.out.println(sb.toString());
		return sb.toString();
    }
        
    public String lgbtqLocation(){
    	StringBuilder sb = new StringBuilder();
    	sb.append("cityname,income,lgbtpercent\n");
		Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>();
		expressionAttributeValues.put(":orientation", new AttributeValue().withS("straight")); 
		expressionAttributeValues.put(":sanfran", new AttributeValue().withS("san francisco, california"));  
		expressionAttributeValues.put(":sanjose", new AttributeValue().withS("vallejo, california"));
		expressionAttributeValues.put(":oakland", new AttributeValue().withS("oakland, california"));
		expressionAttributeValues.put(":sanrafael", new AttributeValue().withS("san rafael, california"));
		expressionAttributeValues.put(":walnutcreek", new AttributeValue().withS("walnut creek, california"));
		ScanRequest scanRequest1 = new ScanRequest()
		  //.withTableName("OkCupid")
			.withTableName("OkCupid")
		    .withFilterExpression("orientation = :orientation AND cupidlocation = :sanfran "
		    		+ "AND cupidlocation <> :sanjose AND cupidlocation <> :oakland "
		    		+ "AND cupidlocation <> :sanrafael AND cupidlocation <> :walnutcreek")
		    .withExpressionAttributeValues(expressionAttributeValues);
		ScanResult result1 = client.scan(scanRequest1);
		//san francisco, oakland, vallejo, san rafael, walnut creek
		ScanRequest scanRequest2 = new ScanRequest()
				   // .withTableName("OkCupid")
			.withTableName("OkCupid")
			.withFilterExpression("orientation = :orientation AND cupidlocation <> :sanfran "
		    		+ "AND cupidlocation = :sanjose AND cupidlocation <> :oakland "
		    		+ "AND cupidlocation <> :sanrafael AND cupidlocation <> :walnutcreek")
			.withExpressionAttributeValues(expressionAttributeValues);
				
		ScanResult result2 = client.scan(scanRequest2);
		
		ScanRequest scanRequest3 = new ScanRequest()
			//.withTableName("OkCupid")
			.withTableName("OkCupid")
			.withFilterExpression("orientation = :orientation AND cupidlocation <> :sanfran "
		    		+ "AND cupidlocation <> :sanjose AND cupidlocation = :oakland "
		    		+ "AND cupidlocation <> :sanrafael AND cupidlocation <> :walnutcreek")			
			.withExpressionAttributeValues(expressionAttributeValues);
						
		ScanResult result3 = client.scan(scanRequest3);
		
		ScanRequest scanRequest4 = new ScanRequest()
				// .withTableName("OkCupid")
			.withTableName("OkCupid")
			.withFilterExpression("orientation = :orientation AND cupidlocation <> :sanfran "
			    	+ "AND cupidlocation <> :sanjose AND cupidlocation <> :oakland "
			    	+ "AND cupidlocation = :sanrafael AND cupidlocation <> :walnutcreek")				
			.withExpressionAttributeValues(expressionAttributeValues);
							
		ScanResult result4 = client.scan(scanRequest4);
		
		ScanRequest scanRequest5 = new ScanRequest()
				// .withTableName("OkCupid")
				.withTableName("OkCupid")
				.withFilterExpression("orientation = :orientation AND cupidlocation <> :sanfran "
			    		+ "AND cupidlocation <> :sanjose AND cupidlocation <> :oakland "
			    		+ "AND cupidlocation = :sanrafael AND cupidlocation <> :walnutcreek")				
				.withExpressionAttributeValues(expressionAttributeValues);
							
		ScanResult result5 = client.scan(scanRequest5);
		
		Map<String, AttributeValue> expressionAttributeValuesZillow = new HashMap<String, AttributeValue>(); 
		expressionAttributeValuesZillow.put(":sanfran", new AttributeValue().withS("san francisco, california"));  
		expressionAttributeValuesZillow.put(":sanjose", new AttributeValue().withS("vallejo, california"));
		expressionAttributeValuesZillow.put(":oakland", new AttributeValue().withS("oakland, california"));
		expressionAttributeValuesZillow.put(":sanrafael", new AttributeValue().withS("san rafael, california"));
		expressionAttributeValuesZillow.put(":walnutcreek", new AttributeValue().withS("walnut creek, california"));
		ScanRequest scanRequest11 = new ScanRequest()
				.withTableName("Zillow")
			    .withFilterExpression("city = :sanfran "
			    		+ "AND city <> :sanjose AND city <> :oakland "
			    		+ "AND city <> :sanrafael AND city <> :walnutcreek")
			    .withExpressionAttributeValues(expressionAttributeValuesZillow);
			ScanResult result11 = client.scan(scanRequest11);
			//san francisco, oakland, vallejo, san rafael, walnut creek
			ScanRequest scanRequest12 = new ScanRequest()
					   // .withTableName("Zillow")
				.withTableName("Zillow")
				.withFilterExpression("city <> :sanfran "
			    		+ "AND city = :sanjose AND city <> :oakland "
			    		+ "AND city <> :sanrafael AND city <> :walnutcreek")
				.withExpressionAttributeValues(expressionAttributeValuesZillow);
					
			ScanResult result12 = client.scan(scanRequest12);
			
			ScanRequest scanRequest13 = new ScanRequest()
				//.withTableName("Zillow")
				.withTableName("Zillow")
				.withFilterExpression("city <> :sanfran "
			    		+ "AND city <> :sanjose AND city = :oakland "
			    		+ "AND city <> :sanrafael AND city <> :walnutcreek")			
				.withExpressionAttributeValues(expressionAttributeValuesZillow);
							
			ScanResult result13 = client.scan(scanRequest13);
			
			ScanRequest scanRequest14 = new ScanRequest()
					// .withTableName("Zillow")
				.withTableName("Zillow")
				.withFilterExpression("city <> :sanfran "
				    	+ "AND city <> :sanjose AND city <> :oakland "
				    	+ "AND city = :sanrafael AND city <> :walnutcreek")				
				.withExpressionAttributeValues(expressionAttributeValuesZillow);
								
			ScanResult result14 = client.scan(scanRequest14);
			
			ScanRequest scanRequest15 = new ScanRequest()
					// .withTableName("Zillow")
					.withTableName("Zillow")
					.withFilterExpression("city <> :sanfran "
				    		+ "AND city <> :sanjose AND city <> :oakland "
				    		+ "AND city <> :sanrafael AND city = :walnutcreek")				
					.withExpressionAttributeValues(expressionAttributeValuesZillow);
								
			ScanResult result15 = client.scan(scanRequest15);
		
		
		ScanRequest scanRequest6 = new ScanRequest()
				  //.withTableName("OkCupid")
					.withTableName("OkCupid")
				    .withFilterExpression("orientation <> :orientation AND cupidlocation = :sanfran "
				    		+ "AND cupidlocation <> :sanjose AND cupidlocation <> :oakland "
				    		+ "AND cupidlocation <> :sanrafael AND cupidlocation <> :walnutcreek")
				    .withExpressionAttributeValues(expressionAttributeValues);
				ScanResult result6 = client.scan(scanRequest6);
				
				sb.append("san francisco" + ","+ result11.getItems().get(0).get("MedianHouseholdIncome").getN().toString() +"," + (double)(result6.getCount())/(result1.getCount() + result6.getCount()) + "\n");
				//san francisco, oakland, vallejo, san rafael, walnut creek
				ScanRequest scanRequest7 = new ScanRequest()
						   // .withTableName("OkCupid")
					.withTableName("OkCupid")
					.withFilterExpression("orientation <> :orientation AND cupidlocation <> :sanfran "
				    		+ "AND cupidlocation = :sanjose AND cupidlocation <> :oakland "
				    		+ "AND cupidlocation <> :sanrafael AND cupidlocation <> :walnutcreek")
					.withExpressionAttributeValues(expressionAttributeValues);
				
				ScanResult result7 = client.scan(scanRequest7);
				sb.append("vallejo" + ","+ result12.getItems().get(0).get("MedianHouseholdIncome").getN().toString() +"," + (double)(result7.getCount())/(result2.getCount() + result7.getCount()) + "\n");
				
				ScanRequest scanRequest8 = new ScanRequest()
					//.withTableName("OkCupid")
					.withTableName("OkCupid")
					.withFilterExpression("orientation <> :orientation AND cupidlocation <> :sanfran "
				    		+ "AND cupidlocation <> :sanjose AND cupidlocation = :oakland "
				    		+ "AND cupidlocation <> :sanrafael AND cupidlocation <> :walnutcreek")			
					.withExpressionAttributeValues(expressionAttributeValues);
								
				ScanResult result8 = client.scan(scanRequest8);
				sb.append("oakland" + ","+ result13.getItems().get(0).get("MedianHouseholdIncome").getN().toString() +"," + (double)(result8.getCount())/(result3.getCount() + result8.getCount()) + "\n");
				ScanRequest scanRequest9 = new ScanRequest()
						// .withTableName("OkCupid")
						.withTableName("OkCupid")
						.withFilterExpression("orientation <> :orientation AND cupidlocation <> :sanfran "
					    		+ "AND cupidlocation <> :sanjose AND cupidlocation <> :oakland "
					    		+ "AND cupidlocation = :sanrafael AND cupidlocation <> :walnutcreek")				
						.withExpressionAttributeValues(expressionAttributeValues);
									
				ScanResult result9 = client.scan(scanRequest9);
				//sb.append(result4.getCount().toString() + "," + result9.getCount().toString() + ","+"san rafael\n");
				sb.append("san rafael" + ","+ result14.getItems().get(0).get("MedianHouseholdIncome").getN().toString() +"," + (double)(result9.getCount())/(result4.getCount() + result9.getCount()) + "\n");

				ScanRequest scanRequest10 = new ScanRequest()
						// .withTableName("OkCupid")
						.withTableName("OkCupid")
						.withFilterExpression("orientation <> :orientation AND cupidlocation <> :sanfran "
					    		+ "AND cupidlocation <> :sanjose AND cupidlocation <> :oakland "
					    		+ "AND cupidlocation <> :sanrafael AND cupidlocation = :walnutcreek")				
						.withExpressionAttributeValues(expressionAttributeValues);
									
				ScanResult result10 = client.scan(scanRequest10);
				//sb.append(result5.getCount().toString() + "," + result10.getCount().toString() + ","+"walnut creek\n");
				sb.append("walnut creek" + ","+ result15.getItems().get(0).get("MedianHouseholdIncome").getN().toString() +"," + (double)(result10.getCount())/(result5.getCount() + result10.getCount()) + "\n");

		System.out.println(sb.toString());
		return sb.toString();
    }

}
