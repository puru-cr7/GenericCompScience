import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * class to get started with dynamo
 *
 * @author Purnendu Rath (puru_cr7).
 *         Created Oct 3, 2018.
 */
public class PerformanceDynamo {
	public static void main(final String[] args) throws InterruptedException, JsonParseException, IOException {
		Util.setCreds();
		final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
				.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2"))
				.build();
		final DynamoDB dynamoDB = new DynamoDB(client);
		final String tableName = "Movies";

		final Table t1 = dynamoDB.getTable(tableName);
		if (t1 != null) {
			t1.delete();
			t1.waitForDelete();
		}
		// CREATE Table
		final Table table = dynamoDB.createTable(tableName,
				Arrays.asList(new KeySchemaElement("year", KeyType.HASH),
						new KeySchemaElement("title", KeyType.RANGE)),
				Arrays.asList(new AttributeDefinition("year", ScalarAttributeType.N),
						new AttributeDefinition("title", ScalarAttributeType.S)),
				new ProvisionedThroughput(10L, 10L));
		table.waitForActive();

		final JsonParser parser = new JsonFactory()
				.createParser(new File("E:\\Programming\\WS\\\\Getting_started_with_NOSQL\\src\\main\\resources\\moviedata1.json"));

		final JsonNode rootNode = new ObjectMapper().readTree(parser);
		final Iterator<JsonNode> iter = rootNode.iterator();

		ObjectNode currentNode;

		// insert all
		long t = System.currentTimeMillis();
		int count = 0;
		while (iter.hasNext()) {
			count++;
			currentNode = (ObjectNode) iter.next();
			final int year = currentNode.path("year").asInt();
			final String title = currentNode.path("title").asText();
			table.putItem(new Item().withPrimaryKey("year", year, "title", title).withJSON("info",
					currentNode.path("info").toString()));

		}
		System.out.println(System.currentTimeMillis() - t + " ms to insert " + count + " records");

		final Map<String, Object> infoMap = new HashMap<>();
		infoMap.put("plot", "Nothing happens at all.");
		infoMap.put("rating", 0);

		// insert
		t = System.currentTimeMillis();
		table.putItem(new Item().withPrimaryKey("year", 2015, "title", "The Big New Movie").withMap("info", infoMap));
		System.out.println(System.currentTimeMillis() - t + " ms to insert " + 1 + " record");

		// read
		t = System.currentTimeMillis();
		final GetItemSpec spec = new GetItemSpec().withPrimaryKey("year", 2015, "title", "The Big New Movie");
		final Item outcome = table.getItem(spec);
		System.out.println(System.currentTimeMillis() - t + " ms to fetch " + 1 + " record");
		System.out.println("GetItem succeeded: " + outcome);

		final int year = 2015;
		final String title = "The Big New Movie";
		// update
		final UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("year", year, "title", title)
				.withUpdateExpression("set info.rating = :r, info.plot=:p, info.actors=:a")
				.withValueMap(new ValueMap().withNumber(":r", 5.5).withString(":p", "Everything happens all at once.")
						.withList(":a", Arrays.asList("Larry", "Moe", "Curly")))
				.withReturnValues(ReturnValue.UPDATED_NEW);
		t = System.currentTimeMillis();
		final UpdateItemOutcome outcome1 = table.updateItem(updateItemSpec);
		System.out.println(System.currentTimeMillis() - t + " ms to update " + 1 + " record");
		System.out.println("UpdateItem succeeded:\n" + outcome1.getItem().toJSONPretty());

		// delete
		t = System.currentTimeMillis();
		final DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
				.withPrimaryKey(new PrimaryKey("year", year, "title", title));

		table.deleteItem(deleteItemSpec);
		System.out.println(System.currentTimeMillis() - t + " ms to delete " + 1 + " record");

		table.delete();
		table.waitForDelete();

	}

}
