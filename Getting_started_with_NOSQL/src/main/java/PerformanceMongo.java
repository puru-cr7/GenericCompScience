import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.bson.Document;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

/**
 * class to get started with mongo
 *
 * @author Purnendu Rath (puru_cr7).
 *         Created Sep 30, 2018.
 */
public class PerformanceMongo {
	public static void main(final String[] args) throws FileNotFoundException, IOException, ParseException {

		try (MongoClient mongo = new MongoClient("localhost", 27017)) {
			final MongoDatabase db = mongo.getDatabase("TEST");
			// CREATE a collection
			final MongoCollection<Document> c = db.getCollection("MOVIES");
			final JSONParser jp = new JSONParser();
			final JSONArray arr = (JSONArray) jp
					.parse(new FileReader(new File("E:\\Programming\\WS\\\\Getting_started_with_NOSQL\\src\\main\\resources\\moviedata.json")));

			// insert
			long t = System.currentTimeMillis();
			for (final Object obj : arr) {
				c.insertOne(Document.parse(obj.toString()));
			}

			System.out.println(System.currentTimeMillis() - t + " ms to insert " + arr.size() + " records");

			// read
			t = System.currentTimeMillis();
			c.find().into(new ArrayList<Document>());
			System.out.println(System.currentTimeMillis() - t + " ms to fetch " + arr.size() + " records");

			// read
			t = System.currentTimeMillis();
			FindIterable<Document> found = c.find(Filters.eq("title", "The Hunger Games: Catching Fire"));
			System.out.println(System.currentTimeMillis() - t + " ms to fetch one particular record");

			// update
			t = System.currentTimeMillis();
			c.updateOne(Filters.eq("title", "The Hunger Games: Catching Fire"), new Document("$set", new Document("new attri", "say hello")));
			System.out.println(System.currentTimeMillis() - t + " ms to update one particular record");

			found = c.find(Filters.eq("title", "The Hunger Games: Catching Fire"));
			System.out.println("Updated record " + found.iterator().next().toJson());

			// map reduce
			final String map = "function () {" +
					"var directors=this.info.directors;"
					+ "if(directors==null) return;"
					+ "for(var i=0;i<directors.length;i++){"
					+ "emit(directors[i],1);}" +
					"}";

			final String reduce = "function (key, values) { " +
					" var total = 0; " +
					" values.forEach(function(v){total+=v;});  " +
					" return total; }";

			t = System.currentTimeMillis();
			for (final Document d : c.mapReduce(map, reduce).filter(Filters.eq("info.directors", "Steven Spielberg"))) {
				System.out.println("MapReduce Output- " + d.toJson());
			}
			System.out.println(System.currentTimeMillis() - t + " ms consumed in mapReduce");

			// delete
			t = System.currentTimeMillis();
			c.deleteOne(Filters.eq("title", "The Hunger Games: Catching Fire"));
			System.out.println(System.currentTimeMillis() - t + " ms to delete one records");

			t = System.currentTimeMillis();
			c.deleteMany(new Document());
			System.out.println(System.currentTimeMillis() - t + " ms to delete all records");

			c.drop();
		}

	}
}
