import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

/**
 * class to get started with cassandra
 *
 * @author Purnendu Rath (puru_cr7).
 *         Created Oct 2, 2018.
 */
public class PerformanceCassandra {

	@SuppressWarnings("unchecked")
	public static void main(final String[] args) throws FileNotFoundException, IOException, ParseException {

		try (Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(9042).build()) {

			// CREATE keyspace
			cluster.connect().execute("CREATE KEYSPACE IF NOT EXISTS perfks WITH replication "
					+ "= {'class':'SimpleStrategy', 'replication_factor':3};");

			// connect to the KEYSPACE
			final Session session = cluster.connect("perfks");
			session.execute("DROP TABLE IF EXISTS perfks.movies");
			session.execute("DROP TYPE IF EXISTS perfks.infoz");

			// CREATE UDT
			final String info = "CREATE TYPE perfks.infoz (directors list<text>, release_date varchar, rating decimal, genres list<text>, image_url varchar, plot varchar, rank int, running_time_secs int, actors list<text> )";
			session.execute(info);
			// CREATE Table or Column family
			final String moviesCreate = "CREATE TABLE perfks.movies (title varchar PRIMARY KEY, year int, info FROZEN<infoz>) ";
			session.execute(moviesCreate);

			final JSONParser jp = new JSONParser();
			final JSONArray arr = (JSONArray) jp
					.parse(new FileReader(new File("E:\\Programming\\WS\\\\Getting_started_with_NOSQL\\src\\main\\resources\\moviedata.json")));

			final List<String> insData = new ArrayList<>();
			for (final Object obj : arr) {
				insData.add("INSERT INTO perfks.movies JSON '" + ((JSONObject) obj).toJSONString().replaceAll("'", "''") + "'");
			}
			// insert
			long t = System.currentTimeMillis();
			for (final String ins : insData) {
				session.execute(ins);
			}
			System.out.println(System.currentTimeMillis() - t + " ms to insert " + arr.size() + " records");

			// read
			t = System.currentTimeMillis();
			ResultSet movieResults = session.execute("SELECT title,info.rating from perfks.movies");
			movieResults.all();
			System.out.println(System.currentTimeMillis() - t + " ms to fetch " + arr.size() + " records");

			// update
			t = System.currentTimeMillis();
			session.execute("UPDATE perfks.movies SET year=? WHERE title=?", 2018,
					"The Hunger Games: Catching Fire");
			System.out.println(System.currentTimeMillis() - t + " ms to update one particular record");

			// read
			t = System.currentTimeMillis();
			movieResults = session.execute("SELECT info.rank from perfks.movies WHERE title=?", "The Hunger Games: Catching Fire");
			movieResults.one().getInt("info.rank");
			System.out.println(System.currentTimeMillis() - t + " ms to fetch one particular record");

			// delete
			t = System.currentTimeMillis();
			session.execute("DELETE FROM perfks.movies WHERE title=?", "The Hunger Games: Catching Fire");
			System.out.println(System.currentTimeMillis() - t + " ms to delete one records");

			t = System.currentTimeMillis();
			session.execute("TRUNCATE perfks.movies");
			System.out.println(System.currentTimeMillis() - t + " ms to delete all records");

			// DROP TABLE AND KEYSPACE
			session.execute("DROP TABLE IF EXISTS perfks.movies");
			session.execute("DROP KEYSPACE IF EXISTS perfks");
		}
	}

}
