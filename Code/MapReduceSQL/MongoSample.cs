using System;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

/**
 * 
 * This file is a created to show you how to connect to MongoDB server v3.2 using driver 2.4
 * There are some instruction uncommented. You try out all this uncommented line to get the 
 * 
 */

namespace MapReduceSQL
{
	public class MongoSample
	{

		public static async void testMongo ()
		{

			//Create a client and collect tot database
			var _client = new MongoClient ();

			//replace "test" with your own database
			var _database = _client.GetDatabase ("test");

			//Get a collection to prcess
			//replace the collection name with your own
			var actors = _database.GetCollection<BsonDocument> ("actors");



			//Create filters to select specific code

			//select * filter 
			//var filter = new BsonDocument ();

			//select * from actors where name = equal to "actor name"
			var anotherFilter = Builders<BsonDocument>.Filter.Eq ("name", "actor name");

			//Select from acotrs collection based on the filer rule
			//var result = actors.Find (anotherFilter).ToList ();


			//Select from acotrs collection based on the filer rule and get the cursor from the server
			//var cursor = actors.Find (new BsonDocument ()).ToCursor();

			//Iterate throu result set
			//foreach (var document in cursor.ToEnumerable ()) {
			//	Console.WriteLine (document);
			//}



			//Mapper function for the collection
			BsonJavaScript actormap = "function() { " +
							"emit(this.actor_id, {'name' : this.name});}";


			//Reduce function which does nothing than returning the result 
			BsonJavaScript reduce = "function(key, values) { " +
							"values.forEach(" +
								"function(value) {" +
									"return value;" +
								"});" +
							"}";

			//Options for the result of the output
			var options = new MapReduceOptions<BsonDocument, BsonDocument> ();
			options.OutputOptions = MapReduceOutputOptions.Inline;

			//Excute map and reduce functions 
			var resultMR = actors.MapReduce (actormap,reduce, options);

			//print first result only  
			Console.WriteLine(resultMR.First ());

		}
	}
}
