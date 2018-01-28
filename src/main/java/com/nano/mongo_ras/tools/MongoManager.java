package com.nano.mongo_ras.tools;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;

@ApplicationScoped
public class MongoManager {
	
	private MongoClient mongoClient ;
	
	@Inject
	private PropertiesManager props ;
	
	@PostConstruct
	public void run() {
		
		MongoCredential mongoCredential = MongoCredential.createCredential(
				props.getProperty("mongodb.username", "nano"), 
				props.getProperty("mongodb.connection.db", "admin"), 
				props.getProperty("mongodb.password", "n@n0#").toCharArray());
		
		ServerAddress serverAddress = new ServerAddress(props.getProperty("mongodb.host.uri", "localhost"), props.getInt("mongodb.host.port", 28019));
		
		mongoClient = new MongoClient(serverAddress, Arrays.asList(mongoCredential));
	}
	
	@PreDestroy
	public void stop() {
		mongoClient.close();
	}
	
	public MongoClient getMongoClient() {
		return mongoClient;
	}
	
	/**
	 * Grants additional roles to a user.
	 * 
	 * @param user userName of mongoDB user
	 * @param role role to be assigned to user
	 * @param db database on which user should have assigned role
	 */
	public void granRoleToUser(String user, 
			String role, String db){
		
		Bson command = new Document("grantRolesToUser", user)
				.append("roles", Arrays.asList(new Document("role", role).append("db", db)));
		
		mongoClient.getDatabase("admin").runCommand(command);
	}
	
	/**
	 * Get MongoDatabase connection for application database to use in querying collections.
	 * 
	 * @return {@link MongoDatabase} object
	 */
	public MongoDatabase getDatabaseConnection(){
		
		String user = props.getProperty("mongodb.username", "nano");
		String role = props.getProperty("mongod.role.owner", "dbOwner");
		String appdb = props.getProperty("monogo.app.db", "nano_db");
		
		granRoleToUser(user, role, appdb);
		return mongoClient.getDatabase(appdb);
	}
	
	/**
	 * Get MongoDatabase connection for specified database to use in querying collections.
	 * 
	 * @param databaseName database name for connection is required
	 * @return {@link MongoDatabase} object
	 */
	public MongoDatabase getDatabaseConnection(String databaseName){
		
		return mongoClient.getDatabase(databaseName);
	}
	
	/**
	 * Get MongoCOllection connection for specified mongoDatabase to use in querying documents.
	 * 
	 * @param mongoDatabase {@link MongoDatabase} record
	 * @param collectionName name of mongoDB collection
	 * @return {@link MongoCollection} record
	 */
	public MongoCollection<Document> getCollectionConnection(MongoDatabase mongoDatabase, 
			String collectionName){
		
		return mongoDatabase.getCollection(collectionName);
	}
	
	/**
	 * Get MongoCOllection connection for application DB to use in querying documents.
	 * 
	 * @param collectionName name of applicationDB collection
	 * @return {@link MongoCollection} record
	 */
	public MongoCollection<Document> getCollectionConnection(String collectionName){
		
		return getDatabaseConnection().getCollection(collectionName);
	}
	
	/**
	 * Update MongoDocument by filter.
	 * 
	 * @param mongoCollection {@link MongoCollection} record
	 * @param document original copy of document to be updated
	 * @param modifications BSON containing changes to effected in document
	 */
	public void updateDocument(MongoCollection<Document> mongoCollection, 
			Document document, Bson modifications){
		
		Bson filter = new Document("_id", document.getObjectId("_id"));
		Bson update = new Document("$set", modifications);
		
		mongoCollection.updateOne(filter, update);
	}
	
	/**
	 * Update multiple MongoDocument by filter.
	 * 
	 * @param mongoCollection {@link MongoCollection} record
	 * @param document original copy of document to be updated
	 * @param modifications BSON containing changes to effected in document
	 */
	public void updateManyDocuments(MongoCollection<Document> mongoCollection, 
			Document document, Bson modifications){
		
		Bson filter = new Document("_id", document.getObjectId("_id"));
		Bson update = new Document("$set", modifications);
		
		mongoCollection.updateMany(filter, update);
	}
	
	/**
	 * Add UNIQUE constraint to fields in collection.
	 * @see https://docs.mongodb.com/manual/core/index-unique/
	 * 
	 * @param mongoCollection {@link MongoCollection} record
	 * @param fields collection fields to be indexed
	 */
	public void createUniqueConstraint(MongoCollection<Document> mongoCollection, 
			String...fields){
		
		List<IndexModel> indexes = new LinkedList<>();
		
		for (String field : fields){
			
			IndexModel indexModel = new IndexModel(Indexes.ascending(field), new IndexOptions().unique(true).name(field.toUpperCase() + "_UNIQUE"));
			indexes.add(indexModel);
		}
		
		mongoCollection.createIndexes(indexes);
	}
	
	/**
	 * MongoDB provides text indexes to support text search of string content. 
	 * Text indexes can include any field whose value is a string or an array of string elements. 
	 * A compound index can be created incorporating a text index 
	 * but it’s important to note there can only be one text index on a collection.
	 * @see http://mongodb.github.io/morphia/1.3/guides/indexing/
	 * 
	 * @param mongoCollection {@link MongoCollection} record
	 * @param fields collection fields to be indexed
	 */
	public void createTextIndex(MongoCollection<Document> mongoCollection, 
			String field){
		
		mongoCollection.createIndex(Indexes.text(field), new IndexOptions().name(field.toUpperCase() + "_TEXT"));
	}
	
	/**
	 * Create indexes on multiple fields within a collection.
	 * @see https://docs.mongodb.com/manual/core/index-compound/
	 * 
	 * @param mongoCollection MongoDB collection
	 * @param fields fields to be indexed
	 */
	public void createCompoundIndex(MongoCollection<Document> mongoCollection, String...fields) {
		
		mongoCollection.createIndex(Indexes.compoundIndex(Indexes.ascending(fields)));
	}

}