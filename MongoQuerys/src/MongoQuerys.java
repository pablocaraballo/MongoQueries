import java.util.Arrays;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;

public class MongoQuerys {

	public static void main(String[] args) {
		
		try{
			//ConexiÃ³n con la DB edx
			MongoClient mongoClient= new MongoClient("15.83.18.27",27017);
			MongoDatabase db=mongoClient.getDatabase("edx");
			System.out.println("ConexiÃ³n establecida");
			
			
			moreOneAuthor(db);
			System.out.println("\n\n");
			studentAgend(db);
			System.out.println("\n\n");
			topAwards(db);
			
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	//BUSCA LOS LIBROS ESCRITOS POR MÃ�S DE UN AUTOR
	public static void moreOneAuthor(MongoDatabase db){
		FindIterable<Document> query=db.getCollection("books")
				.find(new Document("author",new Document("$exists",true).append("$not",new Document("$size",1))))
				.projection(new Document("title",true).append("author",true).append("_id", false));
		
		System.out.println("Lista de libros escritos por mÃ¡s de un autor");
		for(Document d: query){
			System.out.println(d);
		}
	}
	
	//MUESTRA UNA AGENDA DE TODOS LOS ESTUDIANTES DE 26 AÃ‘OS O MAYORES
	public static void studentAgend(MongoDatabase db){
		FindIterable<Document> query2=db.getCollection("students")
				.find(new Document("birth_year",new Document("$gte",1993)))
				.projection(new Document("name",true).append("lastname1",true).append("_id", false).append("email",true).append("phone",true));
		
		System.out.println("Agenda de los estudiantes que han nacido en 1993 o posterior.");
		for(Document d2: query2){
			System.out.println(d2);
		}
	}
	
	//CUENTA LOS PREMIOS QUE TIENE CADA PERSONA Y LOS ORDENA DE MAYOR A MENOR
	public static void topAwards(MongoDatabase db){
		AggregateIterable<Document> query3=db.getCollection("bios")
				.aggregate(Arrays.asList(
						new Document("$unwind","$awards"),
						new Document("$group", new Document("_id", new Document("name","$name.first").append("lastname", "$name.last")).append("premios", new Document("$sum",1))),
						new Document("$sort", new Document("premios",-1)),
						new Document("$project", new Document("_id","$_id").append("Total_Premios", "$premios"))
						));
		
		System.out.println("Ranking de premios recibidos");
		for(Document d3: query3){
			System.out.println(d3);
		}
	}
	
	//Mongo Query
	/*db.bios.aggregate([
	        		   {"$unwind": "$awards"},
	                           { $group: { _id:{ "name":"$name.first",
	        				     "lastname":"$name.last"}, premios:{"$sum": 1 } } },
	                           { $project: {_id:true, "Total_Premios":"$premios"}}])*/
	
	
	


}
