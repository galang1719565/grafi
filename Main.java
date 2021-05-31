package gebd.grafi;

import java.util.ArrayList;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;

public class Main {
	
	
	public static void main(String[] args) {
		ArrayList<Persona> persone = new ArrayList<>();
		persone.add(new Persona("a", "Alice", 34));
		persone.add(new Persona("b", "Bob", 36));
		persone.add(new Persona("c", "Charlie", 30));
		persone.add(new Persona("d", "David", 29));
		persone.add(new Persona("e", "Esther", 32));
		persone.add(new Persona("f", "Fanny", 36));
		
		System.setProperty("hadoop.home.dir", "C:\\Users\\Ferdinand Galang\\Downloads");

		SparkSession spark = SparkSession.
				builder().
				master("local[*]").
				appName("Test grafi").
				getOrCreate();
		
		spark.sparkContext().setLogLevel("ERROR");
		
		Dataset<Row> vertici = spark.createDataFrame(persone, Persona.class);
		
		ArrayList<Relazione> relazioni = new ArrayList<>();
		relazioni.add(new Relazione("a", "e", "friend"));
		relazioni.add(new Relazione("f", "b", "follow"));
		relazioni.add(new Relazione("c", "e", "friend"));
		relazioni.add(new Relazione("a", "b", "friend"));
		relazioni.add(new Relazione("b", "c", "follow"));
		relazioni.add(new Relazione("c", "b", "follow"));
		relazioni.add(new Relazione("f", "c", "follow"));
		relazioni.add(new Relazione("e", "f", "follow"));
		relazioni.add(new Relazione("e", "d", "friend"));
		relazioni.add(new Relazione("d", "a", "friend"));
		
		Dataset<Row> archi = spark.createDataFrame(relazioni, Relazione.class);
		
		GraphFrame grafo = new GraphFrame(vertici, archi);
		
		spark.sparkContext().setCheckpointDir("checkpoint_grafi");
		// directory di checkpoint: necessario per gli algoritmi
		// ---> salvataggio dello stato progressivo di questi, per eludere eventuali errori
		// *** ---> ERRORE NEL SALVATAGGIO: (null) entry in command string: null chmod 0644 ***
		
		System.out.println("\n\nGradi dei nodi:\n");
		grafo.degrees().show();
		// OSS: metodo show() ritorna un altro DataFrame
		// grafo.inDegrees().show();
		System.out.println("\n\nComponenti connesse:\n");
		// Algoritmo, ***MOLTO ONEROSO***
		// 				---> metodo run()
		grafo.connectedComponents().run().show();
		System.out.println("se vabbè, lasciamo perde\n");
		
		GraphFrame amicizie = grafo.filterEdges("relationship = 'friend'");
		System.out.println("\n\nAmicizie:\n");
		amicizie.edges().show();
		// amicizie.connectedComponents().run().show();
		
		// metodo find():
		// input - string/pattern; output - DataFrame con tutti gli oggetti richiamati 
		System.out.println("\n\nRelazioni circolari:\n");
		grafo.find("(src)-[e]->(dst); (dst)-[e2]->(src)").show();
		
		System.out.println("\n\nBFS - visita:\n");
		// 					trova nodo A e cerca SOLO il primo nodo B che soddisfa la query
		grafo.bfs().fromExpr("age > 30").toExpr("age < 31").run().show();
		
		ArrayList<Object> id_landmarks = new ArrayList<>();
		id_landmarks.add("f");
		id_landmarks.add("c");
		System.out.println("\n\nShortest path:\n");
		grafo.shortestPaths().landmarks(id_landmarks).run().show();
		
//		System.out.println("DAJE");
		
		
		
		
		
		
		
				
	}
}
