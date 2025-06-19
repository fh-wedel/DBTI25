package fhwedel.Mongo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import com.mongodb.client.AggregateIterable;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Sorts.*;
import java.util.Arrays;
import java.util.List;

public class CRUDclientTest {

    private static MongoClient mongoClient;
    private static MongoDatabase database;
    private static MongoCollection<Document> buecherCollection;
    private static MongoCollection<Document> leserCollection;
    private static MongoCollection<Document> entleihenCollection;

    @BeforeAll
    public static void setUp() {
        // Verbindung zur MongoDB herstellen
        mongoClient = MongoClients.create("mongodb://localhost:27017");
        database = mongoClient.getDatabase("bibliothek_test");

        // Collections erstellen/abrufen
        buecherCollection = database.getCollection("buecher");
        leserCollection = database.getCollection("leser");
        entleihenCollection = database.getCollection("entleihen");

        // Collections vor allen Tests leeren
        buecherCollection.drop();
        leserCollection.drop();
        entleihenCollection.drop();
    }

    @AfterAll
    public static void tearDown() {
        // Verbindung schließen
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @Test
    public void a() {
        // Insert eines einzelnen Buchs
        Document buch1 = new Document("INVNR", "123456")
                .append("Autor", "Marc-Uwe Kling")
                .append("Titel", "Die Känguru-Chroniken: Ansichten eines vorlauten Beuteltiers")
                .append("Verlag", "Ullstein-Verlag");
        buecherCollection.insertOne(buch1);
        System.out.println("Buch eingefügt: " + buch1.getString("Titel"));

        // Insert eines einzelnen Lesers
        Document leser1 = new Document("LNR", "1001")
                .append("Name", "Friedrich Funke")
                .append("Adresse", "Bahnhofstraße 17, 23758 Oldenburg");
        leserCollection.insertOne(leser1);
        System.out.println("Leser eingefügt: " + leser1.getString("Name"));

        // Insert mehrerer Bücher
        List<Document> buecher = Arrays.asList(
                new Document("INVNR", "123457")
                        .append("Autor", "George Orwell")
                        .append("Titel", "1984")
                        .append("Verlag", "Ullstein"),
                new Document("INVNR", "123458")
                        .append("Autor", "J.K. Rowling")
                        .append("Titel", "Harry Potter und der Stein der Weisen")
                        .append("Verlag", "Carlsen"),
                new Document("INVNR", "123459")
                        .append("Autor", "Frank Schätzing")
                        .append("Titel", "Der Schwarm")
                        .append("Verlag", "Fischer"),
                new Document("INVNR", "123460")
                        .append("Autor", "Hape Kerkeling")
                        .append("Titel", "Ich bin dann mal weg")
                        .append("Verlag", "Piper"),
                new Document("INVNR", "123461")
                        .append("Autor", "Stephen King")
                        .append("Titel", "Es")
                        .append("Verlag", "Heyne"));
        buecherCollection.insertMany(buecher);
        System.out.println("Mehrere Bücher eingefügt: " + buecher.size() + " Bücher");

        // Insert mehrerer Leser
        List<Document> leser = Arrays.asList(
                new Document("LNR", "1002")
                        .append("Name", "Anna Müller")
                        .append("Adresse", "Lindenweg 3, 10115 Berlin"),
                new Document("LNR", "1003")
                        .append("Name", "Ben Schröder")
                        .append("Adresse", "Am Hang 12, 80331 München"),
                new Document("LNR", "1004")
                        .append("Name", "Clara Becker")
                        .append("Adresse", "Marktplatz 5, 50667 Köln"),
                new Document("LNR", "1005")
                        .append("Name", "Daniel Schmidt")
                        .append("Adresse", "Hauptstraße 9, 20095 Hamburg"),
                new Document("LNR", "1006")
                        .append("Name", "Eva Lange")
                        .append("Adresse", "Bergstraße 14, 04109 Leipzig"));
        leserCollection.insertMany(leser);
        System.out.println("Mehrere Leser eingefügt: " + leser.size() + " Leser");

        // Insert von Entleihungen
        Document entleihe1 = new Document("LNR", "1001")
                .append("INVNR", "123457")
                .append("Rueckgabedatum", "2025-07-15");
        entleihenCollection.insertOne(entleihe1);

        Document entleihe2 = new Document("LNR", "1001")
                .append("INVNR", "123458")
                .append("Rueckgabedatum", "2025-07-15");
        entleihenCollection.insertOne(entleihe2);

        System.out.println("Entleihungen eingefügt");
        System.out.println("Alle Operationen aus Abschnitt 'a' erfolgreich ausgeführt!");
    }

    @Test
    public void b() {
        // Suche nach einem Buch
        Document gefundenesBuch = buecherCollection.find(new Document("Autor", "Marc-Uwe Kling")).first();
        System.out.println("Gefundenes Buch: " + gefundenesBuch.getString("Titel"));
        System.out.println("Alle Operationen aus Abschnitt 'b' erfolgreich ausgeführt!");
    }

    @Test
    public void c() {
        // Zähle alle Bücher
        long anzahlBuecher = buecherCollection.countDocuments();
        System.out.println("Anzahl der Bücher: " + anzahlBuecher);
        System.out.println("Alle Operationen aus Abschnitt 'c' erfolgreich ausgeführt!");
    }

    @Test
    public void d() {
        // Aggregation: Finde Leser mit mehr als einem entliehenen Buch
        AggregateIterable<Document> result = entleihenCollection.aggregate(Arrays.asList(
            group("$LNR", sum("anzahlBuecher", 1)),
            match(gt("anzahlBuecher", 1)),
            sort(descending("anzahlBuecher")),
            lookup("leser", "_id", "LNR", "leserInfo")
        ));
        
        System.out.println("Leser mit mehr als einem entliehenen Buch:");
        for (Document doc : result) {
            System.out.println("LNR: " + doc.getString("_id") + 
                               ", Anzahl Bücher: " + doc.getInteger("anzahlBuecher") + 
                               ", Leser Info: " + doc.get("leserInfo"));
        }
        
        System.out.println("Alle Operationen aus Abschnitt 'd' erfolgreich ausgeführt!");
    }

    @Test
    public void e() {       
        // Insert der Entleihe
        Document entleihe = new Document("LNR", "1001")
                .append("INVNR", "123456")
                .append("Rueckgabedatum", "2025-07-10");
        entleihenCollection.insertOne(entleihe);
        System.out.println("Entleihe eingefügt: LNR=" + entleihe.getString("LNR") + 
                          ", INVNR=" + entleihe.getString("INVNR"));
        
        // Delete der gleichen Entleihe
        Document filter = new Document("LNR", "1001")
                .append("INVNR", "123456");
        entleihenCollection.deleteOne(filter);
        System.out.println("Entleihe gelöscht: LNR=1001, INVNR=123456");
        
        System.out.println("Alle Operationen aus Abschnitt 'e' erfolgreich ausgeführt!");
    }

    @Test
    public void f() {
        // Erstelle eingebettetes Dokument für Entleihe
        Document eingebetteteEntleihe = new Document("Titel", "Der König von Berlin")
                .append("Autor", "Horst Evers")
                .append("Verlag", "Rowohlt-Verlag")
                .append("Rueckgabedatum", "2025-08-01");
        
        // Insert Leser mit eingebetteter Entleihungen-Array
        Document leser7 = new Document("LNR", "1007")
                .append("Name", "Heinz Müller")
                .append("Adresse", "Klopstockweg 17, 38124 Braunschweig")
                .append("entleihen", Arrays.asList(eingebetteteEntleihe));
        
        leserCollection.insertOne(leser7);
        System.out.println("Leser mit eingebetteten Entleihungen eingefügt: " + leser7.getString("Name"));
        System.out.println("Alle Operationen aus Abschnitt 'f' erfolgreich ausgeführt!");
    }

    @Test
    public void g() {
        // Pull Operation - entferne Buch aus der entleihen Array
        Document filter1 = new Document("LNR", "1007");
        Document update1 = new Document("$pull", 
                new Document("entleihen", 
                        new Document("Titel", "Der König von Berlin")));
        
        leserCollection.updateOne(filter1, update1);
        System.out.println("Buch aus Entleihungen entfernt: LNR=1007, Titel=Der König von Berlin");
        
        // Push Operation - füge Buch zur entleihen Array hinzu
        Document filter2 = new Document("LNR", "1001");
        Document buch1 = new Document("Titel", "Der König von Berlin")
                .append("Autor", "Horst Evers")
                .append("Verlag", "Rowohlt-Verlag")
                .append("Rueckgabedatum", "2025-08-01");
        
        Document update2 = new Document("$push", 
                new Document("entleihen", buch1));
        
        leserCollection.updateOne(filter2, update2);
        System.out.println("Buch zu Entleihungen hinzugefügt: LNR=1001, Titel=Der König von Berlin");
        System.out.println("Alle Operationen aus Abschnitt 'g' erfolgreich ausgeführt!");
    }
}
