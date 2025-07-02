package fhwedel.Mongo;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import org.bson.Document;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CRUDClient {

    public static void main(String[] args) {
        // Verbindung zu MongoDB herstellen
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");

        // Datenbank auswählen
        MongoDatabase db = mongoClient.getDatabase("bibliothek");

        // Collections definieren
        MongoCollection<Document> books = db.getCollection("buch");
        MongoCollection<Document> readers = db.getCollection("leser");
        MongoCollection<Document> loans = db.getCollection("entliehen");

        // Buch einfügen
        books.insertOne(new Document("invr", 1)
                .append("autor", "Marc-Uwe Kling")
                .append("titel", "Die Känguru-Chroniken: Ansichten eines vorlauten Beuteltiers")
                .append("verlag", "Ulstein-Verlag"));

        // Leser einfügen
        readers.insertOne(new Document("lnr", 1)
                .append("name", "Friedrich Funke")
                .append("adresse", "Bahnhofstraße 17, 23758 Oldenburg"));

        // Mehrere Leser einfügen
        List<Document> readerDocs = Arrays.asList(
                new Document(Map.of("lnr", 2, "name", "Marty Menge", "adresse", "geheim")),
                new Document(Map.of("lnr", 3, "name", "Max Mustermann", "adresse", "12345 Berlin, Große Straße 1")),
                new Document(Map.of("lnr", 4, "name", "Bob der Baumeister", "adresse", "in Bobs Welt")),
                new Document(Map.of("lnr", 5, "name", "Hildegard Müller", "adresse", "überall")),
                new Document(Map.of("lnr", 6, "name", "Naruto Uzumaki", "adresse", "Konoha"))
        );
        readers.insertMany(readerDocs);

        // Mehrere Bücher einfügen
        List<Document> booksDocs = Arrays.asList(
                new Document(Map.of("invr", 2, "autor", "Hajime Isayama", "titel", "Attack on Titan", "verlag", "Kōdansha")),
                new Document(Map.of("invr", 3, "autor", "Max Mustermann", "titel", "Buch", "verlag", "Verlag")),
                new Document(Map.of("invr", 4, "autor", "Hendrik Hengst", "titel", "haarige Hühner", "verlag", "Verlag")),
                new Document(Map.of("invr", 5, "autor", "Großer Gerd", "titel", "Gerds Autobiografie", "verlag", "Verlag")),
                new Document(Map.of("invr", 6, "autor", "Turbo Torsten", "titel", "Turbo Traktor", "verlag", "Turbo Verlag"))
        );
        books.insertMany(booksDocs);

        // Suche nach einem Buch
        FindIterable<Document> result = books.find(Filters.eq("autor", "Marc-Uwe Kling"));
        for (Document doc : result) {
            System.out.println(doc.toJson());
        }

        // Cleanup (löschen für sauberen nächsten Lauf)
        books.drop();
        readers.drop();
        loans.drop();

        // Verbindung schließen
        mongoClient.close();
    }
}