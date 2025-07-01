package fhwedel.Mongo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;
import java.util.List;

public class MariaDBToMongoMigrationTest {

    private static MongoClient mongoClient;
    private static MongoDatabase database;
    private static MongoCollection<Document> personalCollection;
    private static MongoCollection<Document> gehaltCollection;

    @BeforeAll
    public static void setUp() {
        // Verbindung zur MongoDB herstellen
        mongoClient = MongoClients.create("mongodb://localhost:27017");
        database = mongoClient.getDatabase("firma_mongo");

        // Collections erstellen/abrufen
        personalCollection = database.getCollection("employees");
        gehaltCollection = database.getCollection("salary_scales");

        // Collections vor allen Tests leeren
        personalCollection.drop();
        gehaltCollection.drop();

        // Migration ausführen
        MariaDBToMongoMigration.main(new String[]{});
    }

    @AfterAll
    public static void tearDown() {
        // Verbindung schließen
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @Test
    public void testCreate() {
        // (a) Create: Neuer Mitarbeiter
        Document newEmployee = new Document("pnr", 417)
                .append("name", "Krause")
                .append("vorname", "Henrik")
                .append("geh_stufe", "it1")
                .append("abt_nr", "d13")
                .append("krankenkasse", "tkk");
        personalCollection.insertOne(newEmployee);
        System.out.println("Neuer Mitarbeiter eingefügt: " + newEmployee.toJson());
    }

    @Test
    public void testRead() {
        // (b) Read: Alle Dokumente anzeigen
        List<Document> employees = personalCollection.find().into(new java.util.ArrayList<>());
        System.out.println("Alle Mitarbeiter:");
        for (Document employee : employees) {
            System.out.println(employee.toJson());
        }
    }

    @Test
    public void testUpdate() {
        // (c) Update: Gehalt um 10% erhöhen
        gehaltCollection.updateOne(eq("scale", "it1"), inc("amount", 252));
        System.out.println("Gehalt für Gehaltsstufe it1 um 10% erhöht.");
    }

    @Test
    public void testDelete() {
        // (d) Delete: Mitarbeiter Lutz Tietze löschen
        personalCollection.deleteOne(and(eq("name", "Tietze"), eq("vorname", "Lutz")));
        System.out.println("Mitarbeiter Lutz Tietze gelöscht.");
    }

    @Test
    public void testQuery() {
        // (e) Frage: MitarbeiterInnen in der Abteilung Verkauf
        List<Document> employeesInSales = personalCollection.find(eq("abt_nr", "d15")).into(new java.util.ArrayList<>());
        System.out.println("MitarbeiterInnen in der Abteilung Verkauf:");
        for (Document employee : employeesInSales) {
            System.out.println(employee.getString("vorname") + " " + employee.getString("name"));
        }
    }
}
