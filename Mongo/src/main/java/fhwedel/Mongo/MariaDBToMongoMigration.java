package fhwedel.Mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Liest die MariaDB-Datenbank "firma" aus und legt sie
 * in MongoDB in einer hybriden Struktur ab:
 * - employees: Hauptcollection mit eingebetteten Kindern, Prämien und Maschinen
 * - salary_scales: separate Collection für Gehaltsstufen
 * - departments: separate Collection für Abteilungen
 *
 */
public class MariaDBToMongoMigration {
    public static void main(String[] args) {
        // Verbindungen aufbauen
        try (Connection sqlCon = connectMariaDB();
                MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017")) {

            MongoDatabase mongoDb = mongoClient.getDatabase("firma_mongo");
            MongoCollection<Document> deptColl = mongoDb.getCollection("departments");
            MongoCollection<Document> scaleColl = mongoDb.getCollection("salary_scales");
            MongoCollection<Document> empColl = mongoDb.getCollection("employees");

            // Collections leeren
            deptColl.drop();
            scaleColl.drop();
            empColl.drop();

            // Migration
            migrateDepartments(sqlCon, deptColl);
            migrateSalaryScales(sqlCon, scaleColl);
            migrateEmployees(sqlCon, empColl);

            System.out.println("Migration abgeschlossen!");
        } catch (SQLException e) {
            System.err.println("SQL-Fehler: " + e.getMessage());
        }
    }

    /**
     * Stellt eine Verbindung zur MariaDB-Datenbank her.
     * 
     * @return Connection-Objekt zur MariaDB-Datenbank "firma"
     * @throws SQLException wenn die Verbindung zur Datenbank fehlschlägt
     */
    private static Connection connectMariaDB() throws SQLException {
        System.out.println("Verbinde zu MariaDB...");
        return DriverManager.getConnection(
                "jdbc:mariadb://localhost:3306/firma", "root", "password");
    }

    /**
     * Liest alle Abteilungen aus der MariaDB-Tabelle "abteilung" aus und
     * fügt sie als Dokumente in die angegebene MongoDB-Collection ein. Jedes
     * Dokument
     * enthält die Abteilungsnummer (deptId) und den Namen der Abteilung.
     * 
     * @param con  die Datenbankverbindung zur MariaDB
     * @param coll die MongoDB-Collection, in die die Abteilungen eingefügt werden
     *             sollen
     * @throws SQLException wenn ein Fehler beim Zugriff auf die MariaDB auftritt
     */
    private static void migrateDepartments(Connection con, MongoCollection<Document> coll) throws SQLException {
        System.out.println("Migration: Abteilungen...");
        String sql = "SELECT abt_nr, name FROM abteilung";
        try (PreparedStatement ps = con.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Document doc = new Document("deptId", rs.getString("abt_nr"))
                        .append("name", rs.getString("name"));
                coll.insertOne(doc);
            }
            System.out.println("Abteilungen migriert.");
        }
    }

    /**
     * Liest alle Gehaltsstufen aus der MariaDB-Tabelle "gehalt" aus
     * und überträgt sie in die angegebene MongoDB-Collection. Jeder Datensatz wird
     * als Document mit den Feldern "scale" (Gehaltsstufe) und "amount" (Betrag)
     * gespeichert.
     * 
     * @param con  die aktive Datenbankverbindung zur MariaDB
     * @param coll die MongoDB-Collection, in die die Gehaltsstufen eingefügt werden
     *             sollen
     * @throws SQLException wenn ein Fehler beim Zugriff auf die MariaDB auftritt
     */
    private static void migrateSalaryScales(Connection con, MongoCollection<Document> coll) throws SQLException {
        System.out.println("Migration: Gehaltsstufen...");
        String sql = "SELECT geh_stufe, betrag FROM gehalt";
        try (PreparedStatement ps = con.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Document doc = new Document("scale", rs.getString("geh_stufe"))
                        .append("amount", rs.getDouble("betrag"));
                coll.insertOne(doc);
            }
            System.out.println("Gehaltsstufen migriert.");
        }
    }

    /**
     * Liest alle Mitarbeiter aus der MariaDB-Tabelle 'personal' und
     * migriert sie in eine MongoDB-Collection. Dabei werden auch die zugehörigen
     * Kinder, Prämien und Maschinen als eingebettete Dokumente übertragen.
     * 
     * Für jeden Mitarbeiter werden folgende Daten migriert:
     * - Grunddaten: Personalnummer, Name, Vorname, Gehaltsstufe, Abteilungsnummer,
     * Krankenkasse
     * - Kinder: Name, Vorname und Geburtsjahr aus der Tabelle 'kind'
     * - Prämien: Beträge aus der Tabelle 'praemie'
     * - Maschinen: Maschinennummer, Name, Anschaffungsdatum, Neuwert und Zeitwert
     * aus der Tabelle 'maschine'
     * 
     * @param con  Die Datenbankverbindung zur MariaDB
     * @param coll Die MongoDB-Collection, in die die Mitarbeiterdaten eingefügt
     *             werden
     * @throws SQLException Wenn ein Datenbankfehler beim Lesen der MariaDB-Daten
     *                      auftritt
     */
    private static void migrateEmployees(Connection con, MongoCollection<Document> coll) throws SQLException {
        System.out.println("Migration: Mitarbeiter...");
        String sql = "SELECT pnr, name, vorname, geh_stufe, abt_nr, krankenkasse FROM personal";
        try (PreparedStatement ps = con.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String pnr = rs.getString("pnr");
                Document emp = new Document("pnr", pnr)
                        .append("name", rs.getString("name"))
                        .append("vorname", rs.getString("vorname"))
                        .append("gehStufe", rs.getString("geh_stufe"))
                        .append("deptId", rs.getString("abt_nr"))
                        .append("krankenkasse", rs.getString("krankenkasse"));

                // Kinder einbetten
                List<Document> children = new ArrayList<>();
                try (PreparedStatement psChild = con.prepareStatement(
                        "SELECT k_name, k_vorname, k_geb FROM kind WHERE pnr = ?")) {
                    psChild.setString(1, pnr);
                    try (ResultSet rsChild = psChild.executeQuery()) {
                        while (rsChild.next()) {
                            Document child = new Document("lastName", rsChild.getString("k_name"))
                                    .append("firstName", rsChild.getString("k_vorname"))
                                    .append("birthYear", rsChild.getInt("k_geb"));
                            children.add(child);
                        }
                    }
                }
                emp.append("children", children);

                // Prämien einbetten
                List<Document> bonuses = new ArrayList<>();
                try (PreparedStatement psBonus = con.prepareStatement(
                        "SELECT p_betrag FROM praemie WHERE pnr = ?")) {
                    psBonus.setString(1, pnr);
                    try (ResultSet rsBonus = psBonus.executeQuery()) {
                        while (rsBonus.next()) {
                            Document bonus = new Document("amount", rsBonus.getInt("p_betrag"));
                            bonuses.add(bonus);
                        }
                    }
                }
                emp.append("bonuses", bonuses);

                // Maschinen einbetten
                List<Document> machines = new ArrayList<>();
                try (PreparedStatement psMachine = con.prepareStatement(
                        "SELECT mnr, name, ansch_datum, neuwert, zeitwert FROM maschine WHERE pnr = ?")) {
                    psMachine.setString(1, pnr);
                    try (ResultSet rsMachine = psMachine.executeQuery()) {
                        while (rsMachine.next()) {
                            Document machine = new Document("machineId", rsMachine.getInt("mnr"))
                                    .append("name", rsMachine.getString("name"))
                                    .append("purchaseDate", rsMachine.getDate("ansch_datum").toString())
                                    .append("newValue", rsMachine.getInt("neuwert"))
                                    .append("currentValue", rsMachine.getInt("zeitwert"));
                            machines.add(machine);
                        }
                    }
                }
                emp.append("machines", machines);

                coll.insertOne(emp);
            }
            System.out.println("Mitarbeiter migriert.");
        }
    }
}
