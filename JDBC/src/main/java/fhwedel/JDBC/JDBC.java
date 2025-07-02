package fhwedel.JDBC;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Diese Klasse stellt die Verbindung zu der Datenbank "firma" her und führt
 * verschiedene SQL-Operationen aus, die Auf dem Aufgabenblatt beschrieben sind.
 * Zuletzt wird die Schema Migration durchgeführt.
 * 
 * @author Leon Lilje
 */
public class JDBC {
    /**
     * Main-Methode, die die Verbindung zur Datenbank herstellt und die
     * SQL-Operationen
     * ausführt.
     * 
     * @param args Kommandozeilenargumente (werden nicht verwendet).
     * @throws SQLException Fehler, der bei der Ausführung der SQL-Operationen
     *                      entsteht.
     */
    public static void main(String[] args) throws SQLException {
        // 1. Connection
        Connection con = connect();
        Statement st = con.createStatement();

        // 2. CREATE
        try {
            st.executeQuery("INSERT INTO personal VALUES (417, 'Krause', 'Henrik', 'it1', 'd13', 'tkk');");
        } catch (SQLException e) {
            System.out.println("Fehler bei Aufgabe 2. Vermutlich wurde der Nutzer bereits angelegt.");
        }

        // 3. READ
        ResultSet rs3 = st.executeQuery("select * from personal");
        printRS(rs3);

        // 4. UPDATE
        st.executeQuery("UPDATE gehalt SET Betrag=2523*1.1 WHERE Geh_Stufe='it1';");

        // 5. DELETE
        st.executeQuery("DELETE FROM personal WHERE name='Tietze' AND vorname='Lutz';");

        // 6. Frage
        System.out.println("Welche MitarbeiterInnen arbeiten in der Abteilung Verkauf?");
        ResultSet rs = st.executeQuery("SELECT * FROM personal WHERE Abt_Nr='d15';");
        printAnswer(rs);

        // Schema Migration
        try {
            schemaMigration(st);

        } catch (SQLException e) {
        }
        System.out.println("Fehler bei der Schema Migration. Vermutlich wurde sie bereits durchgeführt.");

    }

    /**
     * Baut eine Verbindung zu dem lokalen mariadb-Server auf und verbindet sich mit
     * der Datenbank "firma".
     * 
     * @return Verbindungsobjekt
     * @throws SQLException Fehler, der beim Verbindungsaufbau entsteht.
     */
    private static Connection connect() throws SQLException {
        return DriverManager.getConnection("jdbc:mariadb://localhost:3306/firma", "root", "password");

    }

    /**
     * Gibt ein ResultSet als Tabelle auf stdout aus.
     * 
     * @param rs ResultSet mit der Tabelle, die ausgegeben werden soll.
     * @throws SQLException Fehler, der beim Lesen der Daten entsteht.
     */
    private static void printRS(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            System.out.print(metaData.getColumnName(i) + ", ");
        }
        System.out.println("");

        while (rs.next()) {
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(rs.getString(i) + ", ");
            }
            System.out.println("");
        }
    }

    /**
     * Gibt die Antwort auf die in Aufgabe 6 gestellte Frage auf stdout aus.
     * 
     * @param rs ResultSet, welches die Antwort auf die Frage enthält.
     * @throws SQLException Fehler, der beim Lesen der Daten entsteht.
     */
    private static void printAnswer(ResultSet rs) throws SQLException {
        System.out.print("In der Abteilung arbeiten die Mitarbeiter ");
        while (rs.next()) {
            System.out.print(rs.getString(3) + " " + rs.getString(2) + ", ");
        }
        System.out.println();
    }

    /**
     * Führt die Schema Migration mit Einführung der neuen Krankenkasse Tabelle ein.
     * 
     * @param st Statement mit der Verbindung zur Tabelle
     * @throws SQLException Fehler, der bei der Migration enteht.
     */
    private static void schemaMigration(Statement st) throws SQLException {
        // 1. Einführen des neuen Schemas/der Änderungen
        st.executeQuery(
                "CREATE table krankenversicherung (kkid integer(4), kuerzel char(3), name VARCHAR(255), CONSTRAINT Schluessel_Krankenversicherung PRIMARY KEY (kkid));");
        st.executeQuery(
                "CREATE table personal_neu (pnr integer(4), name char(20) not null, vorname char(20), geh_stufe varchar(4), abt_nr char(3), krankenkasse integer(4), CONSTRAINT Schluessel_Personal PRIMARY KEY (pnr));");

        // 2. Lesen der Daten gemäß altem Schema
        st.execute(
                "INSERT INTO personal_neu (pnr, name, vorname, geh_stufe, abt_nr, krankenkasse) SELECT pnr, name, vorname, geh_stufe, abt_nr, NULL FROM personal;");

        // 3. Konvertieren, ergänzen, abändern der Daten
        st.executeQuery("INSERT INTO krankenversicherung VALUES (1, 'aok', 'Allgemeine Ortskrankenkasse')");
        st.executeQuery("INSERT INTO krankenversicherung VALUES (2, 'bak', 'Betriebskrankenkasse B. Braun Aesculap')");
        st.executeQuery("INSERT INTO krankenversicherung VALUES (3, 'bek', 'Barmer Ersatzkasse')");
        st.executeQuery("INSERT INTO krankenversicherung VALUES (4, 'dak', 'Deutsche Angestelltenkrankenkasse')");
        st.executeQuery("INSERT INTO krankenversicherung VALUES (5, 'tkk', 'Techniker Krankenkasse')");
        st.executeQuery("INSERT INTO krankenversicherung VALUES (6, 'kkh', 'Kaufmännische Krankenkasse')");

        // 4. Schreiben der Daten gemäß neuem Schema
        st.executeUpdate(
                "UPDATE personal_neu pn JOIN personal p ON pn.pnr = p.pnr JOIN krankenversicherung kk ON p.krankenkasse = kk.kuerzel SET pn.krankenkasse = kk.kkid;");

        // 5. Umbenennen der Tabellen, Anpassung der Constraints
        st.executeQuery("ALTER TABLE personal RENAME TO personal_alt;");
        st.executeQuery("ALTER TABLE personal_neu RENAME TO personal;");

        // 6. Löschen der alten Daten
        st.executeQuery("DELETE FROM personal_alt;");

        // 7. Löschen des alten Schmemas
        st.executeQuery("DROP TABLE personal_alt;");
    }
}
