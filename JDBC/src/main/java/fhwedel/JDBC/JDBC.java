package fhwedel.JDBC;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBC {
    public static void main(String[] args) throws SQLException {
        // 1. Connection
        Connection con = connect();
        Statement st = con.createStatement();

        // 2. CREATE
        try {
            st.executeQuery("INSERT INTO personal VALUES (417, 'Krause', 'Henrik', 'it1', 'd13', 'tkk');");
        } catch (SQLException e) {
            System.out.println("Fehler bei Aufgabe 2. Vermutlich wurde der Nutzer bereits angelegt;");
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

    }

    /**
     * Baut eine Verbindung zu dem lokalen mariadb-Server auf und verbindet sich mit der Datenbank "firma". 
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
}
