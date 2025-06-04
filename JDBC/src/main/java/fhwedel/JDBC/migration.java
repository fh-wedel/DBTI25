package fhwedel.JDBC;

import java.sql.*;

public class migration {
    public static void main(String[] args) {
        try {
            //Aufgabe 1
            Connection con = DriverManager.getConnection("jdbc:mariadb://localhost:3306/firma", "root", "password");

            //Aufgabe 2
            //addEmployee(con, 417, "Krause", "Henrik", "it1", "d13", "tkk");

            //Aufgabe 3
            showTable(con, "personal");

            //Aufgabe 4
            updateGehalt(con, "it1", 0.1);
            showTable(con, "gehalt");

            //Aufgabe 5
            deleteEmployee(con, "Tietze", "Lutz");

            //Aufgabe 6
            showSalesEmployees(con);

        } catch (Exception e) {
            System.out.print(e);
        }
        
        
    }

    public static void addEmployee(Connection con, int pnr, String name, String vorname, 
                                String gehalt, String abteilung, String krankenkasse) {
        try {
            String sql = "INSERT INTO personal VALUES (?, ?, ?, ?, ?, ?)";
            PreparedStatement stmt = con.prepareStatement(sql);
            stmt.setInt(1, pnr);
            stmt.setString(2, name);
            stmt.setString(3, vorname);
            stmt.setString(4, gehalt);
            stmt.setString(5, abteilung);
            stmt.setString(6, krankenkasse);
            stmt.executeUpdate();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static void showTable(Connection con, String tablename) {
        try {
            Statement stat = con.createStatement();
            String sql = "SELECT * FROM " + tablename;
            ResultSet rs = stat.executeQuery(sql);
            showResultSet(rs);
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static void showResultSet(ResultSet rs) {
        try {
            ResultSetMetaData rsmd = rs.getMetaData();
            
            while (rs.next()) {
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    String name = rsmd.getColumnName(i);
                    System.out.print(name + ": " + rs.getObject(i) + "; ");
                }
                System.out.println();
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }


    public static void updateGehalt(Connection con, String gehaltStufe, double erhoehung) {
        try {
            ResultSet currentGehaltRS = con.createStatement().executeQuery("SELECT betrag FROM gehalt WHERE geh_stufe = \"" + gehaltStufe + "\"");
            currentGehaltRS.next();
            int currentGehalt =  currentGehaltRS.getInt(1);
            int newGehalt = (int)(currentGehalt * (1+erhoehung));

            String sql = "UPDATE gehalt SET betrag = ? WHERE geh_stufe = ?";
            PreparedStatement stat = con.prepareStatement(sql);
            stat.setInt(1, newGehalt);
            stat.setString(2, gehaltStufe);
            stat.executeUpdate();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static void deleteEmployee(Connection con, String name, String vorname) {
        try {
            String sql = "DELETE FROM personal WHERE name = \"" + name + "\" AND vorname = \"" + vorname + "\"";
            Statement stat = con.createStatement();
            stat.executeUpdate(sql);
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static void showSalesEmployees(Connection con) {
        try {
            String sql = "SELECT * FROM personal JOIN abteilung ON personal.abt_nr = abteilung.abt_nr WHERE abteilung.name = \"Verkauf\"";
            Statement stat = con.createStatement();
            ResultSet rs = stat.executeQuery(sql);
            showResultSet(rs);
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
