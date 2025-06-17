package fhwedel.JDBC;

import java.sql.*;

public class migration {
    public static void main(String[] args) {
        try {
            //Aufgabe 1
            Connection con = DriverManager.getConnection("jdbc:mariadb://localhost:3306/firma", "root", "password");

            //Aufgabe 2
            addEmployee(con, 417, "Krause", "Henrik", "it1", "d13", "tkk");

            //Aufgabe 3
            showTable(con, "personal");

            //Aufgabe 4
            updateGehalt(con, "it1", 0.1);
            showTable(con, "gehalt");

            //Aufgabe 5
            deleteEmployee(con, "Tietze", "Lutz");

            //Aufgabe 6
            showSalesEmployees(con);

            //Migration
            migrateKrankenkasse(con);

            con.close();
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
            stmt.close();
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
            stat.close();
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
            stat.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static void deleteEmployee(Connection con, String name, String vorname) {
        try {
            String sql = "DELETE FROM personal WHERE name = \"" + name + "\" AND vorname = \"" + vorname + "\"";
            Statement stat = con.createStatement();
            stat.executeUpdate(sql);
            stat.close();
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
            stat.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static void migrateKrankenkasse(Connection con) {
        try {
            String sql = "CREATE TABLE krankenversicherung (kkid INTEGER, kuerzel varchar(10), name varchar(100), constraint Schluessel_KK primary key (kkid));";
            Statement stat = con.createStatement();
            stat.executeUpdate(sql);

            insertKrankenkasse(con, 1, "aok", "Allgemeine Ortskrankenkasse");
            insertKrankenkasse(con, 2, "bak", "Betriebskrankenkasse B. Braun Aesculap");
            insertKrankenkasse(con, 3, "bek", "Barmer Ersatzkasse");
            insertKrankenkasse(con, 4, "dak", "Deutsche Angestelltenkrankenkasse");
            insertKrankenkasse(con, 5, "tkk", "Techniker Krankenkasse");
            insertKrankenkasse(con, 6, "kkh", "Kaufmännische Krankenkasse");

            alterPersonal(con);

            stat.close();

        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static void insertKrankenkasse(Connection con, int kkid, String kuerzel, String name) {
        try {
            String sql = "INSERT INTO krankenversicherung VALUES (?, ?, ?)";
            PreparedStatement stat = con.prepareStatement(sql);
            stat.setInt(1, kkid);
            stat.setString(2, kuerzel);
            stat.setString(3, name);
            stat.executeUpdate();
            stat.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static void alterPersonal(Connection con) {
        try {
            String columnsql = "ALTER TABLE personal ADD temp_krankenkasse INTEGER";
            String constraintsql = "ALTER TABLE personal ADD FOREIGN KEY (temp_krankenkasse) REFERENCES krankenversicherung(kkid)";
            Statement stat = con.createStatement();
            stat.executeUpdate(columnsql);
            stat.executeUpdate(constraintsql);

            String selectPersonal = "SELECT pnr, krankenkasse FROM personal";
            ResultSet rs = stat.executeQuery(selectPersonal);

            while (rs.next()) {
                int pnr = rs.getInt(1);
                String krankenkasse = rs.getString(2);

                String selectKrankenkasse = "SELECT kkid FROM krankenversicherung WHERE kuerzel = ?";
                PreparedStatement ps = con.prepareStatement(selectKrankenkasse);
                ps.setString(1, krankenkasse);
                ResultSet rs2 = ps.executeQuery();
                rs2.next();
                
                int kkid = rs2.getInt("kkid");
                String updatePersonal = "UPDATE personal SET temp_krankenkasse = ? WHERE pnr = ?";
                PreparedStatement ps2 = con.prepareStatement(updatePersonal);
                ps2.setInt(1, kkid);
                ps2.setInt(2, pnr);
                ps2.executeUpdate();
                
                ps.close();
                ps2.close();
            }

            String dropColumn = "ALTER TABLE personal DROP COLUMN krankenkasse";
            String renameColumn = "ALTER TABLE personal CHANGE temp_krankenkasse krankenkasse INTEGER";
            stat.executeUpdate(dropColumn);
            stat.executeUpdate(renameColumn);
            stat.close();
            
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
