package fhwedel.JDBC;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class Hello {
    public static void main(String[] args) {
        //System.out.println("Hello, Du noob");
        Connection con = createDbConn();
        System.out.println("Verbindung aufgebaut!");
        //createNewEmployee(con);
        //showAllInPersonal(con);
        //updateIt1(con);
        //deleteEmployee(con);
        showAllEmloyeeInSales(con);
    }


    public static Connection createDbConn(){
        try {
            return DriverManager.getConnection("jdbc:mariadb://localhost:3306/firma","root","password");
        } catch (Exception e) {
            System.err.println("Verbindung Fehlgeschlagen!");
            return null;
        }
    }
    
    public static void createNewEmployee(Connection con){
        try {
            Statement statement = con.createStatement();
            statement.executeQuery("INSERT INTO personal(pnr,name,vorname,geh_stufe,abt_nr, krankenkasse) VALUES(417,'Krause','Henrik','it1','d13','tkk')");            
        } catch (Exception e) {
            System.err.println(e);
            System.err.println("Ein Fehler ist aufgetreten!");
        }
    }

    public static void showAllInPersonal(Connection con ){
        try {
            Statement statement = con.createStatement();
            ResultSet resultSet = statement.executeQuery("Select * from personal");
            while (resultSet.next()) {
                System.out.println(resultSet.getString("Vorname") + " " + resultSet.getString("name"));
            }
        } catch (Exception e) {
            System.err.println(e);
            System.err.println("Fehler beim Anzeigen!");
        }
    }

    public static void updateIt1(Connection con){
        try {
            Statement statement = con.createStatement();  
            statement.executeQuery("UPDATE gehalt SET betrag = betrag * 1.1 WHERE geh_stufe = 'it1';");          
        } catch (Exception e) {
            System.err.println(e);
            System.err.println("Fehler beim Ändern!");
        }
    }

    public static void deleteEmployee(Connection con){
        try {
            Statement statement = con.createStatement();  
            statement.executeQuery("DELETE FROM personal WHERE vorname = 'Lutz' AND name = 'Tietze';");          
        } catch (Exception e) {
            System.err.println(e);
            System.err.println("Fehler beim Löschen!");
        } 
    }

    public static void showAllEmloyeeInSales(Connection con){
         try {
            Statement statement = con.createStatement();  
            ResultSet resultSet = statement.executeQuery("Select * FROM personal AS p JOIN abteilung AS a ON a.abt_nr = p.abt_nr WHERE a.name = 'Verkauf';");    
            while (resultSet.next()) {
                System.out.println(resultSet.getString("p.vorname")+" "+ resultSet.getString("p.name"));
            }
        } catch (Exception e) {
            System.err.println(e);
            System.err.println("Fehler beim Anzeigen!");
        } 
    }

    public static void createNewSchema(Connection con){
         try {
            Statement statement = con.createStatement();  
            statement.executeQuery("CREATE TABLE krankenversicherung (kkid int NOT NULL,kuerzel varchar(255) NOT NULL, name varchar(255) NOT NULL, PRIMARY KEY (kkid));");
        } catch (Exception e) {
            System.err.println(e);
            System.err.println("Fehler bei Schema-Migration!");
        } 
    }

}
