package fhwedel.JDBC;

import java.sql.*;

public class migration {
    public static void main(String[] args) {
        try {
            Connection con = DriverManager.getConnection("jdbc:mariadb://localhost:3306/firma", "root", "password");
            Statement statement = con.createStatement();
            ResultSet rs = statement.executeQuery("SELECT geh_stufe FROM gehalt");
            while (rs.next()) {
                System.out.print(rs.getString(1));
            }
        } catch (Exception e) {
            System.out.print(e);
        }
        
    }
}
