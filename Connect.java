package bdd;

import java.sql.Connection;
import java.sql.DriverManager;

public class Connect {
    public static Connection seConnecterOracle(String user , String password) throws Exception
    {
        Class.forName("oracle.jdbc.driver.OracleDriver");
        Connection co = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1522:ORCL",user,password);
        return co;
    }
    public static Connection seConnecterPostgre(String user , String mdp , String base) throws Exception
    {
        Class.forName("org.postgresql.Driver");
        String url="jdbc:postgresql://localhost:5432/"+base;
        Connection con=DriverManager.getConnection(url,user,mdp);
        return con;
    }
    
    public static Connection getConnection() throws Exception{
        return Connect.seConnecterOracle("scott","tiger");
        // return Connect.seConnecterPostgre("postgres","Tojonirina" , "billard");
    }
}
