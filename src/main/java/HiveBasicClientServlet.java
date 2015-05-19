import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.PrintWriter;

/**
 *
 * @author Francisco Romero Bueno frb@tid.es
 * 
 * Basic remote client for Hive mimicing the native Hive CLI behaviour.
 * 
 * Can be used as the base for more complex clients, interactive or not interactive.
 */
public final class HiveBasicClientServlet extends HttpServlet {

@Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        resp.getWriter().println("Heloo Top");
        PrintWriter writer = resp.getWriter();
        demo(writer);
}



    // JDBC driver required for Hive connections
    private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
    private static Connection con;
    
     /* 
     * @param hiveServer
     * @param hivePort
     * @param hadoopUser
     * @param hadoopPassword
     * @return
     */
    private static Connection getConnection(String hiveServer, String hivePort, String hadoopUser,
            String hadoopPassword) {
        try {
            // dynamically load the Hive JDBC driver
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            System.out.println(e.getMessage());
            return null;
        } // try catch
      
        try {
            System.out.println("Connecting to jdbc:hive://" + hiveServer + ":" + hivePort
                    + "/default?user=" + hadoopUser + "&password=XXXXXXXXXX");
            // return a connection based on the Hive JDBC driver
            return DriverManager.getConnection("jdbc:hive://" + hiveServer + ":" + hivePort
                    + "/default?user=" + hadoopUser + "&password=" + hadoopPassword);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return null;
        } // try catch
    } // getConnection

    private static void doQuery(PrintWriter writer) {
        try {
            // from here on, everything is SQL!
            Statement stmt = con.createStatement();
            
            if(stmt != null){
                System.out.println("It works");
            }
            
            ResultSet res = stmt
                    .executeQuery("select *"
                            + "from hostabee");
            
            // iterate on the result
                  // iterate on the result
                while (res.next()) {
                    String s = "";

                    for (int i = 1; i < res.getMetaData().getColumnCount(); i++) {
                        s += res.getString(i) + ",";
                    } // for
                  
                    s += res.getString(res.getMetaData().getColumnCount());
                    writer.println(s);
                } 

            // close everything
            res.close();
            stmt.close();
            con.close();
        } catch (SQLException ex) {
            System.exit(0);
        } // try catch
    } // doQuery


    /**
     * 
     * @param args
     */
    public void demo(PrintWriter writer) {
        con = getConnection("130.206.80.46", "10000", "guillaume.jourdain",
                "**********");

        if (null != con)
            doQuery(writer);
    } // main
    
} //HiveClientTest
