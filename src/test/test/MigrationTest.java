package test;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;

public class MigrationTest {
    private Connection con;
    @Before
    public void connect() throws ClassNotFoundException, SQLException {
        Class.forName("org.postgresql.Driver");
        con =  DriverManager.getConnection(CONNECT_URL,USERNAME,PASSWORD);
    }
    @Test
    public void firstTest() throws SQLException, IOException {

       Statement stmt=con.createStatement();

       ResultSet rs=stmt.executeQuery("select " +
               "creator_id, " +
               "COUNT(*) as count_t," +
               "string_agg(title, ';') as names ," +
               "max(created_at) as date_time, " +
               "min(length(description)) as min_description " +
               "from subjects " +
               "group by creator_id, group_id " +
               "order by count_t desc");
       String csvFilePath = "export1.csv";
       BufferedWriter fileWriter = new BufferedWriter(new FileWriter(csvFilePath));

       while(rs.next()){
           String line = String.format("%s,%d,\"%s\",%s,%s",
                   rs.getString("creator_id"),
                   rs.getInt("count_t"),
                   rs.getString("names"),
                   rs.getString("date_time"),
                   rs.getString("min_description"));
           fileWriter.newLine();
           fileWriter.write(line);
       }
       fileWriter.close();
       con.close();
       Assert.assertTrue(difference("expected1.csv","export1.csv","difference1.csv"));
    }

    @Test
    public void secondTest() throws SQLException, ClassNotFoundException, IOException {
        Class.forName("org.postgresql.Driver");

        Connection con =  DriverManager.getConnection(CONNECT_URL,USERNAME,PASSWORD);
        Statement stmt=con.createStatement();

        ResultSet rs=stmt.executeQuery("select * from users order by username");
        String csvFilePath = "export2.csv";
        BufferedWriter fileWriter = new BufferedWriter(new FileWriter(csvFilePath));

        while(rs.next()){
            String line = String.format("%s,%s,%s,%s,%s",
                    rs.getString("id"),
                    rs.getString("username"),
                    rs.getString("full_name"),
                    rs.getString("email"),
                    rs.getString("password"));
            fileWriter.newLine();
            fileWriter.write(line);
        }
        fileWriter.close();
        con.close();
        Assert.assertTrue(difference("expected2.csv","export2.csv","difference2.csv"));
    }
    public boolean difference(String expected, String export,String result) throws IOException {
        ArrayList <String> al1=new ArrayList<>();
        ArrayList <String> al2=new ArrayList<>();

        BufferedReader CSVFile1 = new BufferedReader(new FileReader(export));
        String dataRow1 = CSVFile1.readLine();
        while (dataRow1 != null)
        {
            String[] dataArray1 = dataRow1.split(",");
            al1.addAll(Arrays.asList(dataArray1));

            dataRow1 = CSVFile1.readLine();
        }

        CSVFile1.close();

        BufferedReader CSVFile2 = new BufferedReader(new FileReader(expected));
        String dataRow2 = CSVFile2.readLine();
        while (dataRow2 != null)
        {
            String[] dataArray2 = dataRow2.split(",");
            al2.addAll(Arrays.asList(dataArray2));
            dataRow2 = CSVFile2.readLine();
        }
        CSVFile2.close();

        for(String bs:al2)
        {
            al1.remove(bs);
        }
        al1.remove("");
        int size=al1.size();
        int size1=al1.size();
        System.out.println("Don`t match: "+size);

        try
        {
            FileWriter writer=new FileWriter(result);
            while(size!=0)
            {
                size--;
                writer.append(al1.get(size));
                writer.append('\n');
            }
            writer.flush();
            writer.close();
        }
        catch(IOException e)
        {
            e.printStackTrace();
        }
        return size1 == 0;
    }
    @After
    public void close() throws SQLException {
        con.close();
    }
}
