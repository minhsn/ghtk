package tuan3;

import com.github.javafaker.Faker;
import com.opencsv.CSVReader;

import java.io.*;
import java.sql.*;

public class bai1 {
    public static Connection getConnect() throws SQLException, ClassNotFoundException {
        final String DB_URL = "jdbc:mysql://localhost:3306/ghtk";
        final String USER = "root";
        final String PASS = "";
        Connection connection=DriverManager.getConnection(DB_URL,USER,PASS);
        return  connection;
    }


    public static void main(String[] agrs) throws ClassNotFoundException, SQLException, IOException {

        int batch_size=2500;
        CSVReader reader = new CSVReader(new FileReader("/home/dell/Desktop/ghtk/src/main/java/tuan3/gop.csv"));
        String[] nextLine;

        Connection conn=getConnect();
        Statement statement=conn.createStatement();
        String sql = "INSERT INTO customers_packages(pkg_order, shop_code, customer_tel, customer_tel_normalize, fullname, pkg_created" + ","+
                " pkg_modified, package_status_id, customer_province_id, customer_district_id, customer_ward_id, created, modified, is_cancel, ightk_user_id) VALUES";


        int i=0;

        while ((nextLine = reader.readNext()) != null)  {
            i++;
            sql += "(" + i + ",'" + nextLine[0] + "','" + nextLine[1] + "','" + nextLine[2] + "',\""
                    + nextLine[3] +"\",'" + nextLine[4] + "','" + nextLine[5]+"'," + nextLine[6]
                    + "," + nextLine[7] + ","
                    + nextLine[8] + "," + nextLine[9] + ",'"
                    + nextLine[10] + "','" + nextLine[11] + "'," + nextLine[12] +"," +nextLine[13] +")";
            if (i%batch_size==0 )  {
                sql+=";";
                System.out.println(i);
                statement.executeUpdate(sql);
                sql = "INSERT INTO customers_packages(pkg_order, shop_code, customer_tel, customer_tel_normalize, fullname, pkg_created" + "," +
                        " pkg_modified, package_status_id, customer_province_id, customer_district_id, customer_ward_id, created, modified, is_cancel, ightk_user_id) VALUES";
            }
            else {
                sql+=',';
            }
        }
    }
}
