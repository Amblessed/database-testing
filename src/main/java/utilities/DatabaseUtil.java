package utilities;

/*
 * @Project Name: database-testing
 * @Author: Okechukwu Bright Onwumere
 * @Created: 15-Nov-24
 */


import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DatabaseUtil {

    public static Connection connectToDatabase() throws SQLException {
        return DriverManager.getConnection("jdbc:mysql://localhost:3306/classicmodels", "root", System.getenv("MYSQL_PASSWORD"));
    }

    public static boolean compareResultSet(ResultSet resultSet1, ResultSet resultSet2) throws SQLException {
        while (resultSet1.next()) {
            resultSet2.next();
            int count = resultSet1.getMetaData().getColumnCount();
            for (int i = 1; i <= count; i++) {
                if(!StringUtils.equals(resultSet1.getString(i), resultSet2.getString(i))){
                    return false;
                }
            }
        }
        return true;
    }
}
