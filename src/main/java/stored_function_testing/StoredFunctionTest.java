package stored_function_testing;



/*
 * @Project Name: database-testing
 * @Author: Okechukwu Bright Onwumere
 * @Created: 15-Nov-24
 */


import utilities.DatabaseUtil;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.*;

public class StoredFunctionTest {

    Connection connection = null;
    Statement statement = null;
    CallableStatement callableStatement = null;
    ResultSet resultSet = null;
    ResultSet resultSet1 = null;
    ResultSet resultSet2 = null;

    @BeforeClass
    void setUp() throws SQLException {
        connection = DatabaseUtil.connectToDatabase();
    }

    @AfterClass
    void tearDown() throws SQLException {
        connection.close();
    }

    @Test(priority = 1)
    void testCustomerLevelExists() throws SQLException {
        statement = connection.createStatement();
        statement.executeQuery("SHOW FUNCTION STATUS WHERE Name = 'CustomerLevel'");
        resultSet = statement.getResultSet();
        resultSet.next();
        Assert.assertEquals(resultSet.getString("Name"), "CustomerLevel");
    }

    @Test(priority = 2)
    void testCustomerLevelWithSQLStatement() throws SQLException {
        statement = connection.createStatement();
        resultSet1 = statement.executeQuery("SELECT customerName, CustomerLevel(creditLimit) FROM customers");

        statement = connection.createStatement();
        String query = """
                SELECT customerName,
                CASE
                WHEN creditLimit > 50000 THEN 'PLATINUM'
                WHEN creditLimit >= 10000 AND creditLimit <= 50000 THEN 'GOLD'
                ELSE 'SILVER'
                END AS customerLevel
                FROM customers
                """;
        resultSet2 = statement.executeQuery(query);
        Assert.assertTrue(DatabaseUtil.compareResultSet(resultSet1, resultSet2));
    }

    @Test(priority = 2)
    void testCustomerLevelWithProcedure() throws SQLException {
        callableStatement = connection.prepareCall("{CALL GetCustomerLevel(?,?)}");
        callableStatement.setInt(1, 131);
        callableStatement.registerOutParameter(2, Types.VARCHAR);
        callableStatement.executeQuery();

        String customerLevel = callableStatement.getString(2);
        System.out.printf("CustomerLevel: %s%n", customerLevel);

        statement = connection.createStatement();
        String query = """
                SELECT customerName,
                CASE
                WHEN creditLimit > 50000 THEN 'PLATINUM'
                WHEN creditLimit >= 10000 AND creditLimit <= 50000 THEN 'GOLD'
                ELSE 'SILVER'
                END AS customerLevel
                FROM customers
                WHERE customerNumber = 131
                """;
        resultSet = statement.executeQuery(query);
        resultSet.next();
        String customerLevelQuery = resultSet.getString("customerLevel");
        System.out.printf("CustomerLevelByQuery: %s%n", customerLevelQuery);
        Assert.assertEquals(customerLevelQuery, customerLevel);


    }


}
