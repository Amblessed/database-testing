package stored_procedure_testing;



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


public class StoredProcedureTest {

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
    void testStoredProcedureExists() throws SQLException {
        statement = connection.createStatement();
        statement.executeQuery("SHOW PROCEDURE STATUS WHERE Name = 'selectAllCustomers'");
        resultSet = statement.getResultSet();
        resultSet.next();
        Assert.assertEquals(resultSet.getString("Name"), "selectAllCustomers");
    }

    @Test(priority = 2)
    void testSelectAllCustomersExists() throws SQLException {
        callableStatement = connection.prepareCall("{CALL selectAllCustomers()}");
        resultSet1 = callableStatement.executeQuery();

        statement = connection.createStatement();
        resultSet2 = statement.executeQuery("SELECT * FROM customers");

        Assert.assertTrue(DatabaseUtil.compareResultSet(resultSet1, resultSet2));
    }

    @Test(priority = 3)
    void testSelectAllCustomersByCity() throws SQLException {
        callableStatement = connection.prepareCall("{CALL selectAllCustomersByCity(?)}");
        callableStatement.setString(1, "Singapore");
        resultSet1 = callableStatement.executeQuery();

        statement = connection.createStatement();
        resultSet2 = statement.executeQuery("SELECT * FROM customers WHERE City = 'Singapore'");

        Assert.assertTrue(DatabaseUtil.compareResultSet(resultSet1, resultSet2));
    }

    @Test(priority = 3)
    void testSelectAllCustomersByCityAndPinCode() throws SQLException {
        callableStatement = connection.prepareCall("{CALL selectAllCustomersByCityAndPin(?, ?)}");
        callableStatement.setString(1, "Singapore");
        callableStatement.setString(2, "079903");
        resultSet1 = callableStatement.executeQuery();

        statement = connection.createStatement();
        resultSet2 = statement.executeQuery("SELECT * FROM customers WHERE City = 'Singapore' AND postalCode = '079903'");

        Assert.assertTrue(DatabaseUtil.compareResultSet(resultSet1, resultSet2));
    }

    @Test(priority = 4)
    void testGetOrderByCustomerNumber() throws SQLException {
        callableStatement = connection.prepareCall("{CALL getOrderByCustomerNumber(?, ?, ?, ?, ?)}");
        callableStatement.setInt(1, 141);
        callableStatement.registerOutParameter(2, Types.INTEGER);
        callableStatement.registerOutParameter(3, Types.INTEGER);
        callableStatement.registerOutParameter(4, Types.INTEGER);
        callableStatement.registerOutParameter(5, Types.INTEGER);
        callableStatement.executeQuery();

        int shipped = callableStatement.getInt(2);
        int cancelled = callableStatement.getInt(3);
        int resolved = callableStatement.getInt(4);
        int disputed = callableStatement.getInt(5);

        System.out.printf("Shipped: %d%nCancelled: %d%nResolved: %d%nDisputed: %d%n", shipped, cancelled, resolved, disputed);

        statement = connection.createStatement();
        String shippedQuery = "(SELECT count(*) AS 'shipped' FROM orders WHERE customerNumber = 141 AND status = 'Shipped') AS Shipped";
        String cancelledQuery = "(SELECT count(*) AS 'cancelled' FROM orders WHERE customerNumber = 141 AND status = 'Canceled') AS Cancelled";
        String resolvedQuery = "(SELECT count(*) AS 'resolved' FROM orders WHERE customerNumber = 141 AND status = 'Resolved') AS Resolved";
        String disputedQuery = "(SELECT count(*) AS 'disputed' FROM orders WHERE customerNumber = 141 AND status = 'Disputed') AS Disputed";
        String query = String.format("SELECT%s,%s,%s,%s;", shippedQuery, cancelledQuery, resolvedQuery, disputedQuery);
        resultSet = statement.executeQuery(query);

        resultSet.next();

        int shippedExp = resultSet.getInt("shipped");
        int cancelledExp = resultSet.getInt("cancelled");
        int resolvedExp = resultSet.getInt("resolved");
        int disputedExp = resultSet.getInt("disputed");
        System.out.printf("ShippedExp: %d%nCancelledExp: %d%nResolvedExp: %d%nDisputedExp: %d%n", shippedExp, cancelledExp, resolvedExp, disputedExp);
        Assert.assertEquals(shipped, shippedExp);
        Assert.assertEquals(cancelled, cancelledExp);
        Assert.assertEquals(resolved, resolvedExp);
        Assert.assertEquals(disputed, disputedExp);
    }


    @Test(priority = 5)
    void testGetCustomerShipping() throws SQLException {
        callableStatement = connection.prepareCall("{CALL GetCustomerShipping(?, ?)}");
        callableStatement.setInt(1, 112);
        callableStatement.registerOutParameter(2, Types.VARCHAR);
        callableStatement.executeQuery();

        String shippingDaysProcedure = callableStatement.getString(2);
        System.out.printf("ShippingDays: %s%n", shippingDaysProcedure);

        statement = connection.createStatement();
        String query = """ 
                SELECT country,
                CASE
                    WHEN country = 'USA' THEN '2-day Shipping'
                    WHEN country = 'Canada' THEN '3-day Shipping'
                    ELSE '5-day Shipping'
                END AS ShippingTime
                FROM customers WHERE customerNumber = 112;
                """;
        resultSet = statement.executeQuery(query);
        resultSet.next();

        String shippingDaysQuery = resultSet.getString("ShippingTime");
        System.out.printf("ShippingDays: %s%n", shippingDaysQuery);

        Assert.assertEquals(shippingDaysProcedure, shippingDaysQuery);

    }


}
