using System;
using System.Data.SqlClient;

namespace Babelfish_Demo
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Getting Connection ...");

            var datasource = @"babelfish-db.cluster-xyz.us-east-1-beta.rds.amazonaws.com";
            var database = "demo";
            var username = "posgres";
            var password = "<type your DB password here>";

            //your connection string
            string connString = @"Data Source=" + datasource + ";Initial Catalog="
                        + database + ";Persist Security Info=True;User ID=" + username + ";Password=" + password;

            //create instanace of database connection
            SqlConnection conn = new SqlConnection(connString);


            try
            {
                Console.WriteLine("Connecting to.." + datasource);

                //open connection
                conn.Open();

                Console.WriteLine("Connection successful!");
                Console.ReadLine();
                Console.WriteLine("Let's read some data ORDER BY first_name");
                Console.ReadLine();

                string querystring = @"SELECT  * FROM
                   sales.customers
                    WHERE
                    state = 'CA'
                    ORDER BY
                    first_name";
                SqlCommand cmd = new SqlCommand(querystring, conn);
                SqlDataReader reader = cmd.ExecuteReader();
                Console.WriteLine("Customer_id\t\tFirstName\t\tLastName");
                while (reader.Read())
                {
                    Console.WriteLine(String.Format("{0} \t\t | {1} \t\t | {2}",
                        reader[0], reader[1], reader[2]));
                }

                Console.WriteLine("Data displayed! Now press enter to move to the next section!");
                Console.ReadLine();
                Console.Clear();
                reader.Close();

                /* Above code was used to display the data from the Database table!
                 * This following section explains the key features to use
                 * to add the data to the table. This is an example of another
                 * SQL Command (INSERT INTO), this will teach the usage of parameters and connection.*/

                Console.WriteLine("INSERT command where order_id=1615 and item_id=6");

                // Create the command, to insert the data into the Table!
                // this is a simple INSERT INTO command!

                SqlCommand insertCommand = new SqlCommand("INSERT INTO sales.order_items(order_id, item_id, product_id, quantity, list_price,discount) VALUES(@0,@1,@2,@3,@4,@5);", conn);
                insertCommand.Parameters.Add(new SqlParameter("0", 1615));
                insertCommand.Parameters.Add(new SqlParameter("1", 6));
                insertCommand.Parameters.Add(new SqlParameter("2", 152));
                insertCommand.Parameters.Add(new SqlParameter("3", 2));
                insertCommand.Parameters.Add(new SqlParameter("4", 946.49));
                insertCommand.Parameters.Add(new SqlParameter("5", 0.05));

                Console.WriteLine("Commands executed! Total rows affected are " + insertCommand.ExecuteNonQuery());
                Console.WriteLine("Done! Let's read this row where order_id=1615 and item_id=6");
                string Selectstring = @"SELECT  * FROM sales.order_items where order_id = @0 and item_id = @1";
                SqlCommand Selectcmd = new SqlCommand(Selectstring, conn);
                Selectcmd.Parameters.Add(new SqlParameter("0", 1615));
                Selectcmd.Parameters.Add(new SqlParameter("1", 6));
                SqlDataReader Selectreader = Selectcmd.ExecuteReader();
                Console.WriteLine("order_id\titem_id\tproduct_id\tquantity\tlist_price\tdiscount");
                while (Selectreader.Read())
                {
                    Console.WriteLine(String.Format("{0} \t | {1} \t | {2} \t | {3} \t | {4} \t | {5}",
                        Selectreader[0], Selectreader[1], Selectreader[2], Selectreader[3], Selectreader[4], Selectreader[5]));
                }

                Selectreader.Close();
                Console.WriteLine("Update this row: set quantity=3 where order_id=1615 and item_id=6");
                Console.ReadLine();

                SqlCommand UPDCommand = new SqlCommand("UPDATE sales.order_items SET quantity=@0 where order_id=@1 and item_id=@2", conn);
                UPDCommand.Parameters.Add(new SqlParameter("0", 3));
                UPDCommand.Parameters.Add(new SqlParameter("1", 1615));
                UPDCommand.Parameters.Add(new SqlParameter("2", 6));

                Console.WriteLine("Update executed! Total rows affected are " + UPDCommand.ExecuteNonQuery());
                Console.WriteLine("Let's read this row again; order_id=1615 and item_id=6");
                string Select1string = @"SELECT  * FROM sales.order_items where order_id = @0 and item_id = @1";
                SqlCommand Select1cmd = new SqlCommand(Select1string, conn);
                Select1cmd.Parameters.Add(new SqlParameter("0", 1615));
                Select1cmd.Parameters.Add(new SqlParameter("1", 6));
                SqlDataReader Select1reader = Select1cmd.ExecuteReader();

                Console.WriteLine("order_id\titem_id\tproduct_id\tquantity\tlist_price\tdiscount");
                while (Select1reader.Read())
                {
                    Console.WriteLine(String.Format("{0} \t | {1} \t | {2} \t | {3} \t | {4} \t | {5}",
                        Select1reader[0], Select1reader[1], Select1reader[2], Select1reader[3], Select1reader[4], Select1reader[5]));
                }

                Select1reader.Close();
                Console.ReadLine();

                Console.WriteLine("Delete row where order_id=1615 and item_id=6");

                SqlCommand DELCommand = new SqlCommand("DELETE FROM sales.order_items where order_id=@0 and item_id=@1", conn);
                DELCommand.Parameters.Add(new SqlParameter("0", 1615));
                DELCommand.Parameters.Add(new SqlParameter("1", 6));

                Console.WriteLine("Delete executed! Total rows affected are " + DELCommand.ExecuteNonQuery());
                Console.WriteLine("Let's read this row again where order_id=1615 and item_id=6");
                string Select2string = @"SELECT  * FROM sales.order_items where order_id = @0 and item_id = @1";
                SqlCommand Select2cmd = new SqlCommand(Select2string, conn);
                Select2cmd.Parameters.Add(new SqlParameter("0", 1615));
                Select2cmd.Parameters.Add(new SqlParameter("1", 6));
                SqlDataReader Select2reader = Select2cmd.ExecuteReader();

                Console.WriteLine("order_id\titem_id\tproduct_id\tquantity\tlist_price\tdiscount");
                while (Select2reader.Read())
                {
                    Console.WriteLine(String.Format("{0} \t | {1} \t | {2} \t | {3} \t | {4} \t | {5}",
                        Select2reader[0], Select2reader[1], Select2reader[2], Select2reader[3], Select2reader[4], Select2reader[5]));
                }

                Console.WriteLine("0 row(s) returned");
                Select2reader.Close();
                Console.ReadLine();

                Console.Clear();

                Console.WriteLine("Execute a nested Select with GROUP BY query");
                Console.ReadLine();

                string ctestring = @"WITH cte_sales_amounts(staff, sales, year) AS(
                SELECT
                 first_name + ' ' + last_name,
                 SUM(quantity * list_price * (1 - discount)),
                 YEAR(order_date)
                   FROM
                  sales.orders o
                   INNER JOIN sales.order_items i ON i.order_id = o.order_id
                   INNER JOIN sales.staffs s ON s.staff_id = o.staff_id
                   GROUP BY
                   first_name + ' ' + last_name,
                    year(order_date)
                    )
                    SELECT
                        staff,
                        sales
                    FROM
                    cte_sales_amounts
                    WHERE
                    year = 2018";
                SqlCommand ctecmd = new SqlCommand(ctestring, conn);
                SqlDataReader ctereader = ctecmd.ExecuteReader();
                while (ctereader.Read())
                {
                    Console.WriteLine(ctereader[0].ToString() + " " + ctereader[1].ToString());
                }

                Console.WriteLine("Done! Press enter to exit application");
                Console.ReadLine();
                Console.Clear();
                ctereader.Close();


            } /*Try loop end */
            catch (Exception e)
            {
                Console.WriteLine("Error: " + e.Message);
            }

            Console.Read();
        }
    }
}
