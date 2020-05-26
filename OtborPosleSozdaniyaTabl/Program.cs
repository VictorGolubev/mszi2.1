using System;
using Tutorial.SqlConn;
using MySql.Data.MySqlClient;
using System.Collections.Generic;

namespace OtborPosleSozdaniyaTabl
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Getting Connection ...");
            MySqlConnection conn = DBUtils.GetDBConnection();

            //Открытие соединения с БЛ
            try
            {
                Console.WriteLine("Openning Connection ...");

                conn.Open();
                string sqlQuery = "USE dump_rasulov";
                MySqlCommand command = new MySqlCommand(sqlQuery, conn);
                command.ExecuteNonQuery();

                sqlQuery = "CREATE TABLE `sample` (`id` INT NOT NULL AUTO_INCREMENT, `ip_source` VARCHAR(450) NULL, `ip_destination` VARCHAR(450) NULL, `port` VARCHAR(450) NULL, `duration` VARCHAR(450) NULL, `total_bytes` VARCHAR(450) NULL, `total_packets` VARCHAR(450) NULL, `tabel_name` VARCHAR(450) NULL, PRIMARY KEY(`id`));";
                command = new MySqlCommand(sqlQuery, conn);
                command.ExecuteNonQuery();

                string sql1 = "SELECT total_bytes,table_name FROM `11statistics`";
                command = new MySqlCommand(sql1, conn);
                MySqlDataReader reader = command.ExecuteReader();

                List<int> bytes = new List<int>();
                List<string> names= new List<string>();
                int sum = 0;
                while (reader.Read())
                {
                    int packetsBytes = Convert.ToInt32(reader[0]);
                    bytes.Add(packetsBytes);
                    string tableNames = reader[1].ToString();
                    names.Add(tableNames);
                    sum += packetsBytes;
                }
                reader.Close();


                int c = 1;
                for(int i = 0; i < names.Count; i++)
                {
                    string tableName = names[i];

                    //Получение количества пакетов
                    string query = getQueryCount(tableName);
                    command = new MySqlCommand(query, conn);
                    int amount = Convert.ToInt32(command.ExecuteScalar().ToString());

                    double percent = (double)bytes[i] / sum * 100;
                    if (amount > 100000)
                    {
                        Console.WriteLine(c + ". " + tableName + "  -  " + percent + " %  кол - " + amount);
                        query = getQueryInsert(tableName, percent, amount);
                        command = new MySqlCommand(query, conn);
                        command.ExecuteNonQuery();
                        c++;
                    }
                }


                Console.WriteLine("END");

            }
            catch (Exception e)
            {
                Console.WriteLine("Error: " + e.Message);
            }
            finally
            {
                //Закрытие соединенися с БЛ
                conn.Close();
            }

            Console.Read();
        }

        private static string getQueryCount(string tableName)
        {
            return "SELECT COUNT(*) FROM `" + tableName + "`;";
        }
        private static string getQueryInsert(string tableName, double percent, int amount)
        {
            return "INSERT INTO `sample` (table_name, percent_bytes, packets) VALUES ('" + tableName + "','" + percent + "','" + amount + "');"; 
        }

    }
}
