using System;
using System.IO;
using System.Collections.Generic;
using LumenWorks.Framework.IO.Csv;
using MySql.Data.MySqlClient;

namespace ActualizarCatalogos
{
    class Program
    {
        static void Main(string[] args)
        {
            //leer lista de archivos
            DirectoryInfo d = new DirectoryInfo(@"..\catalogos"); //Assuming Test is your Folder
            FileInfo[] Files = d.GetFiles("*.csv"); //Getting Text files
            string str = "";
            foreach (FileInfo file in Files)
            {
                str = str + ", " + file.Name;
            }
            Console.WriteLine(str);
            Console.ReadKey();
            
            //lista de excepciones
            List<string> ex_rep = new List<string>();
            //leer excepciones a ignorar
            string[] excepciones = System.IO.File.ReadAllLines(@"..\catalogos\ignorar.txt");
            List<string> ex_ign = new List<string>();
            foreach (string excepcion in excepciones)
                if (!excepcion.StartsWith("//"))
                    ex_ign.Add(excepcion);

            //insertando las tablas
            foreach (FileInfo file in Files)
            {
                insertarTablas(file, ex_ign, ex_rep);

                //reportando excepciones ocurridas
                if (ex_rep.Count > 0)
                    using (var sw = new StreamWriter(File.Open(@"..\catalogos\" + file.Name.Replace(".csv", ".log"), FileMode.Create)))
                        foreach (string excepcion in ex_rep)
                            sw.WriteLine(excepcion);

                ex_rep.Clear();
            }
            Console.ReadKey();
        }

        static private void insertarTablas(FileInfo file, List<string> ex_ign, List<string> ex_rep)
        {
            using (CsvReader csv = new CsvReader(new StreamReader(file.FullName), true))
            {
                int fieldCount;
                string[] headers;
                string ms_campos;
                string ms_vars;
                string ms_valores;
                List<string> campostemp = new List<string>();
                List<string> varstemp = new List<string>();
                List<string> valores = new List<string>();

                Console.WriteLine("...............................................");
                Console.WriteLine("...procesando archivo " + file.FullName + " ...");
                Console.WriteLine("...............................................");
                fieldCount = csv.FieldCount;
                headers = csv.GetFieldHeaders();
                campostemp.Add("cata_cata");
                campostemp.Add("cata_llave");
                for (int i = 1; i < fieldCount; i++)
                {
                    campostemp.Add(headers[i]);
                }
                foreach (var campotemp in campostemp)
                {
                    varstemp.Add("@" + campotemp);
                }
                ms_campos = string.Join(",", campostemp);
                ms_vars = string.Join(",", varstemp);

                using (var connection = new MySqlConnection(@"server=mysql4.gear.host;userid=programamestadb;password=Ot0S2-PVBA~A;database=programamestadb"))
                {

                    while (csv.ReadNextRecord())
                    {
                        string linea = "";

                        if (string.IsNullOrWhiteSpace(csv[0]))
                            continue;

                        valores.Clear();

                        valores.Add(headers[0]);
                        valores.Add(csv[0]);
                        for (int i = 1; i < fieldCount; i++)
                        {
                            string campo = headers[i];
                            string dato = csv[i];
                            if (campo == "porcentaje")
                                if (string.IsNullOrWhiteSpace(dato))
                                    dato = "0.00";
                                else
                                    dato = dato.Replace("%", "");
                            valores.Add(dato);
                        }
                        ms_valores = string.Join(",", valores);

                        try
                        {
                            if (connection.State != System.Data.ConnectionState.Open)
                            {
                                connection.Close();
                                connection.Open();
                            }

                            MySqlCommand command;

                            // Create insert command.
                            command = new MySqlCommand("INSERT INTO " +
                             "sat_catalogos_33(" + ms_campos + ") VALUES(" + ms_vars + ")", connection);

                            // Prepare the command.
                            command.Prepare();

                            // Add paramaters.
                            for (int i = 0; i < campostemp.Count; i++)
                                command.Parameters.AddWithValue(campostemp[i], valores[i]);

                            // Execute SQL command.
                            int recordAffected = command.ExecuteNonQuery();

                            Console.WriteLine(".");
                        }
                        catch (Exception ex)
                        {
                            linea = "";
                            for (int i = 0; i < fieldCount; i++)
                                linea += csv[i] + ",";
                            ex_rep.Add(file.Name + "|" + linea + "|" + ex.Message);
                            Console.WriteLine("---ERROR---");
                            Console.WriteLine(ex);
                            if (!ex_ign.Contains(ex.Message))
                            {
                                if (connection != null && connection.State != System.Data.ConnectionState.Closed)
                                    connection.Close();
                                break;
                            }
                        }
                    }
                    Console.WriteLine("---------------------------------------");
                    Console.WriteLine("archivo " + file.FullName + " procesado");
                    Console.WriteLine("---------------------------------------");
                }
            }
        }
    }
}