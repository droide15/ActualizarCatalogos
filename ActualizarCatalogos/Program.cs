using System;
using System.IO;
using System.Collections.Generic;
using LumenWorks.Framework.IO.Csv;
using Npgsql;

namespace ActualizarCatalogos
{
    class Program
    {
        static void Main(string[] args)
        {

            //leer lista de archivos
            DirectoryInfo d = new DirectoryInfo(@"C:\Users\Hector Avila\Desktop\sat_doc\cfdi 3.3\ActualizarCatalogos\catalogos"); //Assuming Test is your Folder
            FileInfo[] Files = d.GetFiles("*.csv"); //Getting Text files
            string str = "";
            foreach (FileInfo file in Files)
            {
                str = str + ", " + file.Name;
            }
            Console.WriteLine(str);
            Console.ReadKey();

            //insertando las tablas
            foreach (FileInfo file in Files)
            {
                insertarTablas(file);
            }
            Console.ReadKey();
        }

        static private void insertarTablas(FileInfo file)
        {
            int fieldCount;
            string[] headers;
            string pg_campos;
            string pg_vars;
            string pg_valores;
            List<string> campostemp = new List<string>();
            List<string> varstemp = new List<string>();
            List<string> valores = new List<string>();

            //using (var connection = new NpgsqlConnection("Server=10.0.100.10;Port=5432;Database=master_itimbre_pruebas;User Id=master_itimbre_pruebas;Password=it1mbr3Pruebas;"))
            using (var connection = new NpgsqlConnection("Server=10.0.100.10;Port=5432;Database=master_itimbre;User Id=master_itimbre;Password=m4st3R1T1mBR3;"))
            {
                using (CsvReader csv =
                 new CsvReader(new StreamReader(file.FullName), true))
                {
                    Console.WriteLine("subiendo la tabla...");
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
                        varstemp.Add(":" + campotemp);
                    }
                    pg_campos = string.Join(",", campostemp);
                    pg_vars = string.Join(",", varstemp);

                    while (csv.ReadNextRecord())
                    {
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
                        pg_valores = string.Join(",", valores);

                        try
                        {
                            connection.Open();
                            NpgsqlCommand command;

                            // Create insert command.
                            command = new NpgsqlCommand("INSERT INTO " +
                             "sat_catalogos_33_nueva(" + pg_campos + ") VALUES(" + pg_vars + ")", connection);

                            // Add paramaters.
                            foreach (var campo in campostemp)
                            {
                                if (campo == "decimales" || campo == "porcentaje")
                                    command.Parameters.Add(new NpgsqlParameter(campo,
                                     NpgsqlTypes.NpgsqlDbType.Real));
                                else
                                    command.Parameters.Add(new NpgsqlParameter(campo,
                                     NpgsqlTypes.NpgsqlDbType.Varchar));
                            }

                            // Prepare the command.
                            command.Prepare();

                            // Add value to the paramater.
                            for (int i = 0; i < valores.Count; i++)
                            {
                                command.Parameters[i].Value = valores[i];
                            }

                            // Execute SQL command.
                            int recordAffected = command.ExecuteNonQuery();

                            Console.WriteLine("un dato fue subido");
                        }
                        catch (NpgsqlException ex)
                        {
                            if (!ex.ToString().Contains("duplicate key value violates unique constraint"))
                                Console.WriteLine("no se pudo subir un dato!");
                            Console.WriteLine("---ERROR---");
                            Console.WriteLine(ex);
                        }
                        connection.Close();
                    }
                    Console.WriteLine("tabla subida exitosamente!");
                }
            }
        }
    }
}