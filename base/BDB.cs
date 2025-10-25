// basic database from scratch with B-tree implementation
using System;
using System.Collections.Generic;
using BasicDataBase.TableHelper;


namespace BasicDataBase.baseBDB
{
    public class BDB
    {


        private string DefaultDatabaseName = "default_db";
        
        public void Init(string databaseName)
        {
            // create database directory if not exists
        
            System.IO.Directory.CreateDirectory(databaseName);
            //create dict bin file
            System.IO.File.Create(System.IO.Path.Combine(databaseName, "table_dict.bin")).Close();

            // assign default database name
            DefaultDatabaseName = databaseName;    
        }

        private void AddTable2Dict(string tableName)
        {
            // create dict file in databse dir if not exists
            if (!System.IO.File.Exists(System.IO.Path.Combine(DefaultDatabaseName, "table_dict.bin")))
            {
                System.IO.File.Create(System.IO.Path.Combine(DefaultDatabaseName, "table_dict.bin")).Close();
            }

            string dictFilePath = System.IO.Path.Combine(DefaultDatabaseName, "table_dict.bin");
            using (var fs = new System.IO.FileStream(dictFilePath, System.IO.FileMode.Append, System.IO.FileAccess.Write))
            using (var writer = new System.IO.BinaryWriter(fs))
            {
                writer.Write(tableName);
            }
        }

        ///////////////////////////////////// database setup operations /////////////////////////////////////
        public void createTable(string tableName)
        {
            // create table directory if not exists
            System.IO.Directory.CreateDirectory(System.IO.Path.Combine(DefaultDatabaseName, tableName));

            AddTable2Dict(tableName);
        }
        public void InitStructure(string tableName, string[] fieldNames, string[] fieldTypes)
        {
            string structFilePath = System.IO.Path.Combine(DefaultDatabaseName, tableName, "structure.bin");
            using (var fs = new System.IO.FileStream(structFilePath, System.IO.FileMode.Create, System.IO.FileAccess.Write))
            using (var writer = new System.IO.BinaryWriter(fs))
            {
                for (int i = 0; i < fieldNames.Length; i++)
                {
                    writer.Write(fieldNames[i]);
                    writer.Write(fieldTypes[i]);
                }
            }
        }
        
        
        //////////////////////////////////// database query operations ///////////////////////////////////////
        public Array ListTables()
        {
            string dictFilePath = System.IO.Path.Combine(DefaultDatabaseName, "table_dict.bin");
            if (!System.IO.File.Exists(dictFilePath))
            {
                return new string[0];
            }

            var tableNames = new System.Collections.Generic.List<string>();
            using (var fs = new System.IO.FileStream(dictFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read))
            using (var reader = new System.IO.BinaryReader(fs))
            {
                while (fs.Position < fs.Length)
                {
                    string tableName = reader.ReadString();
                    tableNames.Add(tableName);
                }
            }
            return tableNames.ToArray();
        }
    }   
}