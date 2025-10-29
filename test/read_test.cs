// read test

using BasicDataBase.FileIO;
using System;

namespace BasicDataBase.Test
{

    public class Read_Test
    {
        public static void Test()
        {
            string schemaStr = "Id:int,Name:string:4,IsActive:bool,CreatedAt:datetime,Data:blob";
            Schema schema = Schema.FromString(schemaStr);
            SchemaInstruction instruction = new SchemaInstruction(schema);

            object[,] records = FileReader.ReadAllData("metadata.meta", "test_data.dat");

            Console.WriteLine("Read Records:");
            for (int i = 0; i < records.GetLength(0); i++)
            {
                var record = new object[records.GetLength(1)];
                for (int j = 0; j < records.GetLength(1); j++)
                {
                    record[j] = records[i, j];

                    Console.Write(records[i, j] + "\t");
                }
                Console.WriteLine();
            }
        }
    }
}