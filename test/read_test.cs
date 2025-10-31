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

            // read all using FileIOManager (use test metadata file)
            object[,] records = FileIOManager.ReadAll("test_metadata.meta", "test_data.dat");

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


            //delete record
            
            // delete record at index 2 (third record)
            FileIOManager.DeleteRecordByIndex("test_metadata.meta", "test_data.dat", 2);
            Console.WriteLine("Record at index 2 deleted.");
            // re-read to show updated content
            records = FileIOManager.ReadAll("test_metadata.meta", "test_data.dat");
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
            //edit test
            // FileWriter writer = new FileWriter();
            // object[] newData = new object[]
            // {
            //     10,
            //     "UpdatedName",
            //     true,
            //     DateTime.Now,
            //     "updated/dir/file.txt"
            // };
            // writer.EditDataFile("test_metadata.meta", "test_data.dat", schemaStr, 0, newData);
            // Console.WriteLine("Record at index 0 updated.");

            // records = FileReader.ReadAllData("metadata.meta", "test_data.dat");

            // Console.WriteLine("Read Records:");
            // for (int i = 0; i < records.GetLength(0); i++)
            // {
            //     var record = new object[records.GetLength(1)];
            //     for (int j = 0; j < records.GetLength(1); j++)
            //     {
            //         record[j] = records[i, j];

            //         Console.Write(records[i, j] + "\t");
            //     }
            //     Console.WriteLine();
            // }

        }
    }
}