// write testing file


using BasicDataBase.FileIO;
using System;

namespace BasicDataBase.Test
{

    public class Write_Test
    {
        public static void Test()
        {
            // reset files
            System.IO.File.WriteAllBytes("test_metadata.meta", new byte[0]);
            System.IO.File.WriteAllBytes("test_data.dat", new byte[0]);

            // Define schema
            string schemaStr = "Id:int,Name:string:4,IsActive:bool,CreatedAt:datetime,Data:blob";
            Schema schema = Schema.FromString(schemaStr);
            SchemaInstruction instruction = new SchemaInstruction(schema);
            System.Collections.BitArray bits = BitGenerator.GenerateBitPadding(instruction.FieldOffsets.ToArray(), instruction.RecordSize);
            
            // Write metadata
            MetaWriter.WriteMetaData("test_metadata.meta", schema, bits);
            Console.WriteLine("Metadata written to test_metadata.meta");

            // Prepare mutiple data
            object[] data = new object[]
            {
                1,
                "John",
                true,
                DateTime.Now,
                "dir1/dir2/file.txt"
            };

            object[] data2 = new object[]
            {
                2,
                "Jane",
                false,
                DateTime.Now,
                "dir1/dir2/file.txt"
            };
            
            // 2dim object array
            object[,] dataArray = new object[,]
            {
                { 3, "Alice", true, DateTime.Now, "dir1/dir2/file.txt" },
                { 4, "Bob", false, DateTime.Now, "dir1/dir2/file.txt" }
            };
            
            // Write data to file
            FileWriter writer = new FileWriter();
            writer.Write2DataFile("test_metadata.meta", "test_data.dat", schemaStr, data);
            writer.Write2DataFile("test_metadata.meta", "test_data.dat", schemaStr, data2);
            writer.Write2DataFile("test_metadata.meta", "test_data.dat", schemaStr, dataArray);
            Console.WriteLine("Data written to test_data.dat");
        }
    }
}