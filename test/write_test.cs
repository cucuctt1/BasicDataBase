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
            
            

            // Write data to file using FileIOManager
            FileIOManager.AppendRecord("test_metadata.meta", "test_data.dat", data);
            FileIOManager.AppendRecord("test_metadata.meta", "test_data.dat", data2);
            // write the 2D array rows
            for (int r = 0; r < dataArray.GetLength(0); r++)
            {
                var row = new object[dataArray.GetLength(1)];
                for (int c = 0; c < dataArray.GetLength(1); c++) row[c] = dataArray[r, c];
                FileIOManager.AppendRecord("test_metadata.meta", "test_data.dat", row);
            }
            // data written
            
        }
    }
}