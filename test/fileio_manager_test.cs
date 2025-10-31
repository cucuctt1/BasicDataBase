using BasicDataBase.FileIO;
using System;

namespace BasicDataBase.Test
{
    public class FileIOManager_Test
    {
        public static void Test()
        {
            // remove any pre-existing test files to ensure a clean run
            foreach (var f in new[]{ "metadata.meta", "test_metadata.meta", "unit_metadata.meta", "test_data.dat", "unit_data.dat" })
            {
                try { if (System.IO.File.Exists(f)) System.IO.File.Delete(f); } catch { }
            }

            // prepare metadata and empty data file
            System.IO.File.WriteAllBytes("unit_metadata.meta", new byte[0]);
            System.IO.File.WriteAllBytes("unit_data.dat", new byte[0]);

            string schemaStr = "Id:int,Name:string:4,IsActive:bool,CreatedAt:datetime,Data:blob";
            Schema schema = Schema.FromString(schemaStr);
            SchemaInstruction instruction = new SchemaInstruction(schema);
            var bits = BitGenerator.GenerateBitPadding(instruction.FieldOffsets.ToArray(), instruction.RecordSize);
            MetaWriter.WriteMetaData("unit_metadata.meta", schema, bits);

            // schema written to unit_metadata.meta

            var r1 = new object[] { 1, "A", true, DateTime.Now, "p1" };
            var r2 = new object[] { 2, "B", false, DateTime.Now, "p2" };
            var r3 = new object[] { 3, "C", true, DateTime.Now, "p3" };

            FileIOManager.AppendRecord("unit_metadata.meta", "unit_data.dat", r1);
            FileIOManager.AppendRecord("unit_metadata.meta", "unit_data.dat", r2);
            FileIOManager.AppendRecord("unit_metadata.meta", "unit_data.dat", r3);

            // All Records After Append (decoded)
            var all = FileIOManager.ReadAll("unit_metadata.meta", "unit_data.dat");
            for (int i = 0; i < all.GetLength(0); i++)
            {
                for (int j = 0; j < all.GetLength(1); j++) Console.Write(all[i, j] + "\t");
                Console.WriteLine();
            }

            // Read single record
            var rec1 = FileIOManager.ReadRecord("unit_metadata.meta", "unit_data.dat", 1);
            Console.WriteLine("--- ReadRecord index 1 ---");
            Console.WriteLine(string.Join(" | ", rec1));

            // Edit record 1 (append new record and delete old)
            var newRec = new object[] { 2, "B-updated", false, DateTime.Now, "p2" };
            FileIOManager.EditRecord("unit_metadata.meta", "unit_data.dat", 1, newRec);
            Console.WriteLine("Edited record index 1 (append+delete old)");

            var afterEdit = FileIOManager.ReadAll("unit_metadata.meta", "unit_data.dat");
            Console.WriteLine("--- All Records After Edit ---");
            for (int i = 0; i < afterEdit.GetLength(0); i++)
            {
                for (int j = 0; j < afterEdit.GetLength(1); j++) Console.Write(afterEdit[i, j] + "\t");
                Console.WriteLine();
            }

            // Delete record index 0
            FileIOManager.DeleteRecordByIndex("unit_metadata.meta", "unit_data.dat", 0);
            Console.WriteLine("Deleted record index 0");
            var afterDelete = FileIOManager.ReadAll("unit_metadata.meta", "unit_data.dat");
            Console.WriteLine("--- All Records After Delete ---");
            for (int i = 0; i < afterDelete.GetLength(0); i++)
            {
                for (int j = 0; j < afterDelete.GetLength(1); j++) Console.Write(afterDelete[i, j] + "\t");
                Console.WriteLine();
            }
        }
    }
}
