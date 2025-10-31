using System;
using BasicDataBase.FileIO;
using BasicDataBase.Index;

namespace BasicDataBase.Test
{
    public class Search_Test
    {
        // Create sample data and build a name index using BPlusTree, then search for 'Alice'
        public static void Test()
        {
            // cleanup
            foreach (var f in new[]{ "search_metadata.meta", "search_data.dat" })
            {
                try { if (System.IO.File.Exists(f)) System.IO.File.Delete(f); } catch { }
            }

            // schema: use Name as username
            string schemaStr = "Id:int,username:string:16,IsActive:bool,CreatedAt:datetime,Data:blob";
            Schema schema = Schema.FromString(schemaStr);
            SchemaInstruction instruction = new SchemaInstruction(schema);
            var bits = BitGenerator.GenerateBitPadding(instruction.FieldOffsets.ToArray(), instruction.RecordSize);
            MetaWriter.WriteMetaData("search_metadata.meta", schema, bits);

            // write sample records
            var names = new string[] { "Alice", "Bob", "Charlie", "Alice_2", "David", "Alice" };
            for (int i = 0; i < names.Length; i++)
            {
                var rec = new object[] { i + 1, names[i], i % 2 == 0, DateTime.Now, $"f{i}.bin" };
                FileIOManager.AppendRecord("search_metadata.meta", "search_data.dat", rec);
            }

            // Build index on username using IndexManager
            var manager = new IndexManager("search_metadata.meta", "search_data.dat");
            manager.BuildIndex("username");

            // Search exact 'Alice'
            var found = manager.SearchExact("username", "Alice");
            Console.WriteLine($"Search exact 'Alice' -> {found.Count} hits");
            foreach (var idx in found)
            {
                var rec = FileIOManager.ReadRecord("search_metadata.meta", "search_data.dat", idx);
                Console.WriteLine($"Row {idx}: {rec?[0]} | {rec?[1]} | {rec?[2]}");
            }

            // Search prefix 'Alice'
            var pref = manager.SearchPrefix("username", "Alice");
            Console.WriteLine($"Search prefix 'Alice' -> {pref.Count} hits");
            foreach (var idx in pref)
            {
                var rec = FileIOManager.ReadRecord("search_metadata.meta", "search_data.dat", idx);
                Console.WriteLine($"Row {idx}: {rec?[0]} | {rec?[1]}");
            }
        }
    }
}
