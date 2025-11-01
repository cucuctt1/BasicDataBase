using System;
using System.Diagnostics;
using BasicDataBase.FileIO;

namespace BasicDataBase.Test
{
    public static class Benchmark_Test
    {
        // Configure medium size here
        public static int DefaultCount = 10000; // medium dataset

        public static void Test(int count = -1)
        {
            if (count <= 0) count = DefaultCount;

            Console.WriteLine($"Benchmark: generating {count} records (medium)");

            // cleanup
            foreach (var f in new[]{ "bench_metadata.meta", "bench_data.dat" })
            {
                try { if (System.IO.File.Exists(f)) System.IO.File.Delete(f); } catch { }
            }

            // prepare schema
            string schemaStr = "Id:int,Name:string:16,IsActive:bool,CreatedAt:datetime,Data:blob";
            Schema schema = Schema.FromString(schemaStr);
            SchemaInstruction instruction = new SchemaInstruction(schema);
            var bits = BitGenerator.GenerateBitPadding(instruction.FieldOffsets.ToArray(), instruction.RecordSize);
            MetaWriter.WriteMetaData("bench_metadata.meta", schema, bits);

            var rnd = new Random(12345);

            // Prepare a small pool of names
            string[] names = new string[] { "Alex", "Ben", "Cara", "Dana", "Eli", "Faye", "Gus", "Hana", "Ira", "Jill" };

            object[] CreateRecord(int id)
            {
                return new object[]
                {
                    id,
                    names[rnd.Next(names.Length)],
                    rnd.NextDouble() > 0.5,
                    DateTime.UtcNow,
                    $"file_{rnd.Next(1000)}.bin"
                };
            }

            // Write benchmark: append count records
            var sw = Stopwatch.StartNew();
            for (int i = 0; i < count; i++)
            {
                FileIOManager.AppendRecord("bench_metadata.meta", "bench_data.dat", CreateRecord(i + 1));
            }
            sw.Stop();
            Console.WriteLine($"Write: {count} records -> {sw.Elapsed.TotalSeconds:F3}s");

            // measure file size
            var fi = new System.IO.FileInfo("bench_data.dat");
            Console.WriteLine($"Data file size: {fi.Length / 1024.0:F2} KB");

            // Read all
            var swr = Stopwatch.StartNew();
            var all = FileIOManager.ReadAll("bench_metadata.meta", "bench_data.dat");
            swr.Stop();
            Console.WriteLine($"ReadAll: rows={all.GetLength(0)}, cols={all.GetLength(1)} in {swr.Elapsed.TotalSeconds:F3}s");

            // Edit: update middle record
            int mid = count / 2;
            var editRec = new object[] { mid + 1, "EditedName", true, DateTime.UtcNow, "edited.bin" };
            var swe = Stopwatch.StartNew();
            FileIOManager.EditRecord("bench_metadata.meta", "bench_data.dat", mid, editRec);
            swe.Stop();
            Console.WriteLine($"Edit (append+delete old) at index {mid}: {swe.Elapsed.TotalSeconds:F3}s");

            // Delete: delete middle record (post-edit)
            var swdMid = Stopwatch.StartNew();
            FileIOManager.DeleteRecordByIndex("bench_metadata.meta", "bench_data.dat", mid);
            swdMid.Stop();
            Console.WriteLine($"Delete middle record: {swdMid.Elapsed.TotalSeconds:F3}s");
            count--;

            // Random edits
            int editOps = Math.Max(10, Math.Min(200, count / 20));
            var editIndices = new List<int>(editOps);
            for (int i = 0; i < editOps; i++) editIndices.Add(rnd.Next(0, Math.Max(1, count)));
            editIndices.Sort();
            var swRandomEdit = Stopwatch.StartNew();
            foreach (var idx in editIndices)
            {
                var rec = new object[] { idx + 1, $"RandEdit_{rnd.Next(1000)}", rnd.NextDouble() > 0.5, DateTime.UtcNow, $"rand_{rnd.Next(1000)}.bin" };
                FileIOManager.EditRecord("bench_metadata.meta", "bench_data.dat", idx, rec);
            }
            swRandomEdit.Stop();
            Console.WriteLine($"Random edits ({editOps} ops): {swRandomEdit.Elapsed.TotalSeconds:F3}s");

            // Random deletes (sorted descending to keep indexes stable)
            int deleteOps = Math.Max(10, Math.Min(200, count / 25));
            var deleteIndices = new HashSet<int>();
            while (deleteIndices.Count < deleteOps)
            {
                deleteIndices.Add(rnd.Next(0, Math.Max(1, count)));
            }
            var deleteList = deleteIndices.ToList();
            deleteList.Sort();
            deleteList.Reverse();
            var swRandomDelete = Stopwatch.StartNew();
            foreach (var idx in deleteList)
            {
                FileIOManager.DeleteRecordByIndex("bench_metadata.meta", "bench_data.dat", idx);
                count--;
            }
            swRandomDelete.Stop();
            Console.WriteLine($"Random deletes ({deleteOps} ops): {swRandomDelete.Elapsed.TotalSeconds:F3}s");

            // Append additional records (simulating inserts after churn)
            int addOps = deleteOps;
            var swAdd = Stopwatch.StartNew();
            for (int i = 0; i < addOps; i++)
            {
                FileIOManager.AppendRecord("bench_metadata.meta", "bench_data.dat", CreateRecord(count + i + 1));
            }
            swAdd.Stop();
            Console.WriteLine($"Random adds ({addOps} ops): {swAdd.Elapsed.TotalSeconds:F3}s");
            count += addOps;

            // Final size
            var fi2 = new System.IO.FileInfo("bench_data.dat");
            Console.WriteLine($"Final data file size: {fi2.Length / 1024.0:F2} KB");

            Console.WriteLine("Benchmark complete.");
        }
    }
}
