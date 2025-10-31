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

            // Write benchmark: append count records
            var sw = Stopwatch.StartNew();
            for (int i = 0; i < count; i++)
            {
                var rec = new object[] {
                    i + 1,
                    names[rnd.Next(names.Length)],
                    (rnd.NextDouble() > 0.5),
                    DateTime.UtcNow,
                    $"file_{rnd.Next(1000)}.bin"
                };
                FileIOManager.AppendRecord("bench_metadata.meta", "bench_data.dat", rec);
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
            var newRec = new object[] { mid + 1, "EditedName", true, DateTime.UtcNow, "edited.bin" };
            var swe = Stopwatch.StartNew();
            FileIOManager.EditRecord("bench_metadata.meta", "bench_data.dat", mid, newRec);
            swe.Stop();
            Console.WriteLine($"Edit (append+delete old) at index {mid}: {swe.Elapsed.TotalSeconds:F3}s");

            // Delete: delete last record
            var swd = Stopwatch.StartNew();
            FileIOManager.DeleteRecordByIndex("bench_metadata.meta", "bench_data.dat", count - 1);
            swd.Stop();
            Console.WriteLine($"Delete last record: {swd.Elapsed.TotalSeconds:F3}s");

            // Final size
            var fi2 = new System.IO.FileInfo("bench_data.dat");
            Console.WriteLine($"Final data file size: {fi2.Length / 1024.0:F2} KB");

            Console.WriteLine("Benchmark complete.");
        }
    }
}
