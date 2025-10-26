using System;
using System.IO;
using System.Diagnostics;

namespace Utils
{
    public static class DummyDbTest
    {
        // Creates a test folder `tests/data/large_db` relative to the current working directory
        // and generates a dummy DB. Prints time and memory usage.
        public static void RunTest()
        {
            string cwd = Directory.GetCurrentDirectory();
            string testRoot = Path.Combine(cwd, "tests", "data", "large_db");
            Console.WriteLine($"Generating dummy DB at: {testRoot}");

            int numTables = 2;
            int chunksPerTable = 10;
            int rowsPerChunk = 100000; // 100k rows per chunk -> adjust as needed for larger tests

            Console.WriteLine($"Parameters: tables={numTables}, chunks/table={chunksPerTable}, rows/chunk={rowsPerChunk}");

            GC.Collect();
            long memBefore = GC.GetTotalMemory(true);
            var sw = Stopwatch.StartNew();

            // generate multi-column rows: key (8 chars) + 5 ints
            DummyDbGenerator.Generate(testRoot, numTables, chunksPerTable, rowsPerChunk, seed: 123, multiColumn: true);

            // For each table, run row-based chunk sort (sort by first column - key)
            for (int t = 0; t < numTables; t++)
            {
                string tableDir = Path.Combine(testRoot, $"table_{t}");
                Console.WriteLine($"Sorting chunks in {tableDir} by first column (key)...");
                ChunkSort.SortChunksByKey(tableDir);
            }

            // Run string-column benchmarks for specific record counts (single-table single-chunk)
            string benchRoot = Path.Combine(Directory.GetCurrentDirectory(), "tests", "data", "string_bench");
            Directory.CreateDirectory(benchRoot);
            int[] sizes = new int[] { 10000, 50000 };
            foreach (var sz in sizes)
            {
                string tableName = $"string_table_{sz}";
                Console.WriteLine($"Generating {sz} string-rows in {benchRoot}/{tableName}...");
                DummyDbGenerator.GenerateTextColumns(benchRoot, tableName, sz, seed: 123, writeChecksum: false);
                string tableDir = Path.Combine(benchRoot, tableName);
                Console.WriteLine($"Sorting by column A for {sz} rows...");
                ChunkSort.SortChunksByStringColumns(tableDir, printStats: true, writeChecksums: false, writeMerged: false);
                Console.WriteLine($"Finished benchmark for {sz} rows. Output at: {Path.Combine(tableDir, "merged_all_strings.bin")}");
            }

            sw.Stop();
            long memAfter = GC.GetTotalMemory(true);

            Console.WriteLine($"Generation completed in {sw.Elapsed.TotalSeconds:F2}s");
            Console.WriteLine($"Memory before: {memBefore / 1024.0 / 1024.0:F2} MB, after: {memAfter / 1024.0 / 1024.0:F2} MB, diff: {(memAfter - memBefore) / 1024.0 / 1024.0:F2} MB");

            // Show sample stats: number of files created
            int totalChunks = 0;
            for (int t = 0; t < numTables; t++)
            {
                string tableDir = Path.Combine(testRoot, $"table_{t}");
                if (Directory.Exists(tableDir))
                {
                    totalChunks += Directory.GetFiles(tableDir, "chunk_*.bin").Length;
                }
            }
            Console.WriteLine($"Total chunk files created: {totalChunks}");
        }
    }
}
