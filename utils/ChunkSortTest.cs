using System;
using System.IO;
using System.Collections.Generic;

namespace Utils
{
    public static class ChunkSortTest
    {
        public static void RunTest()
        {
            string baseDir = AppContext.BaseDirectory;
            string chunkDir = Path.Combine(baseDir, "test_chunks");
            Directory.CreateDirectory(chunkDir);

            // create some chunk files with random ints
            var rnd = new Random(123);
            int chunks = 3;
            int perChunk = 20;

            for (int i = 0; i < chunks; i++)
            {
                string path = Path.Combine(chunkDir, $"chunk_{i}.bin");
                using (var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None))
                using (var writer = new BinaryWriter(fs))
                {
                    for (int j = 0; j < perChunk; j++)
                    {
                        writer.Write(rnd.Next(0, 1000));
                    }
                }
            }

            Console.WriteLine($"Created {chunks} chunk files in {chunkDir}");

            // Sort chunks and merge (skip checksum validation for test data)
            ChunkSort.SortChunks(chunkDir, writeChecksums: false, writeMerged: true);

            string merged = Path.Combine(chunkDir, "merged_all.bin");
            if (!File.Exists(merged))
            {
                Console.WriteLine("Merged output not found.");
                return;
            }

            // read first 50 values from merged file and print
            using (var fs = new FileStream(merged, FileMode.Open, FileAccess.Read, FileShare.Read))
            using (var reader = new BinaryReader(fs))
            {
                int count = 0;
                Console.WriteLine("First 50 values of merged file:");
                while (fs.Position + 4 <= fs.Length && count < 50)
                {
                    Console.Write(reader.ReadInt32());
                    Console.Write(" ");
                    count++;
                }
                Console.WriteLine();
            }
        }
    }
}
