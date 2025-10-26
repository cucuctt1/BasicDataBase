using System;
using System.IO;
using System.Diagnostics;

namespace Utils
{
    public static class DummyDbGenerator
    {
        // Generate a dummy database as directories with chunk files containing 32-bit ints.
        // rootPath: path where "tests/data/large_db" (or any specified folder) will be created.
        // numTables: number of table directories to create
        // chunksPerTable: number of chunk files per table
        // rowsPerChunk: number of int rows to write per chunk
    public static void Generate(string rootPath, int numTables = 2, int chunksPerTable = 10, int rowsPerChunk = 100000, int seed = 123, bool multiColumn = false, bool writeChecksum = true)
        {
            Directory.CreateDirectory(rootPath);

            var rnd = new Random(seed);
            for (int t = 0; t < numTables; t++)
            {
                string tableName = $"table_{t}";
                string tableDir = Path.Combine(rootPath, tableName);
                Directory.CreateDirectory(tableDir);

                for (int c = 0; c < chunksPerTable; c++)
                {
                    string chunkPath = Path.Combine(tableDir, $"chunk_{c}.bin");
                    using (var fs = new FileStream(chunkPath, FileMode.Create, FileAccess.Write, FileShare.None))
                    using (var writer = new BinaryWriter(fs))
                    {
                        if (!multiColumn)
                        {
                            for (int r = 0; r < rowsPerChunk; r++)
                            {
                                // write a random int (0..int.MaxValue)
                                writer.Write(rnd.Next());
                            }
                        }
                        else
                        {
                            // write rows with: 8-byte ASCII key + 5 ints (fixed-size row = 8 + 20 = 28 bytes)
                            for (int r = 0; r < rowsPerChunk; r++)
                            {
                                string key = RandomString(rnd, 8);
                                var keyBytes = System.Text.Encoding.ASCII.GetBytes(key);
                                if (keyBytes.Length != 8)
                                {
                                    Array.Resize(ref keyBytes, 8);
                                }
                                writer.Write(keyBytes);
                                // 5 random int fields
                                for (int f = 0; f < 5; f++) writer.Write(rnd.Next());
                            }
                        }
                    }
                    // write checksum file for integrity checks (optional)
                    if (writeChecksum)
                    {
                        try { BasicDataBase.TableHelper.ChunkValidator.WriteChecksum(chunkPath); } catch { }
                    }
                }
            }
        }

        private static string RandomString(Random rnd, int length)
        {
            const string letters = "abcdefghijklmnopqrstuvwxyz";
            var buf = new char[length];
            for (int i = 0; i < length; i++) buf[i] = letters[rnd.Next(letters.Length)];
            return new string(buf);
        }

        // Generate a single-table single-chunk file containing rows with 4 text columns (a,b,c,d), each 8 chars
    public static void GenerateTextColumns(string rootPath, string tableName, int numRecords, int seed = 123, bool writeChecksum = true)
        {
            Directory.CreateDirectory(rootPath);
            var rnd = new Random(seed);
            string tableDir = Path.Combine(rootPath, tableName);
            Directory.CreateDirectory(tableDir);
            string chunkPath = Path.Combine(tableDir, "chunk_0.bin");

            using (var fs = new FileStream(chunkPath, FileMode.Create, FileAccess.Write, FileShare.None))
            using (var writer = new BinaryWriter(fs))
            {
                for (int i = 0; i < numRecords; i++)
                {
                    // write 4 columns of 8 ASCII chars each
                    for (int c = 0; c < 4; c++)
                    {
                        string s = RandomString(rnd, 8);
                        var bytes = System.Text.Encoding.ASCII.GetBytes(s);
                        if (bytes.Length < 8)
                        {
                            var tmp = new byte[8];
                            Array.Copy(bytes, tmp, bytes.Length);
                            bytes = tmp;
                        }
                        writer.Write(bytes);
                    }
                }
            }
            // write checksum for generated chunk (optional)
            if (writeChecksum)
            {
                try { BasicDataBase.TableHelper.ChunkValidator.WriteChecksum(chunkPath); } catch { }
            }
        }
    }
}
