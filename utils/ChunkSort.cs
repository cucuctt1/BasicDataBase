// sort in-chunk and across chunks by a "data" field

using System;
using System.Collections.Generic;
using System.IO;
using System.Diagnostics;

namespace Utils
{
    public class ChunkSort
    {
        // Hybrid quicksort with insertion sort fallback for small partitions.
        const int InsertionSortThreshold = 16;

        public static void QuickSort<T>(T[] array, int left, int right, Comparison<T> cmp)
        {
            if (left >= right) return;

            if (right - left <= InsertionSortThreshold)
            {
                InsertionSort(array, left, right, cmp);
                return;
            }

            int i = left;
            int j = right;
            T pivot = array[(left + right) / 2];

            while (i <= j)
            {
                while (cmp(array[i], pivot) < 0) i++;
                while (cmp(array[j], pivot) > 0) j--;

                if (i <= j)
                {
                    // Swap
                    T temp = array[i];
                    array[i] = array[j];
                    array[j] = temp;
                    i++;
                    j--;
                }
            }

            // Recursion
            if (left < j)
                QuickSort(array, left, j, cmp);
            if (i < right)
                QuickSort(array, i, right, cmp);
        }

        public static void QuickSort<T>(T[] array, Comparison<T>? cmp)
        {
            if (array == null || array.Length <= 1) return;
            var comparer = cmp ?? Comparer<T>.Default.Compare;
            QuickSort(array, 0, array.Length - 1, comparer);
        }

        private static void InsertionSort<T>(T[] array, int left, int right, Comparison<T> cmp)
        {
            for (int i = left + 1; i <= right; i++)
            {
                var key = array[i];
                int j = i - 1;
                while (j >= left && cmp(array[j], key) > 0)
                {
                    array[j + 1] = array[j];
                    j--;
                }
                array[j + 1] = key;
            }
        }

        // Generic merge sort that accepts a comparison delegate
        public static T[] MergeSort<T>(T[] array, Comparison<T>? cmp)
        {
            if (array == null || array.Length <= 1)
                return array;

            var comparer = cmp ?? Comparer<T>.Default.Compare;

            int mid = array.Length / 2;
            T[] left = new T[mid];
            T[] right = new T[array.Length - mid];

            Array.Copy(array, 0, left, 0, mid);
            Array.Copy(array, mid, right, 0, array.Length - mid);

            return Merge(MergeSort(left, comparer), MergeSort(right, comparer), comparer);
        }

    private static T[] Merge<T>(T[] left, T[] right, Comparison<T> cmp)
        {
            T[] result = new T[left.Length + right.Length];
            int i = 0, j = 0, k = 0;

            while (i < left.Length && j < right.Length)
            {
                if (cmp(left[i], right[j]) <= 0)
                {
                    result[k++] = left[i++];
                }
                else
                {
                    result[k++] = right[j++];
                }
            }

            while (i < left.Length)
            {
                result[k++] = left[i++];
            }

            while (j < right.Length)
            {
                result[k++] = right[j++];
            }

            return result;
        }

        // Sort each chunk using QuickSort and then perform a k-way merge across chunks
        // The method currently assumes chunk files contain 32-bit integers written with BinaryWriter.
        public static void SortChunks(string chunkDir, Comparison<int>? cmp = null, bool writeChecksums = true, bool writeMerged = true)
        {
            var comparer = cmp ?? Comparer<int>.Default.Compare;

            // Get all chunk files
            var chunkFiles = Directory.GetFiles(chunkDir, "chunk_*.bin");
            if (chunkFiles.Length == 0) return;

            // per-chunk sort
            foreach (var chunkFile in chunkFiles)
            {
                // validate chunk before processing (skip validation if checksums disabled)
                if (writeChecksums)
                {
                    bool valid = true;
                    try { valid = BasicDataBase.TableHelper.ChunkValidator.ValidateChunk(chunkFile); } catch { valid = false; }
                    if (!valid)
                    {
                        Console.WriteLine($"Warning: chunk validation failed for {chunkFile}. Skipping.");
                        continue;
                    }
                }

                // read ints
                var dataList = new List<int>();
                using (var fs = new FileStream(chunkFile, FileMode.Open, FileAccess.Read, FileShare.Read))
                using (var reader = new BinaryReader(fs))
                {
                    while (fs.Position + 4 <= fs.Length)
                    {
                        dataList.Add(reader.ReadInt32());
                    }
                }

                // sort data
                var dataArray = dataList.ToArray();
                QuickSort(dataArray, (a, b) => comparer(a, b));

                // write sorted data back to chunk file atomically
                string tmp = chunkFile + ".tmp";
                using (var ofs = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None))
                using (var writer = new BinaryWriter(ofs))
                {
                    foreach (var data in dataArray) writer.Write(data);
                }
                try { File.Replace(tmp, chunkFile, null); }
                catch { File.Delete(chunkFile); File.Move(tmp, chunkFile); }
                if (writeChecksums) { try { BasicDataBase.TableHelper.ChunkValidator.WriteChecksum(chunkFile); } catch { } }
            }

            // cross-chunk k-way merge (external merge) into merged_all.bin
            if (!writeMerged) return;
            string mergedPath = Path.Combine(chunkDir, "merged_all.bin");

            // Prepare readers and initial queue
            var readers = new List<BinaryReader>(chunkFiles.Length);
            try
            {
                for (int i = 0; i < chunkFiles.Length; i++)
                {
                    var fs = new FileStream(chunkFiles[i], FileMode.Open, FileAccess.Read, FileShare.Read);
                    readers.Add(new BinaryReader(fs));
                }

                var pq = new PriorityQueue<(int value, int fileIndex), int>();

                for (int i = 0; i < readers.Count; i++)
                {
                    var r = readers[i];
                    var s = r.BaseStream as FileStream;
                    if (s != null && s.Position + 4 <= s.Length)
                    {
                        int v = r.ReadInt32();
                        pq.Enqueue((v, i), v);
                    }
                }

                using (var outFs = new FileStream(mergedPath, FileMode.Create, FileAccess.Write, FileShare.None))
                using (var writer = new BinaryWriter(outFs))
                {
                    while (pq.Count > 0)
                    {
                        var item = pq.Dequeue();
                        writer.Write(item.value);

                        // read next int from that reader
                        var r = readers[item.fileIndex];
                        var s = r.BaseStream as FileStream;
                        if (s != null && s.Position + 4 <= s.Length)
                        {
                            int next = r.ReadInt32();
                            pq.Enqueue((next, item.fileIndex), next);
                        }
                    }
                }
            }
            finally
            {
                foreach (var r in readers)
                {
                    try { r?.BaseStream?.Dispose(); } catch { }
                    try { r?.Dispose(); } catch { }
                }
            }
        }

        // Fixed-size row representation: 8-byte ASCII key + 5 ints
        public class Row
        {
            public string Key { get; set; } = string.Empty; // 8 chars
            public int[] Fields { get; set; } = new int[5];
            public const int KeySize = 8;
            public const int FieldCount = 5;
            public const int RowSize = KeySize + FieldCount * 4;

            public static Row Read(BinaryReader reader)
            {
                var keyBytes = reader.ReadBytes(KeySize);
                if (keyBytes.Length < KeySize) return null!; // handled by caller
                string key = System.Text.Encoding.ASCII.GetString(keyBytes);
                var fields = new int[FieldCount];
                for (int i = 0; i < FieldCount; i++) fields[i] = reader.ReadInt32();
                return new Row { Key = key, Fields = fields };
            }

            public void Write(BinaryWriter writer)
            {
                var bytes = System.Text.Encoding.ASCII.GetBytes(Key);
                if (bytes.Length < KeySize)
                {
                    var tmp = new byte[KeySize];
                    Array.Copy(bytes, tmp, bytes.Length);
                    bytes = tmp;
                }
                else if (bytes.Length > KeySize)
                {
                    var tmp = new byte[KeySize];
                    Array.Copy(bytes, tmp, KeySize);
                    bytes = tmp;
                }
                writer.Write(bytes);
                for (int i = 0; i < FieldCount; i++) writer.Write(Fields[i]);
            }
        }

        // Sort chunk files that contain fixed-size rows (8-byte key + 5 ints) by the key column.
        public static void SortChunksByKey(string chunkDir, bool writeChecksums = true, bool writeMerged = true)
        {
            var chunkFiles = Directory.GetFiles(chunkDir, "chunk_*.bin");
            if (chunkFiles.Length == 0) return;

            // per-chunk sort (rows)
            foreach (var chunkFile in chunkFiles)
            {
                var rows = new List<Row>();
                using (var fs = new FileStream(chunkFile, FileMode.Open, FileAccess.Read, FileShare.Read))
                using (var reader = new BinaryReader(fs))
                {
                    while (fs.Position + Row.RowSize <= fs.Length)
                    {
                        var r = Row.Read(reader);
                        if (r != null) rows.Add(r);
                    }
                }

                if (rows.Count > 1)
                {
                    var arr = rows.ToArray();
                    QuickSort(arr, (a, b) => string.CompareOrdinal(a.Key, b.Key));

                    // write back atomically
                    string tmp = chunkFile + ".tmp";
                    using (var fs = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None))
                    using (var writer = new BinaryWriter(fs))
                    {
                        foreach (var r in arr) r.Write(writer);
                    }
                    try { File.Replace(tmp, chunkFile, null); }
                    catch { File.Delete(chunkFile); File.Move(tmp, chunkFile); }
                    if (writeChecksums) { try { BasicDataBase.TableHelper.ChunkValidator.WriteChecksum(chunkFile); } catch { } }
                }
            }
            // k-way merge into merged_all_rows.bin
            if (!writeMerged) return;
            string mergedPath = Path.Combine(chunkDir, "merged_all_rows.bin");
            var readers = new List<BinaryReader>(chunkFiles.Length);
            try
            {
                for (int i = 0; i < chunkFiles.Length; i++)
                {
                    var fs = new FileStream(chunkFiles[i], FileMode.Open, FileAccess.Read, FileShare.Read);
                    readers.Add(new BinaryReader(fs));
                }

                var pq = new PriorityQueue<(Row row, int fileIndex), string>();

                for (int i = 0; i < readers.Count; i++)
                {
                    var r = readers[i];
                    var s = r.BaseStream as FileStream;
                    if (s != null && s.Position + Row.RowSize <= s.Length)
                    {
                        var row = Row.Read(r);
                        if (row != null) pq.Enqueue((row, i), row.Key);
                    }
                }

                using (var outFs = new FileStream(mergedPath, FileMode.Create, FileAccess.Write, FileShare.None))
                using (var writer = new BinaryWriter(outFs))
                {
                    while (pq.Count > 0)
                    {
                        var item = pq.Dequeue();
                        item.row.Write(writer);

                        var r = readers[item.fileIndex];
                        var s = r.BaseStream as FileStream;
                        if (s != null && s.Position + Row.RowSize <= s.Length)
                        {
                            var next = Row.Read(r);
                            if (next != null) pq.Enqueue((next, item.fileIndex), next.Key);
                        }
                    }
                }
            }
            finally
            {
                foreach (var r in readers)
                {
                    try { r?.BaseStream?.Dispose(); } catch { }
                    try { r?.Dispose(); } catch { }
                }
            }
        }

        // Row structure for 4 text columns (a,b,c,d) each 8 bytes
        public class TextRow
        {
            public string A { get; set; } = string.Empty;
            public string B { get; set; } = string.Empty;
            public string C { get; set; } = string.Empty;
            public string D { get; set; } = string.Empty;
            public const int ColSize = 8;
            public const int ColCount = 4;
            public const int RowSize = ColSize * ColCount;

            public static TextRow? Read(BinaryReader reader)
            {
                var buf = reader.ReadBytes(RowSize);
                if (buf.Length < RowSize) return null;
                string a = System.Text.Encoding.ASCII.GetString(buf, 0, ColSize);
                string b = System.Text.Encoding.ASCII.GetString(buf, 8, ColSize);
                string c = System.Text.Encoding.ASCII.GetString(buf, 16, ColSize);
                string d = System.Text.Encoding.ASCII.GetString(buf, 24, ColSize);
                return new TextRow { A = a, B = b, C = c, D = d };
            }

            public void Write(BinaryWriter writer)
            {
                void writeCol(string s)
                {
                    var bytes = System.Text.Encoding.ASCII.GetBytes(s);
                    if (bytes.Length < ColSize)
                    {
                        var tmp = new byte[ColSize];
                        Array.Copy(bytes, tmp, bytes.Length);
                        bytes = tmp;
                    }
                    else if (bytes.Length > ColSize)
                    {
                        var tmp = new byte[ColSize];
                        Array.Copy(bytes, tmp, ColSize);
                        bytes = tmp;
                    }
                    writer.Write(bytes);
                }

                writeCol(A);
                writeCol(B);
                writeCol(C);
                writeCol(D);
            }
        }

        // Sort chunk files containing 4 text columns by column A and measure time/memory
        public static void SortChunksByStringColumns(string chunkDir, bool printStats = true, bool writeChecksums = true, bool writeMerged = true)
        {
            var chunkFiles = Directory.GetFiles(chunkDir, "chunk_*.bin");
            if (chunkFiles.Length == 0) return;

                foreach (var chunkFile in chunkFiles)
            {
                // validate chunk before processing (optional)
                if (writeChecksums)
                {
                    bool valid = true;
                    try { valid = BasicDataBase.TableHelper.ChunkValidator.ValidateChunk(chunkFile); } catch { valid = false; }
                    if (!valid)
                    {
                        Console.WriteLine($"Warning: chunk validation failed for {chunkFile}. Skipping.");
                        continue;
                    }
                }

                var rows = new List<TextRow>();
                using (var fs = new FileStream(chunkFile, FileMode.Open, FileAccess.Read, FileShare.Read))
                using (var reader = new BinaryReader(fs))
                {
                    while (fs.Position + TextRow.RowSize <= fs.Length)
                    {
                        var r = TextRow.Read(reader);
                        if (r != null) rows.Add(r);
                    }
                }

                if (rows.Count == 0) continue;

                GC.Collect();
                long memBefore = GC.GetTotalMemory(true);
                var sw = Stopwatch.StartNew();

                var arr = rows.ToArray();
                QuickSort(arr, (x, y) => string.CompareOrdinal(x.A, y.A));

                sw.Stop();
                long memAfter = GC.GetTotalMemory(true);

                    // write back sorted chunk atomically (temp -> replace)
                    string tmp = chunkFile + ".tmp";
                    using (var fs = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None))
                    using (var writer = new BinaryWriter(fs))
                    {
                        foreach (var r in arr) r.Write(writer);
                    }
                    try { File.Replace(tmp, chunkFile, null); }
                    catch { File.Delete(chunkFile); File.Move(tmp, chunkFile); }
                    // update checksum
                    if (writeChecksums) { try { BasicDataBase.TableHelper.ChunkValidator.WriteChecksum(chunkFile); } catch { } }

                if (printStats)
                {
                    Console.WriteLine($"Sorted {rows.Count} rows in {chunkFile} in {sw.Elapsed.TotalSeconds:F3}s; memory delta {(memAfter - memBefore) / 1024.0 / 1024.0:F3} MB");
                }
            }

            // k-way merge across files into merged_all_strings.bin
            string mergedPath = Path.Combine(chunkDir, "merged_all_strings.bin");
            var readers = new List<BinaryReader>(chunkFiles.Length);
            try
            {
                for (int i = 0; i < chunkFiles.Length; i++)
                {
                    var fs = new FileStream(chunkFiles[i], FileMode.Open, FileAccess.Read, FileShare.Read);
                    readers.Add(new BinaryReader(fs));
                }

                var pq = new PriorityQueue<(TextRow row, int fileIndex), string>();
                for (int i = 0; i < readers.Count; i++)
                {
                    var r = readers[i];
                    var s = r.BaseStream as FileStream;
                    if (s != null && s.Position + TextRow.RowSize <= s.Length)
                    {
                        var row = TextRow.Read(r);
                        if (row != null) pq.Enqueue((row, i), row.A);
                    }
                }

                using (var outFs = new FileStream(mergedPath, FileMode.Create, FileAccess.Write, FileShare.None))
                using (var writer = new BinaryWriter(outFs))
                {
                    while (pq.Count > 0)
                    {
                        var item = pq.Dequeue();
                        item.row.Write(writer);

                        var r = readers[item.fileIndex];
                        var s = r.BaseStream as FileStream;
                        if (s != null && s.Position + TextRow.RowSize <= s.Length)
                        {
                            var next = TextRow.Read(r);
                            if (next != null) pq.Enqueue((next, item.fileIndex), next.A);
                        }
                    }
                }
            }
            finally
            {
                foreach (var r in readers)
                {
                    try { r?.BaseStream?.Dispose(); } catch { }
                    try { r?.Dispose(); } catch { }
                }
            }
        }
    }
}