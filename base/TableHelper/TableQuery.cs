using System;
using System.Collections.Generic;
using System.IO;

namespace BasicDataBase.TableHelper
{
    public class TableQuery
    {
        private readonly string _databaseName;
        private readonly string _tableName;
        private readonly TableOperate _operate;
        private List<RowPointer> _results = new List<RowPointer>();

        public TableQuery(string databaseName, string tableName)
        {
            _databaseName = databaseName;
            _tableName = tableName;
            _operate = new TableOperate(databaseName, tableName);
        }

        // Load all pointers (build index if necessary)
        public TableQuery SelectAll()
        {
            var idx = _operate.BuildIndex();
            // iterate index is not exposed; instead read all chunk files and add pointers
            string tableDir = Path.Combine(_databaseName, _tableName);
            var chunkFiles = Directory.GetFiles(tableDir, "chunk_*.bin");
            Array.Sort(chunkFiles);
            foreach (var chunk in chunkFiles)
            {
                int rows = _operate.GetRowCountInChunk(chunk);
                for (int i = 0; i < rows; i++) _results.Add(new RowPointer(chunk, i));
            }
            return this;
        }

        public TableQuery SelectIf(Func<object[], bool> predicate)
        {
            var filtered = new List<RowPointer>();
            foreach (var p in _results)
            {
                var row = _operate.ReadRowByPointer(p);
                if (predicate(row)) filtered.Add(p);
            }
            _results = filtered;
            return this;
        }

        // select by equality of first column (string key)
        public TableQuery SelectByKey(string key)
        {
            var idx = _operate.BuildIndex();
            var pointers = idx.Search(key);
            _results = pointers ?? new List<RowPointer>();
            return this;
        }

        // Select keys between inclusive bounds.
        public TableQuery SelectRange(string fromKey, string toKey)
        {
            // simple scan approach: build index then scan chunk files
            _results = new List<RowPointer>();
            var idx = _operate.BuildIndex();
            string tableDir = Path.Combine(_databaseName, _tableName);
            var chunkFiles = Directory.GetFiles(tableDir, "chunk_*.bin");
            Array.Sort(chunkFiles);
            foreach (var chunk in chunkFiles)
            {
                using var fs = new FileStream(chunk, FileMode.Open, FileAccess.Read, FileShare.Read);
                using var reader = new BinaryReader(fs);
                int rowIndex = 0;
                while (fs.Position < fs.Length)
                {
                    if (fs.Length - fs.Position >= 8)
                    {
                        var keyBytes = reader.ReadBytes(8);
                        if (keyBytes.Length < 8) break;
                        string key = System.Text.Encoding.ASCII.GetString(keyBytes);
                        // skip rest of row depending on size (attempt to skip 24 bytes)
                        long remain = Math.Min(24, fs.Length - fs.Position);
                        fs.Seek(remain, SeekOrigin.Current);
                        if (string.CompareOrdinal(key, fromKey) >= 0 && string.CompareOrdinal(key, toKey) <= 0)
                        {
                            _results.Add(new RowPointer(chunk, rowIndex));
                        }
                        rowIndex++;
                        continue;
                    }
                    break;
                }
            }
            return this;
        }

        public TableQuery Limit(int n)
        {
            if (n < _results.Count) _results = _results.GetRange(0, n);
            return this;
        }

        public int Count()
        {
            return _results.Count;
        }

        public TableQuery OrderByColumn(int colIndex)
        {
            _results.Sort((a, b) =>
            {
                var ra = _operate.ReadRowByPointer(a);
                var rb = _operate.ReadRowByPointer(b);
                object? va = colIndex < ra.Length ? ra[colIndex] : null;
                object? vb = colIndex < rb.Length ? rb[colIndex] : null;
                return Comparer<object?>.Default.Compare(va, vb);
            });
            return this;
        }

        public List<object[]> ToList()
        {
            var list = new List<object[]>();
            foreach (var p in _results)
            {
                list.Add(_operate.ReadRowByPointer(p));
            }
            return list;
        }
    }
}
