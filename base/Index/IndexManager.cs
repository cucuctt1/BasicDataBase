using System;
using System.Collections.Generic;
using BasicDataBase.FileIO;

namespace BasicDataBase.Index
{
    // High-level index manager for tables. Builds an index on a named field
    // and provides search helpers that return record indexes or full records.
    public class IndexManager
    {
        private readonly string metadataPath;
        private readonly string dataPath;
        private readonly Dictionary<string, BPlusTree> indexes = new Dictionary<string, BPlusTree>(StringComparer.OrdinalIgnoreCase);

        public IndexManager(string metadataPath, string dataPath)
        {
            this.metadataPath = metadataPath ?? throw new ArgumentNullException(nameof(metadataPath));
            this.dataPath = dataPath ?? throw new ArgumentNullException(nameof(dataPath));
        }

        // Build an index on the named field (e.g., "username"), returns the number of indexed rows
        public int BuildIndex(string fieldName)
        {
            if (string.IsNullOrEmpty(fieldName)) throw new ArgumentException("fieldName required");

            var schema = Schema.FromString(System.IO.File.ReadAllText(metadataPath).Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries)[0]);
            int fieldPos = -1;
            for (int i = 0; i < schema.Fields.Count; i++)
            {
                if (schema.Fields[i].Name.Equals(fieldName, StringComparison.OrdinalIgnoreCase)) { fieldPos = i; break; }
            }
            if (fieldPos < 0) throw new ArgumentException($"Field '{fieldName}' not found in schema");

            var all = FileIOManager.ReadAll(metadataPath, dataPath);
            var tree = new BPlusTree();
            for (int r = 0; r < all.GetLength(0); r++)
            {
                var key = all[r, fieldPos]?.ToString() ?? string.Empty;
                tree.Insert(key, r);
            }

            indexes[fieldName] = tree;
            return all.GetLength(0);
        }

        // Exact search on a previously built index. Returns record indexes.
        public List<int> SearchExact(string fieldName, string key)
        {
            if (!indexes.TryGetValue(fieldName, out var tree)) throw new InvalidOperationException($"Index for '{fieldName}' not built");
            return tree.Search(key);
        }

        // Prefix search on a previously built index. Returns record indexes.
        public List<int> SearchPrefix(string fieldName, string prefix)
        {
            if (!indexes.TryGetValue(fieldName, out var tree)) throw new InvalidOperationException($"Index for '{fieldName}' not built");
            return tree.SearchPrefix(prefix);
        }

        // Convenience: return records (object arrays) for exact search results
        public List<object[]?> SearchRecordsExact(string fieldName, string key)
        {
            var ids = SearchExact(fieldName, key);
            var result = new List<object[]?>();
            foreach (var id in ids)
            {
                result.Add(FileIOManager.ReadRecord(metadataPath, dataPath, id));
            }
            return result;
        }
    }
}
