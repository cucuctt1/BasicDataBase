using System;
using System.Collections.Generic;

namespace BasicDataBase.Index
{
    // Lightweight in-memory B+ tree-like index wrapper.
    // For now this is a simple wrapper around a sorted dictionary that
    // stores key -> list of record ids. It provides Insert and Search (exact match)
    // and SearchPrefix for prefix lookups. This acts as a drop-in index used by tests.
    public class BPlusTree
    {
        private readonly SortedDictionary<string, List<int>> dict = new SortedDictionary<string, List<int>>(StringComparer.Ordinal);

        // Insert key -> recordId
        public void Insert(string key, int recordId)
        {
            if (!dict.TryGetValue(key, out var list))
            {
                list = new List<int>();
                dict[key] = list;
            }
            list.Add(recordId);
        }

        // Exact match search
        public List<int> Search(string key)
        {
            if (dict.TryGetValue(key, out var list)) return new List<int>(list);
            return new List<int>();
        }

        // Prefix search (returns record ids for keys that start with prefix)
        public List<int> SearchPrefix(string prefix)
        {
            var result = new List<int>();
            foreach (var kv in dict)
            {
                if (kv.Key.StartsWith(prefix, StringComparison.Ordinal))
                {
                    result.AddRange(kv.Value);
                }
            }
            return result;
        }

        // Simple count of distinct keys
        public int KeyCount => dict.Count;
    }
}
