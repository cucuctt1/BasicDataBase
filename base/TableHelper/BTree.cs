using System;
using System.Collections.Generic;

namespace BasicDataBase.TableHelper
{
    // Simple B-Tree implementation storing string keys and lists of pointers (generic T)
    public class BTree<TPointer>
    {
        private readonly int _t; // minimum degree
        private Node _root;

        private class Node
        {
            public List<string> Keys = new List<string>();
            public List<object> Values = new List<object>(); // for leaves: List<TPointer>
            public List<Node> Children = new List<Node>();
            public bool Leaf => Children.Count == 0;
        }

        public BTree(int degree = 32)
        {
            if (degree < 2) throw new ArgumentException("degree must be >= 2");
            _t = degree;
            _root = new Node();
        }

        // Search returns list of pointers or null
        public List<TPointer>? Search(string key)
        {
            return SearchNode(_root, key);
        }

        private List<TPointer>? SearchNode(Node node, string key)
        {
            int i = 0;
            while (i < node.Keys.Count && string.CompareOrdinal(key, node.Keys[i]) > 0) i++;
            if (i < node.Keys.Count && string.CompareOrdinal(key, node.Keys[i]) == 0)
            {
                if (node.Leaf)
                {
                    return node.Values[i] as List<TPointer>;
                }
                else
                {
                    // in our design, duplicates reside in leaves; descend
                    return SearchNode(node.Children[i + 1], key);
                }
            }
            if (node.Leaf) return null;
            return SearchNode(node.Children[i], key);
        }

        public void Insert(string key, TPointer pointer)
        {
            var r = _root;
            if (r.Keys.Count == 2 * _t - 1)
            {
                var s = new Node();
                _root = s;
                s.Children.Add(r);
                SplitChild(s, 0);
                InsertNonFull(s, key, pointer);
            }
            else
            {
                InsertNonFull(r, key, pointer);
            }
        }

        private void SplitChild(Node parent, int index)
        {
            var y = parent.Children[index];
            var z = new Node();
            int t = _t;

            // move last t-1 keys of y to z
            for (int j = 0; j < t - 1; j++)
            {
                z.Keys.Add(y.Keys[t]);
                y.Keys.RemoveAt(t);
                // move values as well for leaves
                if (y.Leaf)
                {
                    z.Values.Add(y.Values[t]);
                    y.Values.RemoveAt(t);
                }
            }

            if (!y.Leaf)
            {
                for (int j = 0; j < t; j++)
                {
                    z.Children.Add(y.Children[t]);
                    y.Children.RemoveAt(t);
                }
            }

            // move median key up
            var medianKey = y.Keys[t - 1];
            y.Keys.RemoveAt(t - 1);
            if (y.Leaf)
            {
                var medianValue = y.Values[t - 1];
                y.Values.RemoveAt(t - 1);
                parent.Values.Insert(index, medianValue);
            }

            parent.Keys.Insert(index, medianKey);
            parent.Children.Insert(index + 1, z);
        }

        private void InsertNonFull(Node node, string key, TPointer pointer)
        {
            int i = node.Keys.Count - 1;
            if (node.Leaf)
            {
                // find position
                int pos = 0;
                while (pos < node.Keys.Count && string.CompareOrdinal(node.Keys[pos], key) < 0) pos++;
                if (pos < node.Keys.Count && string.CompareOrdinal(node.Keys[pos], key) == 0)
                {
                    // append pointer to existing list
                    var list = node.Values[pos] as List<TPointer>;
                    list!.Add(pointer);
                }
                else
                {
                    node.Keys.Insert(pos, key);
                    var newList = new List<TPointer> { pointer };
                    node.Values.Insert(pos, newList as object);
                }
            }
            else
            {
                while (i >= 0 && string.CompareOrdinal(key, node.Keys[i]) < 0) i--;
                i++;
                var child = node.Children[i];
                if (child.Keys.Count == 2 * _t - 1)
                {
                    SplitChild(node, i);
                    if (string.CompareOrdinal(key, node.Keys[i]) > 0) i++;
                }
                InsertNonFull(node.Children[i], key, pointer);
            }
        }
    }
}
