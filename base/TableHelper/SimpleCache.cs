using System.Collections.Generic;

namespace BasicDataBase.TableHelper
{
    // Very small LRU cache for caching rows (thread-unsafe, simple)
    public class SimpleCache<TKey, TValue>
    {
        private readonly int _capacity;
        private readonly Dictionary<TKey, LinkedListNode<(TKey key, TValue value)>> _map;
        private readonly LinkedList<(TKey key, TValue value)> _lru;

        public SimpleCache(int capacity = 1024)
        {
            _capacity = capacity;
            _map = new Dictionary<TKey, LinkedListNode<(TKey, TValue)>>();
            _lru = new LinkedList<(TKey, TValue)>();
        }

        public void Put(TKey key, TValue value)
        {
            if (_map.TryGetValue(key, out var node))
            {
                _lru.Remove(node);
                _lru.AddFirst(node);
                node.Value = (key, value);
                return;
            }

            var newNode = new LinkedListNode<(TKey, TValue)>((key, value));
            _lru.AddFirst(newNode);
            _map[key] = newNode;
            if (_map.Count > _capacity)
            {
                var last = _lru.Last!;
                _map.Remove(last.Value.key);
                _lru.RemoveLast();
            }
        }

        public bool TryGet(TKey key, out TValue value)
        {
            if (_map.TryGetValue(key, out var node))
            {
                _lru.Remove(node);
                _lru.AddFirst(node);
                value = node.Value.value;
                return true;
            }
            value = default!;
            return false;
        }
    }
}
