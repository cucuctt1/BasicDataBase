# Binary Search Tree Overview

This document describes the design, API surface, and usage of `base/Index/BinarySearchTree.cs` in the `BasicDataBase` project.

## Motivation

The binary search tree (BST) offers an ordered in-memory index for string keys. Each key maps to a list of record identifiers. Compared to the previous B+ tree, the BST is lighter weight and easier to reason about, making it a good fit for moderate data sets when simplicity matters more than perfectly balanced height.

`IndexManager` relies on this BST for equality, prefix, and range lookups.

## Node Structure

- `BinarySearchTree.Node`
  - `string Key`: normalized key (null keys are converted to the empty string at insert time).
- `List<int> Values`: record ordinal bucket for the key.
  - `Node? Left` / `Node? Right`: child pointers.

The tree does not perform automatic balancing; shape depends on insert order.

## Core Operations

| API | Description | Notes |
|-----|-------------|-------|
| `Insert(string key, int recordId)` | Adds a record id to the key bucket. | Duplicate ids for the same key are ignored. |
| `Delete(string key, int recordId)` | Removes a record id from the key bucket. | Deletes the node if the bucket becomes empty. |
| `Search(string key)` | Returns the ids stored for an exact key. | Produces a new list so callers can modify it safely. |
| `SearchPrefix(string prefix)` | Collects ids whose keys start with the prefix. | Traverses in-order and stops once keys exceed the prefix window. |
| `SearchRange(string? minKey, string? maxKey)` | Returns ids whose keys are inside `[min, max]`. | Accepts null bounds to express open ends. |
| `Traverse(...)` | In-order iterator over key/value buckets with optional bounds. | Used by helpers like `SearchTopK`. |
| `Clear()` | Removes every node. | Simply resets the root to `null`. |

## Range Traversal

`Traverse(minKey, maxKey, minInclusive, maxInclusive)` walks the tree in-order while checking bounds:

1. Visit the left child if keys there might satisfy the lower bound.
2. Yield the current node when it is within range (using the inclusive flags).
3. Visit the right child if keys there might satisfy the upper bound.

The iterator powers two common patterns:

- Build top-k ascending lists by reading until `k` ids have been collected.
- Build top-k descending lists by gathering everything then reversing (current approach in `IndexManager`).

## Integration with `IndexManager`

```csharp
var manager = new IndexManager(metaPath, dataPath);
manager.BuildIndex("username");

var exact = manager.SearchExact("username", "Alice");
var prefix = manager.SearchPrefix("username", "Al");
var range = manager.SearchRange("username", "Bob", "Eve");
```

- `BuildIndex` reads the whole table and inserts the target column into the BST.
- Query helpers (`SearchExact`, `SearchPrefix`, `SearchRange`, `SearchGreaterThan`, `SearchLessThan`, `SearchTopK`) all delegate to BST primitives.

## Future Enhancements

- **Balancing**: adopt AVL/Red-Black logic or periodic rebuilds to cap tree height under skewed inserts.
- **Memory**: experiment with pooled arrays when a single key has many ids.
- **Descending traversal**: add a dedicated reverse iterator to avoid allocating intermediate lists.
- **Persistence**: serialize the tree when snapshotting indexes to disk.

## Implementation Notes

- All comparisons use `StringComparison.Ordinal` for a deterministic ordering.
- Callers should normalize their keys (case folding, trimming) before inserting if domain rules require it.
- Prefix and range searches return copies of the id buckets so consumers cannot corrupt internal state.

The BST is now the sole indexing structure in the project; no B-tree variants remain.
