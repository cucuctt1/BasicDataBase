# Tổng Quan Chỉ Mục B+ Tree

Tài liệu này mô tả thiết kế, API và cách sử dụng của `base/Index/BPlusTree.cs` trong dự án `BasicDataBase`.

## Động lực & Mức độ phù hợp

B+ tree cung cấp chỉ mục có thứ tự trên khóa dạng chuỗi cho các bảng được lưu trữ bởi lớp File I/O. Mỗi khóa ánh xạ tới một hoặc nhiều mã định danh bản ghi (row ordinal). Cấu trúc này hỗ trợ truy vấn bằng toán tử bằng (`=`) và khoảng (`BETWEEN`) hiệu quả, đồng thời liên kết các lá để việc quét tuần tự vẫn giữ được thứ tự thân thiện với đĩa.

## Cấu trúc nút

- **Bậc (order)**: ngầm định; nút trong giữ mảng khóa đã sắp xếp và các con trỏ con với kích thước phù hợp.
- **InternalNode**
  - `List<string> Keys`: các khóa chia đoạn phạm vi của con.
  - `List<Node> Children`: danh sách con trỏ tới nút con (lúc đầy có nhiều hơn số khóa một phần tử).
- **LeafNode**
  - `List<string> Keys`: danh sách khóa đã sắp xếp.
  - `List<List<int>> Values`: danh sách song song lưu các bucket id bản ghi cho từng khóa.
  - `LeafNode? Next` / `LeafNode? Previous`: danh sách liên kết đôi giúp quét theo thứ tự.

Mọi nút đều kế thừa `Node` (abstract) để hỗ trợ đa hình và che giấu chi tiết triển khai.

## Các thao tác lõi

| API | Mô tả | Độ phức tạp |
|-----|-------|-------------|
| `Insert(string key, int recordId)` | Chèn khóa (cho phép trùng) kèm id bản ghi vào lá, tách nút khi cần. | O(log n) cho tìm kiếm + chi phí tách trung bình |
| `Search(string key)` | Trả về bucket id ứng với khóa chính xác (hoặc rỗng). | O(log n + k) với `k` là số kết quả |
| `SearchPrefix(string prefix)` | Tìm vị trí bắt đầu theo tiền tố và duyệt lá cho tới khi hết khớp. | O(log n + r) với `r` là số phần tử trong dải |
| `SearchRange(string? min, string? max, bool minInclusive = true, bool maxInclusive = true)` | Quét trong khoảng tùy chọn (bao gồm/loại trừ biên). | O(log n + r) |
| `Traverse(...)` | Iterator `IEnumerable<KeyValuePair<string, IReadOnlyList<int>>>` cho phép stream theo chuỗi lá, hỗ trợ giới hạn và cờ bao gồm biên. | O(log n + r) |
| `Delete(string key, int recordId)` *(nội bộ)* | Xóa id bản ghi khỏi bucket, cân bằng lại nếu nút thiếu phần tử. | O(log n) với khả năng gộp |

### Ví dụ nhanh: thao tác trực tiếp với `BPlusTree`

```csharp
using BasicDataBase.Index;

var tree = new BPlusTree();
tree.Insert("alice", 0);
tree.Insert("bob", 1);
tree.Insert("bob", 5); // khóa trùng gom chung bucket

var exact = tree.Search("bob"); // trả về [1, 5]
var range = tree.SearchRange("a", "c"); // chọn mọi khóa trong [a, c]

foreach (var pair in tree.Traverse())
{
    Console.WriteLine($"{pair.Key} -> {string.Join(",", pair.Value)}");
}
```

## Cơ chế dựng cây (Insert pipeline)

Luồng dựng cây xoay quanh các phương thức `Node.Insert`, `InternalNode.Insert` và `LeafNode.Insert`:

1. **Đi xuống lá**: `InternalNode.Insert` chọn con bằng `FindChildIndex` (so sánh nhị phân) rồi gọi `Insert` đệ quy.
2. **Chèn vào lá**: `LeafNode.Insert` sử dụng `BinarySearch` để tìm vị trí chính xác, chèn khóa và danh sách id tương ứng.
3. **Tách nút**: Khi số khóa của lá vượt `_maxKeys`, nó tách đôi, tạo `SplitResult` chứa khóa đi lên và nút phải mới.
4. **Đẩy khóa lên cha**: Cha nhận `SplitResult`, chèn khóa phân cách và thêm con phải vào danh sách `Children`. Nếu cha cũng tràn, quá trình tách tiếp tục lan lên cho tới gốc (có thể sinh gốc mới).

Trích đoạn từ `InternalNode.Insert` mô tả việc nhận `SplitResult` và tách nút trong:

```csharp
var split = child.Insert(key, value, maxKeys, minInternal, minLeaf);
if (split != null)
{
  Keys.Insert(childIndex, split.Key);
  Children.Insert(childIndex + 1, split.RightNode);
  split.RightNode.Parent = this;
}

if (Keys.Count > maxKeys)
{
  int mid = Keys.Count / 2;
  string upKey = Keys[mid];
  var right = new InternalNode(Order);
  // di chuyển khóa và con sang nút phải
  ...
  return new SplitResult(upKey, right);
}
```

Nhờ cơ chế này, gốc luôn giữ chiều cao tối thiểu và cây cân bằng (mọi lá cùng độ sâu).

### Cân bằng sau khi xóa

Khi `Delete` loại bỏ một id và khiến nút thiếu phần tử, `InternalNode.Delete` kích hoạt `BalanceChild`:

- **Borrow (mượn)**: lấy bớt khóa/child từ anh em khi họ còn dư (`TryBorrowLeft` / `TryBorrowRight`).
- **Merge (gộp)**: nếu không mượn được, gộp hai nút và cập nhật liên kết lá/cha.
- **RebuildKeys**: cập nhật khóa phân cách trong cha để phản ánh giới hạn mới.

Một đoạn trong `TryBorrowLeft` cho lá:

```csharp
var movedKey = leftLeaf.Keys[^1];
var movedValues = leftLeaf.Values[^1];
leftLeaf.Keys.RemoveAt(leftLeaf.Keys.Count - 1);
leafChild.Keys.Insert(0, movedKey);
leafChild.Values.Insert(0, movedValues);
Keys[separatorIndex] = leafChild.Keys[0];
```

Việc cân bằng đảm bảo mỗi nút (trừ gốc) luôn giữ ít nhất `_minKeysLeaf` hoặc `_minKeysInternal` khóa.

## Lớp hỗ trợ tìm kiếm bậc cao

`IndexManager` và `TableManager` bao bọc các thao tác thô của cây:

- `SearchExact(fieldName, key)` → `Search`
- `SearchPrefix(fieldName, prefix)` → `SearchPrefix`
- `SearchRange(fieldName, min, max)` → `SearchRange`
- `SearchGreaterThan(fieldName, key, inclusive)` → quét từ biên dưới
- `SearchLessThan(fieldName, key, inclusive)` → quét tới biên trên
- `SearchTopK(fieldName, k, descending)` → duyệt có thứ tự lấy `k` phần tử đầu/cuối

Các helper đảm bảo chỉ mục đã sẵn sàng (`TableDefinition.EnsureIndex`).

### Ví dụ: kết hợp với `TableManager`

```csharp
using BasicDataBase.Table;

var manager = new TableManager("table");
manager.BuildIndex("users", "username");

var top3 = manager.SearchTopK("users", "username", 3);
var greater = manager.SearchGreaterThan("users", "username", "charlie");
```

## Quy trình chèn & tách

1. Dò từ gốc xuống bằng tìm kiếm nhị phân trên khóa.
2. Khi tới lá:
   - Tìm vị trí bằng tìm kiếm nhị phân.
   - Chèn khóa và id (tạo bucket mới nếu cần).
3. Nếu lá tràn:
   - Chia thành hai lá, phân phối khóa/giá trị đều nhau.
   - Cập nhật liên kết lá (`Previous`/`Next`).
   - Đưa khóa phân cách lên cha thông qua `InsertIntoParent`.
4. Quá trình có thể lan lên; tách gốc tạo gốc mới.

## Duyệt phạm vi

`Traverse` kết hợp nhiều helper để cung cấp quét có thứ tự:

- `GetLeftmostLeaf()` lấy lá trái nhất cho duyệt toàn bộ.
- `FindFirstPosition(key, inclusive)` xác định vị trí bắt đầu trong lá.
- Iterator trả về `KeyValuePair<string, IReadOnlyList<int>>` để người dùng stream mà không cần gom bộ nhớ.

`IndexManager.SearchTopK` minh họa việc dừng sau khi thu được `k` id. Khi cần thứ tự giảm dần, helper gom toàn bộ và đảo chiều; với tập lớn có thể tối ưu thêm.

## Điểm tích hợp

- `IndexManager.BuildIndex(fieldName)` tạo cây cho từng cột.
- `TableDefinition.EnsureIndex(fieldName)` xây dựng lười và lưu cache.
- Bộ test (`test/search_test.cs`) minh họa các truy vấn bằng, tiền tố, khoảng, lớn hơn/nhỏ hơn và top-k.

### Build index từ dữ liệu bảng

`IndexManager.BuildIndex` đọc toàn bộ bảng, tạo `BPlusTree`, sau đó gọi `Insert` cho từng bản ghi:

```csharp
var tree = new BPlusTree();
for (int r = 0; r < all.GetLength(0); r++)
{
  var key = all[r, fieldPos]?.ToString() ?? string.Empty;
  tree.Insert(key, r);
}
indexes[fieldName] = tree;
```

Nhờ vậy việc xây dựng lại chỉ mục đơn giản và tái sử dụng chính pipeline chèn của cây.

## Ghi chú triển khai

- Khóa hiện xử lý phân biệt hoa thường; nếu cần chuẩn hóa, gọi phía ngoài.
- Khóa trùng dùng chung một bucket (`List<int>`); thao tác xóa bỏ id nhưng chưa dọn bucket rỗng ngay (có thể bổ sung sau).
- Lá dùng `Next`/`Previous` để quét nhanh; chú thích nullable đảm bảo tránh lỗi khi tới cuối chuỗi.
- Cây lưu trong bộ nhớ; mỗi lần cần có thể tái xây dựng từ dữ liệu bảng.

## Hướng mở rộng

- **Bulk load**: tạo cây cân bằng trực tiếp từ danh sách khóa đã sắp xếp để tăng tốc dựng chỉ mục.
- **Reverse traversal**: bổ sung iterator ngược tránh phải gom toàn bộ khi cần thứ tự giảm dần.
- **Cursor API**: cung cấp con trỏ có thể tiếp tục giữa chừng cho các phiên quét dài.
- **Statistics**: theo dõi số lượng nút/chiều cao cho mục đích chẩn đoán.
- **Concurrency**: thêm khóa đọc/ghi nếu nhiều luồng truy cập đồng thời.

## Tài liệu tham khảo

- Comer, Douglas. "The Ubiquitous B-tree." *ACM Computing Surveys*.
- Bayer, Rudolf; McCreight, Edward. "Organization and maintenance of large ordered indexes." *(1970).* 
- Graefe, Goetz. "Modern B-tree techniques." *(2004).* 
