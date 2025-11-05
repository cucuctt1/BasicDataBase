# Tổng Quan Cây Nhị Phân Tìm Kiếm

Tài liệu này giải thích thiết kế, API và cách sử dụng của `base/Index/BinarySearchTree.cs` trong dự án `BasicDataBase`.

## Động lực

Cây tìm kiếm nhị phân (BST) cung cấp chỉ mục trong bộ nhớ theo thứ tự cho các khóa dạng chuỗi. Mỗi khóa ánh xạ tới một danh sách chỉ số bản ghi. So với cấu trúc B+ tree trước đây, BST nhẹ hơn và dễ phân tích hơn, phù hợp với các tập dữ liệu vừa phải khi sự đơn giản quan trọng hơn việc luôn duy trì chiều cao cân bằng.

`IndexManager` dựa vào BST để hỗ trợ truy vấn bằng, tiền tố và khoảng.

## Cấu trúc nút

- `BinarySearchTree.Node`
  - `string Key`: khóa đã được chuẩn hóa (giá trị null được thay bằng chuỗi rỗng khi chèn).
  - `List<int> Values`: danh sách chỉ số bản ghi tương ứng với khóa.
  - `Node? Left` / `Node? Right`: liên kết tới nút con bên trái và bên phải.

Cây không tự cân bằng; hình dạng phụ thuộc thứ tự chèn dữ liệu.

## Các thao tác chính

| API | Mô tả | Ghi chú |
|-----|-------|---------|
| `Insert(string key, int recordId)` | Chèn một chỉ số bản ghi vào bucket của khóa. | Loại bỏ trùng lặp nếu chỉ số đã tồn tại trong cùng khóa. |
| `Delete(string key, int recordId)` | Xóa một chỉ số bản ghi khỏi khóa. | Xóa cả nút nếu bucket trở nên rỗng. |
| `Search(string key)` | Trả về các chỉ số tương ứng với khóa chính xác. | Tạo danh sách mới để người gọi có thể thay đổi an toàn. |
| `SearchPrefix(string prefix)` | Thu thập chỉ số có khóa bắt đầu bằng tiền tố. | Duyệt theo thứ tự trung và dừng khi khóa vượt khỏi tiền tố. |
| `SearchRange(string? minKey, string? maxKey)` | Trả về các chỉ số nằm trong khoảng `[min, max]`. | Có thể để biên giới `null` để mở khoảng ở đầu hoặc cuối. |
| `Traverse(...)` | Duyệt theo thứ tự trung với giới hạn tùy chọn. | Được sử dụng bởi các hàm tiện ích như `SearchTopK`. |
| `Clear()` | Xóa toàn bộ cây. | Đặt `root` về `null`. |

## Duyệt khoảng

`Traverse(minKey, maxKey, minInclusive, maxInclusive)` duyệt cây theo thứ tự trung và kiểm tra giới hạn:

1. Vào nhánh trái nếu có thể có khóa thỏa mãn giới hạn dưới.
2. Trả về nút hiện tại nếu khóa nằm trong phạm vi và đáp ứng các cờ bao gồm.
3. Vào nhánh phải nếu có thể có khóa thỏa mãn giới hạn trên.

Cách duyệt này hỗ trợ:

- Lấy danh sách top-k tăng dần bằng cách đọc đến khi đủ dữ liệu.
- Tạo danh sách top-k giảm dần bằng cách thu thập tất cả rồi đảo ngược (cách làm hiện tại trong `IndexManager`).

## Tích hợp với `IndexManager`

```csharp
var manager = new IndexManager(metaPath, dataPath);
manager.BuildIndex("username");

var exact = manager.SearchExact("username", "Alice");
var prefix = manager.SearchPrefix("username", "Al");
var range = manager.SearchRange("username", "Bob", "Eve");
```

- `BuildIndex` đọc toàn bộ bảng và chèn cột đích vào BST.
- Các hàm truy vấn (`SearchExact`, `SearchPrefix`, `SearchRange`, `SearchGreaterThan`, `SearchLessThan`, `SearchTopK`) đều sử dụng các thao tác cơ bản của BST.

## Ghi chú triển khai

- Mọi phép so sánh sử dụng `StringComparison.Ordinal` để đảm bảo thứ tự ổn định.
- Nên chuẩn hóa khóa (giảm chữ hoa, cắt khoảng trắng, ...) trước khi chèn nếu hệ thống yêu cầu.
- `SearchPrefix` và `SearchRange` trả về bản sao của danh sách chỉ số nhằm tránh ảnh hưởng tới cấu trúc nội bộ.

BST hiện là cấu trúc chỉ mục duy nhất của dự án; không còn phụ thuộc bất kỳ biến thể B-tree nào.
