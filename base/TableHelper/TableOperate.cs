using ConfigHandler;
// table operations


namespace BasicDataBase.TableHelper
{
    public partial class TableOperate
    {
        // fallback chunk size (bytes). If you have a ConfigHandler providing TableChunkSize,
        // replace this with ConfigHandler.TableChunkSize.
        private readonly int chunkSize = 4096;
        private readonly string databaseName;
        private readonly string tableName;

        // simple cache for rows
        private static readonly SimpleCache<string, object[]> _rowCache = new SimpleCache<string, object[]>(4096);

        // index (B-tree) cached per TableOperate instance
        private BTree<RowPointer>? _index;
        private bool _indexBuilt = false;

        public TableOperate(string databaseName, string tableName)
        {
            this.databaseName = databaseName;
            this.tableName = tableName;
        }

        private void CreateNewChunkFile(int chunkIndex)
        {
            string tableDir = System.IO.Path.Combine(databaseName, tableName);
            string chunkFilePath = System.IO.Path.Combine(tableDir, $"chunk_{chunkIndex}.bin");
            using (var fs = new System.IO.FileStream(chunkFilePath, System.IO.FileMode.Create, System.IO.FileAccess.Write))
            {
                // create empty chunk file
            }
        }

        //before row insert check
        private void ChunkRowCheck()
        {
            // find last chunk file
            string tableDir = System.IO.Path.Combine(databaseName, tableName);
            var chunkFiles = System.IO.Directory.GetFiles(tableDir, "chunk_*.bin");
            if (chunkFiles.Length == 0)
            {
                // no chunk files, create the first one
                CreateNewChunkFile(0);
                return;
            }
            // get last chunk file
            Array.Sort(chunkFiles);
            string lastChunkFile = chunkFiles[chunkFiles.Length - 1];

            // check number of rows in last chunk file
            var fileInfo = new System.IO.FileInfo(lastChunkFile);
            if (fileInfo.Length >= chunkSize)
            {
                // create new chunk file
                int lastChunkIndex = chunkFiles.Length - 1;
                CreateNewChunkFile(lastChunkIndex + 1);
            }
        }

        ////// opertateions //////
        public void InsertRow(object[] rowData)
        {
            // check chunk file
            ChunkRowCheck();

            // find last chunk file
            string tableDir = System.IO.Path.Combine(databaseName, tableName);
            var chunkFiles = System.IO.Directory.GetFiles(tableDir, "chunk_*.bin");
            Array.Sort(chunkFiles);
            string lastChunkFile = chunkFiles[chunkFiles.Length - 1];

            // append row data to last chunk file
            using (var fs = new System.IO.FileStream(lastChunkFile, System.IO.FileMode.Append, System.IO.FileAccess.Write))
            using (var writer = new System.IO.BinaryWriter(fs))
            {
                foreach (var data in rowData)
                {
                    if (data is int)
                    {
                        writer.Write((int)data);
                    }
                    else if (data is string)
                    {
                        writer.Write((string)data);
                    }
                    // add more data types as needed
                }
            }
        }

        public Array ReadRow(int rowIndex)
        {
            // find which chunk and row index within chunk
            string tableDir = System.IO.Path.Combine(databaseName, tableName);
            var chunkFiles = System.IO.Directory.GetFiles(tableDir, "chunk_*.bin");
            Array.Sort(chunkFiles);
            int remaining = rowIndex;
            foreach (var chunk in chunkFiles)
            {
                int rows = GetRowCountInChunk(chunk);
                if (remaining < rows)
                {
                    return ReadRowByPointer(new RowPointer(chunk, remaining));
                }
                remaining -= rows;
            }
            return Array.Empty<object>();
        }

        // Build an in-memory B-tree index mapping the first column (string) to pointers
        public BTree<RowPointer> BuildIndex()
        {
            if (_indexBuilt && _index != null) return _index;
            _index = new BTree<RowPointer>(64);
            string tableDir = System.IO.Path.Combine(databaseName, tableName);
            if (!Directory.Exists(tableDir)) { _indexBuilt = true; return _index; }
            var chunkFiles = Directory.GetFiles(tableDir, "chunk_*.bin");
            Array.Sort(chunkFiles);
            foreach (var chunk in chunkFiles)
            {
                using (var fs = new FileStream(chunk, FileMode.Open, FileAccess.Read, FileShare.Read))
                using (var reader = new BinaryReader(fs))
                {
                    int rowIndex = 0;
                    while (fs.Position < fs.Length)
                    {
                        // attempt to read 4 text columns (32 bytes)
                        if (fs.Length - fs.Position >= 32)
                        {
                            var keyBytes = reader.ReadBytes(8);
                            if (keyBytes.Length < 8) break;
                            string key = System.Text.Encoding.ASCII.GetString(keyBytes);
                            // skip remaining 24 bytes of row
                            var rest = reader.ReadBytes(24);
                            _index.Insert(key, new RowPointer(chunk, rowIndex));
                            rowIndex++;
                            continue;
                        }
                        break;
                    }
                }
            }
            _indexBuilt = true;
            return _index;
        }

        public int GetRowCountInChunk(string chunkPath)
        {
            var fi = new System.IO.FileInfo(chunkPath);
            long len = fi.Length;
            if (len % 32 == 0) return (int)(len / 32);
            if (len % 28 == 0) return (int)(len / 28);
            if (len % 4 == 0) return (int)(len / 4);
            return 0;
        }

        // Read a row by RowPointer. Returns object[] with columns (strings or ints depending on format)
        public object[] ReadRowByPointer(RowPointer pointer)
        {
            string cacheKey = pointer.ChunkFile + ":" + pointer.RowIndexInChunk;
            if (_rowCache.TryGet(cacheKey, out var cached)) return cached;

            using (var fs = new FileStream(pointer.ChunkFile, FileMode.Open, FileAccess.Read, FileShare.Read))
            using (var reader = new BinaryReader(fs))
            {
                long pos = 0;
                long len = fs.Length;
                if (len % 32 == 0)
                {
                    pos = (long)pointer.RowIndexInChunk * 32;
                    fs.Seek(pos, SeekOrigin.Begin);
                    var buf = reader.ReadBytes(32);
                    string a = System.Text.Encoding.ASCII.GetString(buf, 0, 8);
                    string b = System.Text.Encoding.ASCII.GetString(buf, 8, 8);
                    string c = System.Text.Encoding.ASCII.GetString(buf, 16, 8);
                    string d = System.Text.Encoding.ASCII.GetString(buf, 24, 8);
                    var arr = new object[] { a, b, c, d };
                    _rowCache.Put(cacheKey, arr);
                    return arr;
                }
                else if (len % 28 == 0)
                {
                    pos = (long)pointer.RowIndexInChunk * 28;
                    fs.Seek(pos, SeekOrigin.Begin);
                    var keyBytes = reader.ReadBytes(8);
                    string key = System.Text.Encoding.ASCII.GetString(keyBytes);
                    var fields = new int[5];
                    for (int i = 0; i < 5; i++) fields[i] = reader.ReadInt32();
                    object[] arr = new object[6];
                    arr[0] = key;
                    for (int i = 0; i < 5; i++) arr[i + 1] = fields[i];
                    _rowCache.Put(cacheKey, arr);
                    return arr;
                }
                else if (len % 4 == 0)
                {
                    pos = (long)pointer.RowIndexInChunk * 4;
                    fs.Seek(pos, SeekOrigin.Begin);
                    int v = reader.ReadInt32();
                    var arr = new object[] { v };
                    _rowCache.Put(cacheKey, arr);
                    return arr;
                }
                else
                {
                    return Array.Empty<object>();
                }
            }
        }



    }
}