namespace BasicDataBase.TableHelper
{
    public struct RowPointer
    {
        public string ChunkFile; // full path
        public int RowIndexInChunk; // 0-based

        public RowPointer(string chunkFile, int rowIndexInChunk)
        {
            ChunkFile = chunkFile;
            RowIndexInChunk = rowIndexInChunk;
        }
    }
}
