using System;
using System.IO;

namespace BasicDataBase.TableHelper
{
    public static class ChunkValidator
    {
        // Simple CRC32 implementation for checksums
        private static readonly uint[] _table = MakeTable();

        private static uint[] MakeTable()
        {
            uint[] table = new uint[256];
            const uint poly = 0xEDB88320u;
            for (uint i = 0; i < 256; i++)
            {
                uint crc = i;
                for (int j = 0; j < 8; j++) crc = (crc & 1) != 0 ? (crc >> 1) ^ poly : (crc >> 1);
                table[i] = crc;
            }
            return table;
        }

        public static uint ComputeCRC32(Stream stream)
        {
            uint crc = 0xFFFFFFFFu;
            byte[] buffer = new byte[8192];
            int read;
            while ((read = stream.Read(buffer, 0, buffer.Length)) > 0)
            {
                for (int i = 0; i < read; i++) crc = (crc >> 8) ^ _table[(crc ^ buffer[i]) & 0xFF];
            }
            return crc ^ 0xFFFFFFFFu;
        }

        public static uint ComputeCRC32(string filePath)
        {
            using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
            return ComputeCRC32(fs);
        }

        // Write checksum to a .chk file next to chunk
        public static void WriteChecksum(string chunkPath)
        {
            var crc = ComputeCRC32(chunkPath);
            File.WriteAllText(chunkPath + ".chk", crc.ToString("X8"));
        }

        public static bool ValidateChunk(string chunkPath)
        {
            string chkPath = chunkPath + ".chk";
            if (!File.Exists(chkPath)) return false;
            string txt = File.ReadAllText(chkPath).Trim();
            if (!uint.TryParse(txt, System.Globalization.NumberStyles.HexNumber, null, out var expected)) return false;
            var actual = ComputeCRC32(chunkPath);
            return expected == actual;
        }
    }
}
