using System;
using System.Collections;
using System.Collections.Generic;

namespace BasicDataBase.FileIO
{

    class FileWriter
    {
        public static string schemaLine;
        public static BitArray decodedBits = new BitArray(1, false);
        // check for header format
        public static void ReadHeader(string MetaDataDir)
        {
            //file opening
            var Dat = System.IO.File.OpenRead(MetaDataDir);
            using (var reader = new System.IO.StreamReader(Dat))
            {
                // row 1  read schema
                schemaLine = reader.ReadLine();
                // row 2 read instruction
                string instructionLine = reader.ReadLine();
                decodedBits = EDHex.HexToBit(instructionLine);
            }
        }

        public void Write2DataFile(string MetadataDir, string DataDir, string schema, object[] Data)
        {
            ReadHeader(MetadataDir);
            //veryfied
            if (schemaLine != schema)
            {
                throw new ArgumentException("Schema mismatch");
            }
            Schema schemaStruct = Schema.FromString(schema);

            // opene data file
            var Dat = System.IO.File.OpenWrite(DataDir);
            //write mode append
            Dat.Seek(0, System.IO.SeekOrigin.End);
            using (var writer = new System.IO.BinaryWriter(Dat))
            {
                foreach (var item in Data)
                {
                    var bytes = DataTypeConverter.ObjectToBytes(item);
                    // write length-prefixed field
                    writer.Write(bytes.Length);
                    if (bytes.Length > 0)
                        writer.Write(bytes);
                }
                writer.Flush();
            }
        }

        public void Write2DataFile(string MetadataDir, string DataDir, string schema, object[,] Data)
        {
            ReadHeader(MetadataDir);
            //veryfied
            if (schemaLine != schema)
            {
                throw new ArgumentException("Schema mismatch");
            }
            Schema schemaStruct = Schema.FromString(schema);

            // open data file
            var Dat = System.IO.File.OpenWrite(DataDir);
            //write mode append
            Dat.Seek(0, System.IO.SeekOrigin.End);
            using (var writer = new System.IO.BinaryWriter(Dat))
            {
                for (int i = 0; i < Data.GetLength(0); i++)
                {
                    for (int j = 0; j < Data.GetLength(1); j++)
                    {
                        var bytes = DataTypeConverter.ObjectToBytes(Data[i, j]);
                        // write length-prefixed field
                        writer.Write(bytes.Length);
                        if (bytes.Length > 0)
                            writer.Write(bytes);
                    }
                }
                writer.Flush();
            }
        }

    }
}