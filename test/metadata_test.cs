using BasicDataBase.FileIO;


namespace BasicDataBase.Test
{

    public class Metadata_Test
    {
        public static void Test()
        {
            Schema schema = Schema.FromString("Id:int,Name:string:4,IsActive:bool,CreatedAt:datetime,Data:blob");
            SchemaInstruction instruction = new SchemaInstruction(schema);
            System.Collections.BitArray bits = BitGenerator.GenerateBitPadding(instruction.FieldOffsets.ToArray(), instruction.RecordSize);
            MetaWriter.WriteMetaData("metadata.meta", schema, bits);
            Console.WriteLine("Metadata written to metadata.meta");
            FileWriter.ReadHeader("metadata.meta");
            
        }
    }
}