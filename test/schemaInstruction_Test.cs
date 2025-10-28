//test schema instruction



using System.Collections;
using BasicDataBase.FileIO;

namespace BasicDataBase.Test
{

    public class schemaInstruction_Test
    {
        public static void Test()
        {
            Schema schema = Schema.FromString("Id:int,Name:string:4,IsActive:bool,CreatedAt:datetime");
            SchemaInstruction instruction = new SchemaInstruction(schema);

            Console.WriteLine("Field Offsets:");
            foreach (var offset in instruction.FieldOffsets)
            {
                Console.WriteLine(offset);
            }

            Console.WriteLine("Record Size: " + instruction.RecordSize);
            BitArray byteRule = BitGenerator.GenerateBitPadding(instruction.FieldOffsets.ToArray(), instruction.RecordSize);
            Console.WriteLine("Byte Rule: " + BitGenerator.BitArrayToString(byteRule));
            Console.WriteLine("Decoded Offsets: " + string.Join(", ", BitGenerator.DecodeBitPadding(byteRule)));
        }
    }
}