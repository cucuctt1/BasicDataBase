//test file


using BasicDataBase.FileIO;
using System;

namespace BasicDataBase.Test
{

    public class schemaConstruct_Test
    {
        public static void Test()
        {
            // Example usage of Schema construction
            Schema schema = Schema.FromString("Id:int,Name:string:100,IsActive:bool,CreatedAt:datetime");
            foreach (var field in schema.Fields)
            {
                Console.WriteLine($"Field Name: {field.Name}, Type: {field.Type}, MaxLength: {field.MaxLength}");
            }

            // Convert back to string
            string schemaStr = schema.ToString();
            Console.WriteLine("Schema String: " + schemaStr);
        }
    }
}