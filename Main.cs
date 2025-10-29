// example run of schema construction
using System;
using BasicDataBase.Test;


namespace BasicDataBase
{
    class Program
    {
        static void Main(string[] args)
        {
            schemaConstruct_Test.Test();
            schemaInstruction_Test.Test();
            Metadata_Test.Test();
            Write_Test.Test();
            Read_Test.Test();
        }
    }
}