// example run of schema construction
using System;
using BasicDataBase.Test;


namespace BasicDataBase
{
    class Program
    {
        static void Main(string[] args)
        {
            // Run benchmark. If an argument is provided, use it as record count (e.g. dotnet run -- 100000)
            int count = -1;
            if (args != null && args.Length > 0)
            {
                int.TryParse(args[0], out count);
            }
            // allow `search` action: dotnet run -- search
            if (args != null && args.Length > 0 && args[0].Equals("search", StringComparison.OrdinalIgnoreCase))
            {
                BasicDataBase.Test.Search_Test.Test();
            }
            else if (args != null && args.Length > 0 && args[0].Equals("table", StringComparison.OrdinalIgnoreCase))
            {
                BasicDataBase.Test.Table_Manager_Test.Test();
            }
            else if (args != null && args.Length > 0 && args[0].Equals("diagnose", StringComparison.OrdinalIgnoreCase))
            {
                int benchCount = -1;
                if (args.Length > 1)
                {
                    int.TryParse(args[1], out benchCount);
                }
                BasicDataBase.Test.Diagnostics_Runner.Run(benchCount);
            }
            else
            {
                BasicDataBase.Test.Benchmark_Test.Test(count);
            }
        }
    }
}