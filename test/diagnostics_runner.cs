using System;
using BasicDataBase.Table;

namespace BasicDataBase.Test
{
    public static class Diagnostics_Runner
    {
        public static void Run(int benchmarkCount = -1)
        {
            Console.WriteLine("=== Running File I/O benchmark ===");
            Benchmark_Test.Test(benchmarkCount);

            Console.WriteLine();
            Console.WriteLine("=== Running table diagnostics ===");
            var manager = new TableManager();
            var report = TableDiagnostics.Analyze(manager);
            TableDiagnostics.Print(report);
        }
    }
}
