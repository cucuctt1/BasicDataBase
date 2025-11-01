using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using BasicDataBase.FileIO;
using BasicDataBase.Table;

namespace BasicDataBase.Test
{
    public static class Table_Manager_Test
    {
        public static void Test()
        {
            string root = Path.Combine(Environment.CurrentDirectory, "table_unit_tests");
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }

            var manager = new TableManager(root);
            var schema = Schema.FromString("Id:int,Name:string:16,IsActive:bool,CreatedAt:datetime,Avatar:blob");
            manager.CreateTable("users", schema);

            var blobHandler = new BlobHandler(manager);

            var catalogRecords = manager.GetAllRecords("__catalog");
            Console.WriteLine("Catalog entries after create:");
            foreach (var entry in catalogRecords)
            {
                Console.WriteLine(string.Join(" | ", entry.Select(v => v?.ToString() ?? "<null>")));
            }

            var baseDate = new DateTime(2025, 1, 1, 8, 0, 0, DateTimeKind.Utc);
            var records = new List<object[]>
            {
                new object[]{ 1, "Alice", true, baseDate, new byte[]{ 1, 2, 3 } },
                new object[]{ 2, "Bob", false, baseDate.AddDays(1), new byte[]{ 4, 5, 6, 7 } },
                new object[]{ 3, "Charlie", true, baseDate.AddDays(2), new byte[]{ 8, 9 } },
                new object[]{ 4, "Alicia", true, baseDate.AddDays(3), new byte[]{ 10 } }
            };
            foreach (var record in records)
            {
                manager.InsertRecord("users", record);
            }

            var initialBlobs = blobHandler.ListBlobs("users", "Avatar");
            Console.WriteLine($"Initial blob count: {initialBlobs.Count}");

            var replacedBlob = blobHandler.ReplaceBlob("users", 1, "Avatar", new byte[] { 42, 43, 44, 45 });
            Console.WriteLine($"Replaced Bob avatar -> {replacedBlob}");

            var blobBytes = blobHandler.ReadBlob("users", 1, "Avatar");
            Console.WriteLine($"Bobby avatar bytes length after replace: {blobBytes?.Length}");

            var aliceBlobPath = manager.GetBlobPath("users", 0, "Avatar");
            Console.WriteLine($"Alice avatar stored at: {aliceBlobPath}");
            var aliceBlob = manager.ReadBlob("users", 0, "Avatar");
            Console.WriteLine($"Alice avatar length: {aliceBlob?.Length}");

            var bobBlobOriginal = manager.GetBlobPath("users", 1, "Avatar");

            var charlieHits = manager.SearchExact("users", "Name", "Charlie");
            Console.WriteLine($"Charlie hits: {string.Join(",", charlieHits)}");

            var aliHits = manager.SearchPrefix("users", "Name", "Ali");
            Console.WriteLine($"Prefix 'Ali' hits: {string.Join(",", aliHits)}");

            var rangeHits = manager.SearchRange("users", "Name", "Bob", "Charlie");
            Console.WriteLine($"Range Bob-Charlie hits: {string.Join(",", rangeHits)}");

            // Update Bob -> Bobby with new avatar
            manager.UpdateRecord("users", 1, new object[] { 2, "Bobby", true, baseDate.AddDays(1), new byte[]{ 11, 12, 13 } });
            var bobbyHits = manager.SearchExact("users", "Name", "Bobby");
            Console.WriteLine($"Bobby hits after update: {string.Join(",", bobbyHits)}");
            var bobbyBlobPath = manager.GetBlobPath("users", 1, "Avatar");
            Console.WriteLine($"Bobby avatar path: {bobbyBlobPath}");
            Console.WriteLine($"Old Bob avatar exists after update: {File.Exists(bobBlobOriginal ?? string.Empty)}");

            // Delete Alice (index 0)
            manager.DeleteRecord("users", 0);
            var postDeleteHits = manager.SearchExact("users", "Name", "Bobby");
            Console.WriteLine($"Bobby hits after deleting Alice: {string.Join(",", postDeleteHits)}");

            var firstRecord = manager.GetRecord("users", 0);
            Console.WriteLine($"First record now: {firstRecord?[0]}, {firstRecord?[1]}, avatar path: {firstRecord?[4]}");

            // Bulk delete remaining Alicia by highest index
            manager.DeleteRecords("users", new[] { 2 });
            var aliAfterBulk = manager.SearchPrefix("users", "Name", "Ali");
            Console.WriteLine($"Prefix 'Ali' after bulk delete: {string.Join(",", aliAfterBulk)}");

            var finalRange = manager.SearchRange("users", "Name", "A", "Z");
            Console.WriteLine($"Final range A-Z hits: {string.Join(",", finalRange)}");

            blobHandler.DeleteBlob("users", 1, "Avatar");
            var orphans = blobHandler.ListOrphanBlobs("users");
            Console.WriteLine($"Orphan blob count after delete: {orphans.Count}");

            var catalogAfterOps = manager.GetAllRecords("__catalog");
            Console.WriteLine("Catalog entries before drop:");
            foreach (var entry in catalogAfterOps)
            {
                Console.WriteLine(string.Join(" | ", entry.Select(v => v?.ToString() ?? "<null>")));
            }

            manager.DropTable("users");

            var catalogAfterDrop = manager.GetAllRecords("__catalog");
            Console.WriteLine("Catalog entries after drop:");
            foreach (var entry in catalogAfterDrop)
            {
                Console.WriteLine(string.Join(" | ", entry.Select(v => v?.ToString() ?? "<null>")));
            }
        }
    }
}
