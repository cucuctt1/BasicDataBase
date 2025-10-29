# BasicDataBase

A small, educational .NET-based miniature database demo focused on a simple schema representation and file I/O utilities.

This repository contains a tiny schema model and helpers to parse/serialize simple table schemas. It is intended for learning and demonstration purposes.

## What this project contains

- `BasicDataBase.csproj` - project file (targeting .NET 9.0 in this workspace).
- `Main.cs` - application entry (demo runner / console app).
- `base/FileIO/SchemaConstruct.cs` - schema model: `FieldType`, `Field`, `Schema` with parsing and formatting helpers.
- `base/FileIO/FieldBitFlags.cs` and `base/FileIO/SchemaInstruction.cs` - supporting file I/O utilities.
- `base/FileIO/utils/BitSchemaPad.cs` - small utilities used by file I/O.
- `test/` - test source files (example tests for schema parsing/formatting).

## Key types (quick reference)

Open `base/FileIO/SchemaConstruct.cs` for full details. Main types:

- `enum FieldType` - supported types: `Integer`, `String`, `Boolean`, `DateTime`, `Blob`.
- `class Field` - properties: `Name`, `Type`, `MaxLength` (for string length limits).
- `class Schema` - holds `List<Field> Fields` and helpers:
  - `static Schema FromString(string schemaString)` - parse a comma-separated schema specification.
  - `static Schema FromArray(string[] fields)` - convenience wrapper around `FromString`.
  - `override string ToString()` - serializes back to the same compact representation.

Schema syntax (used by `FromString`) is a CSV of colon-delimited field definitions:

- `Name:type` or `Name:type:maxLength` for strings.
- Example: `Id:int,Name:string:100,IsActive:bool,CreatedAt:datetime,Data:blob`

Supported types mapping (string tokens -> `FieldType`):
- `int` -> `Integer`
- `string` -> `String`
- `bool` -> `Boolean`
- `datetime` -> `DateTime`
- `blob` -> `Blob`

## Example usage (C# snippet)

This shows how to parse and inspect a schema programmatically:

```csharp
using System;
using BasicDataBase.FileIO;

class Demo
{
    static void Main()
    {
        var schema = Schema.FromString("Id:int,Name:string:100,IsActive:bool,CreatedAt:datetime,Data:blob");
        Console.WriteLine("Parsed fields:");
        foreach (var f in schema.Fields)
        {
            Console.WriteLine($"- {f.Name} ({f.Type}){(f.Type == FieldType.String && f.MaxLength > 0 ? $\" [max={f.MaxLength}]\" : string.Empty)}");
        }

        // Serialize back to compact string
        Console.WriteLine("\nSerialized: " + schema.ToString());
    }
}
```

## Build & run (Windows PowerShell)

From the project root (where `BasicDataBase.csproj` is located):

```powershell
# Build
dotnet build .\BasicDataBase.csproj

# Run using dotnet (uses Main.cs)
dotnet run --project .\BasicDataBase.csproj

# Or run the compiled exe after building
.
# After successful build, the exe lives at:
# .\bin\Debug\net9.0\BasicDataBase.exe
& .\bin\Debug\net9.0\BasicDataBase.exe
```

If you need to target a different configuration, change the `dotnet build` parameters.

## Tests

There are a couple of sample tests in the `test/` folder (for example `schemaConstruct_Test.cs` and `schemaInstruction_Test.cs`). If a test project is present/configured, run tests with:

```powershell
# if test project exists and is properly configured
dotnet test
```

If tests are not wired into a separate test project in this repo yet, you can run the simple example in `Main.cs` or add a test project and reference the library code.

## Notes & assumptions

- The code targets .NET 9.0 (see `bin/Debug/net9.0` in the repository layout). Adjust the SDK if you use a different runtime.
- The schema parser expects well-formed tokens (simple validation inside `FromString` will throw an `ArgumentException` on unknown types).
- `MaxLength` currently only applies to `string` fields and will be zero when not provided.

## Next steps / suggestions

- Add a small sample `README`-driven demo (console arguments) to make running the parser with custom input easier.
- Wire tests into a proper `dotnet test` project so CI can run them.
- Add small examples showing writing/reading records using the `base/FileIO` utilities.

## License

Add your preferred license file (e.g., `LICENSE`) if you plan to publish this project.

---

If you want, I can also:
- Add a short demo program that prints schema parsing output from a CLI argument.
- Create a proper test csproj and a couple of unit tests to run with `dotnet test`.

Tell me which of those you'd like next.
