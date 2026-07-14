using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Orleans.Lattice;

namespace Orleans.Lattice.Tests.Docs;

/// <summary>
/// Compiles every <c>```csharp verify</c>-fenced snippet under <c>docs/</c> against
/// the current <c>Orleans.Lattice</c> public surface. Any snippet that fails to
/// compile fails this test - so a rename of a public type or method breaks the
/// build instead of silently rotting the docs.
/// </summary>
/// <remarks>
/// Opt-in by design: mark a fence as <c>```csharp verify</c> (instead of just
/// <c>```csharp</c>) to include it. Snippets are treated as method bodies and
/// are given these ambient parameters for free:
/// <list type="bullet">
///   <item><description><c>IGrainFactory grainFactory</c></description></item>
///   <item><description><c>IClusterClient client</c></description></item>
///   <item><description><c>ISiloBuilder siloBuilder</c></description></item>
///   <item><description><c>ILattice tree</c></description></item>
///   <item><description><c>CancellationToken cancellationToken</c></description></item>
/// </list>
/// An ambient <c>record User(string Name, int Age)</c> is also declared so typed
/// helper examples compile without ceremony. Method-body snippets additionally
/// see an ambient <c>MyReplicationObserver</c> stub so DI-registration examples
/// for <c>IMutationObserver</c> compile in isolation.
/// </remarks>
[TestFixture]
[Category("Docs")]
public sealed class DocsSnippetCompilationTests
{
    /// <summary>Source cases: every verify-fenced snippet found under <c>docs/</c> and the repo-root <c>README.md</c>.</summary>
    public static IEnumerable<TestCaseData> VerifySnippets()
    {
        var docsRoot = FindDocsRoot();
        var repoRoot = Directory.GetParent(docsRoot)!.FullName;

        foreach (var file in Directory.EnumerateFiles(docsRoot, "*.md", SearchOption.AllDirectories))
        {
            var text = File.ReadAllText(file);
            var matches = VerifyFenceRegex.Matches(text);
            for (int i = 0; i < matches.Count; i++)
            {
                var body = matches[i].Groups["body"].Value;
                var rel = Path.GetRelativePath(docsRoot, file).Replace('\\', '/');
                var name = $"{rel}#{i}";
                yield return new TestCaseData(name, body)
                    .SetName($"Doc_snippet_compiles({rel}#{i})");
            }
        }

        // Also scan the repo-root README.md so the project's front-page Quick
        // Start snippets are held to the same compilation contract as the
        // docs/ tree.
        var readme = Path.Combine(repoRoot, "README.md");
        if (File.Exists(readme))
        {
            var text = File.ReadAllText(readme);
            var matches = VerifyFenceRegex.Matches(text);
            for (int i = 0; i < matches.Count; i++)
            {
                var body = matches[i].Groups["body"].Value;
                var name = $"README.md#{i}";
                yield return new TestCaseData(name, body)
                    .SetName($"Doc_snippet_compiles({name})");
            }
        }
    }

    /// <summary>Compiles one snippet and asserts zero error diagnostics.</summary>
    [TestCaseSource(nameof(VerifySnippets))]
    public void Doc_snippet_compiles(string name, string body)
    {
        var source = WrapAsMethodBody(name, body);
        var tree = CSharpSyntaxTree.ParseText(source);
        var compilation = CSharpCompilation.Create(
            assemblyName: "DocsSnippetCompilation_" + System.Guid.NewGuid().ToString("N"),
            syntaxTrees: [tree],
            references: GetMetadataReferences(),
            options: new CSharpCompilationOptions(
                OutputKind.DynamicallyLinkedLibrary,
                nullableContextOptions: NullableContextOptions.Enable));

        var errors = compilation
            .GetDiagnostics()
            .Where(d => d.Severity == DiagnosticSeverity.Error)
            .ToArray();

        if (errors.Length > 0)
        {
            var sb = new StringBuilder();
            sb.Append("Doc snippet '").Append(name).AppendLine("' failed to compile:");
            foreach (var e in errors)
            {
                sb.Append("  ").AppendLine(e.ToString());
            }
            sb.AppendLine("--- generated source ---");
            sb.AppendLine(source);
            Assert.Fail(sb.ToString());
        }
    }

    private static readonly Regex VerifyFenceRegex = new(
        @"^```csharp\s+verify\s*\r?\n(?<body>.*?)^```",
        RegexOptions.Multiline | RegexOptions.Singleline | RegexOptions.Compiled);

    private static string WrapAsMethodBody(string name, string body)
    {
        // Pull any leading `using ...;` directives out of the body and lift
        // them into the generated file's using list, so docs that show
        // real `using` syntax still compile as method-body snippets.
        var extraUsings = new StringBuilder();
        body = HoistUsings(body, extraUsings);

        // Detect the snippet shape so we can wrap it appropriately.
        // Class-members shape triggers on a leading attribute (e.g. [TestCase])
        // OR on a top-level type declaration (class/interface/record/struct/enum)
        // optionally preceded by `//` comment lines. This lets docs declare
        // helper types (e.g. an IMutationObserver implementation) at the top of
        // a snippet without forcing readers to wrap them in a method body.
        bool looksLikeClassMembers =
            Regex.IsMatch(body, @"^\s*\[", RegexOptions.Singleline)
            || Regex.IsMatch(
                body,
                @"^\s*(//[^\n]*\r?\n\s*)*(?:(?:public|internal|private|protected|sealed|abstract|static|partial)\s+)*(?:class|interface|record|struct|enum)\s+\w",
                RegexOptions.Singleline);
        bool looksLikeInterfaceMember = !looksLikeClassMembers
            && Regex.IsMatch(body, @"^\s*(public\s+|internal\s+|private\s+)?(async\s+)?(Task|ValueTask|IAsyncEnumerable|IEnumerable|void|int|bool|string|long|double|byte\[\])\b[^{;]*\)\s*;?\s*$",
                RegexOptions.Singleline);

        var header = $$"""
            // Auto-generated wrapper for: {{name}}
            #pragma warning disable CS0219 // unused local
            #pragma warning disable CS8321 // unused local function
            #pragma warning disable CS0168 // unused variable
            #pragma warning disable CS0067 // unused event
            using System;
            using System.Collections.Generic;
            using System.Linq;
            using System.Text;
            using System.Text.Json;
            using System.Threading;
            using System.Threading.Tasks;
            using Orleans;
            using Orleans.Hosting;
            using Orleans.Lattice;
            using Orleans.Lattice.BPlusTree;
            using Orleans.Lattice.Membership;
            using Orleans.Lattice.Auth;
            using Orleans.Lattice.Storage.AzureTable;
            using Orleans.Lattice.Replication;
            using Orleans.Lattice.Replication.Grpc;
            using Orleans.Lattice.Api.State;
            using Orleans.Lattice.Api.State.Grpc;
            using Orleans.Lattice.Api.Data;
            using Orleans.Lattice.Api.Data.Grpc;
            using Orleans.Lattice.Api.Mcp;
            using Microsoft.AspNetCore.Builder;
            using Microsoft.Extensions.DependencyInjection;
            using Microsoft.Extensions.Hosting;
            {{extraUsings}}
            public sealed record User(string Name, int Age);
            public sealed record Order(string Id, decimal Total);

            """;

        if (looksLikeClassMembers)
        {
            // Shim types for test-framework attributes that docs may
            // reference without pulling in NUnit.
            var shims = """
                [AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
                public sealed class TestCaseAttribute : Attribute
                {
                    public TestCaseAttribute(params object[] args) { }
                    public string? TestName { get; set; }
                }
                """;
            return header + shims + $$"""

                public class DocsSnippet
                {
                {{body}}
                }
                """;
        }

        if (looksLikeInterfaceMember)
        {
            return header + $$"""
                public interface IDocsSnippet
                {
                {{body}}
                }
                """;
        }

        // Default: method body. If the snippet redeclares `tree` or `lattice`,
        // drop the ambient parameter of the same name to avoid CS0136.
        bool declaresTree = Regex.IsMatch(body, @"\b(var|ILattice)\s+tree\b");
        var treeParam = declaresTree ? string.Empty : "ILattice tree, ";

        bool declaresLattice = Regex.IsMatch(body, @"\b(var|ILattice)\s+lattice\b");
        var latticeParam = declaresLattice ? string.Empty : "ILattice lattice, ";

        // Provide an ambient `MyReplicationObserver` stub so DI-registration
        // snippets that reference the canonical observer name (as documented
        // in api.md) compile as standalone method-body snippets. Class-members
        // snippets declare their own and never see this stub.
        var ambientObserver = """
            public sealed class MyReplicationObserver : IMutationObserver
            {
                public Task OnMutationAsync(LatticeMutation mutation, CancellationToken ct)
                    => Task.CompletedTask;
            }

            """;

        return header + ambientObserver + $$"""
            public static class DocsSnippet
            {
                public static async Task RunAsync(
                    IGrainFactory grainFactory,
                    IClusterClient client,
                    ISiloBuilder siloBuilder,
                    {{treeParam}}{{latticeParam}}CancellationToken cancellationToken)
                {
                    await Task.Yield();
            {{body}}
                }
            }
            """;
    }

    /// <summary>
    /// Strips leading <c>using X;</c> lines from the snippet body and appends
    /// them to <paramref name="extraUsings"/>. Stops at the first non-using,
    /// non-blank line.
    /// </summary>
    private static string HoistUsings(string body, StringBuilder extraUsings)
    {
        var lines = body.Split('\n');
        int firstNonUsing = 0;
        for (int i = 0; i < lines.Length; i++)
        {
            var trimmed = lines[i].TrimEnd('\r').Trim();
            if (trimmed.Length == 0) { firstNonUsing = i + 1; continue; }
            if (trimmed.StartsWith("using ") && trimmed.EndsWith(";"))
            {
                extraUsings.AppendLine(trimmed);
                firstNonUsing = i + 1;
                continue;
            }
            break;
        }
        return firstNonUsing == 0 ? body : string.Join('\n', lines.Skip(firstNonUsing));
    }

    private static IReadOnlyList<MetadataReference> GetMetadataReferences()
    {
        // Touch types from every assembly we expect to reference so they are
        // loaded into the AppDomain before we enumerate it.
        _ = typeof(object);                                 // System.Private.CoreLib
        _ = typeof(System.Linq.Enumerable);                 // System.Linq
        _ = typeof(System.Collections.Generic.HashSet<>);   // System.Collections
        _ = typeof(System.Threading.CancellationToken);     // System.Threading
        _ = typeof(IGrainFactory);                          // Orleans.Core.Abstractions
        _ = typeof(IClusterClient);                         // Orleans.Core.Abstractions
        _ = typeof(ISiloBuilder);                           // Orleans.Runtime / Hosting
        _ = typeof(Microsoft.Extensions.DependencyInjection.ServiceCollection);
        _ = typeof(ILattice);                               // Orleans.Lattice
        _ = typeof(Orleans.Lattice.Api.Mcp.LatticeApiMcpOptions);   // Orleans.Lattice.Api.Mcp
        _ = typeof(Microsoft.AspNetCore.Builder.WebApplication);          // Microsoft.AspNetCore (shared framework)
        _ = typeof(Microsoft.AspNetCore.Routing.IEndpointRouteBuilder);   // Microsoft.AspNetCore.Routing
        _ = typeof(Microsoft.AspNetCore.Hosting.IWebHostEnvironment);     // Microsoft.AspNetCore.Hosting.Abstractions

        var refs = new List<MetadataReference>();
        var seen = new HashSet<string>(System.StringComparer.OrdinalIgnoreCase);

        // Pull in everything the test host has loaded so far.
        foreach (var asm in System.AppDomain.CurrentDomain.GetAssemblies())
        {
            if (asm.IsDynamic) continue;
            var loc = asm.Location;
            if (string.IsNullOrEmpty(loc)) continue;
            if (!seen.Add(loc)) continue;
            refs.Add(MetadataReference.CreateFromFile(loc));
        }

        // Also add every DLL sitting in the test output directory - this
        // picks up Orleans.Core, Orleans.Runtime and similar that may not
        // yet be loaded but are required to resolve doc-snippet API calls.
        var baseDir = System.AppContext.BaseDirectory;
        foreach (var dll in Directory.EnumerateFiles(baseDir, "*.dll"))
        {
            if (seen.Add(dll))
            {
                try { refs.Add(MetadataReference.CreateFromFile(dll)); }
                catch { /* skip non-managed / unreadable */ }
            }
        }

        // Also add every DLL from the Microsoft.AspNetCore.App shared framework
        // directory - the shared framework lives outside the test output dir
        // and not every assembly is loaded into the AppDomain by default, but
        // snippets that demonstrate web host wiring (WebApplication,
        // IEndpointRouteBuilder, ...) need them to compile.
        var aspNetCoreDir = Path.GetDirectoryName(typeof(Microsoft.AspNetCore.Builder.WebApplication).Assembly.Location);
        if (!string.IsNullOrEmpty(aspNetCoreDir) && Directory.Exists(aspNetCoreDir))
        {
            foreach (var dll in Directory.EnumerateFiles(aspNetCoreDir, "*.dll"))
            {
                var name = Path.GetFileName(dll);
                if (!name.StartsWith("Microsoft.AspNetCore", System.StringComparison.OrdinalIgnoreCase) &&
                    !name.StartsWith("Microsoft.Extensions.", System.StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }
                if (seen.Add(dll))
                {
                    try { refs.Add(MetadataReference.CreateFromFile(dll)); }
                    catch { /* skip non-managed / unreadable */ }
                }
            }
        }

        return refs;
    }

    private static string FindDocsRoot()
    {
        // Walk up from the test assembly location until we find a 'docs' folder
        // sitting next to the repo's README.md.
        var dir = new DirectoryInfo(
            Path.GetDirectoryName(typeof(DocsSnippetCompilationTests).Assembly.Location)!);
        while (dir is not null)
        {
            var candidate = Path.Combine(dir.FullName, "docs");
            if (Directory.Exists(candidate) && File.Exists(Path.Combine(dir.FullName, "README.md")))
            {
                return candidate;
            }
            dir = dir.Parent;
        }

        throw new DirectoryNotFoundException(
            "Could not locate the repo 'docs' folder walking up from "
            + typeof(DocsSnippetCompilationTests).Assembly.Location);
    }
}
