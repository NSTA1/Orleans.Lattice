using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using NUnit.Framework;

namespace Orleans.Lattice.Testing.Docs;

/// <summary>
/// Compiles every <c>```csharp verify</c>-fenced snippet in this fixture's
/// <see cref="Scope"/> against the product surface referenced by the consuming
/// test project. Any snippet that fails to compile fails this test - so a rename
/// of a public type or method breaks the build instead of silently rotting the
/// docs.
/// <para>
/// The compilation logic lives here in the shared base so every package's test
/// project can verify its own <c>docs/&lt;package&gt;</c> snippets by adding a
/// thin subclass that only binds a <see cref="DocsSnippetScope"/>. Because CI
/// runs a package's test project whenever that package changes, a snippet broken
/// by a rename in <c>src/&lt;package&gt;</c> is caught by the same package's
/// project - closing the gap where a docs snippet owned by one package was only
/// verified when the (unrelated) core project happened to be in the CI test set.
/// </para>
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
/// <para>
/// The generated wrapper's <c>using</c> header is resilient: an optional product
/// <c>using</c> (for example <c>using Orleans.Lattice.Replication;</c>) is only
/// emitted when that namespace actually resolves in the consuming project's
/// reference closure, so a package project that does not reference every sibling
/// package still compiles its own snippets.
/// </para>
/// </remarks>
public abstract class DocsSnippetCompilationTestsBase
{
    /// <summary>The docs slice this fixture is responsible for compiling.</summary>
    protected abstract DocsSnippetScope Scope { get; }

    /// <summary>
    /// Compiles every verify-fenced snippet within this fixture's scope and fails
    /// listing every snippet that did not compile, with its diagnostics and the
    /// generated source, so the fix is mechanical.
    /// </summary>
    [Test]
    public void All_doc_snippets_compile()
    {
        var docsRoot = FindDocsRoot();
        var repoRoot = Directory.GetParent(docsRoot)!.FullName;

        var failures = new List<string>();
        int compiled = 0;

        foreach (var file in EnumerateOwnedMarkdown(docsRoot, repoRoot))
        {
            var text = File.ReadAllText(file);
            var matches = VerifyFenceRegex.Matches(text);
            if (matches.Count == 0)
            {
                continue;
            }

            var name = SnippetFilePrefix(file, docsRoot, repoRoot);
            for (int i = 0; i < matches.Count; i++)
            {
                var body = matches[i].Groups["body"].Value;
                var caseName = $"{name}#{i}";
                compiled++;
                var failure = CompileSnippet(caseName, body);
                if (failure is not null)
                {
                    failures.Add(failure);
                }
            }
        }

        Assert.That(
            failures,
            Is.Empty,
            $"{failures.Count} of {compiled} doc snippet(s) in this fixture's scope failed to compile:"
            + Environment.NewLine
            + string.Join(Environment.NewLine + Environment.NewLine, failures));
    }

    /// <summary>
    /// Compiles one snippet, returning <c>null</c> on success or a formatted
    /// failure report (diagnostics plus generated source) on error.
    /// </summary>
    private static string? CompileSnippet(string name, string body)
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

        if (errors.Length == 0)
        {
            return null;
        }

        var sb = new StringBuilder();
        sb.Append("Doc snippet '").Append(name).AppendLine("' failed to compile:");
        foreach (var e in errors)
        {
            sb.Append("  ").AppendLine(e.ToString());
        }
        sb.AppendLine("--- generated source ---");
        sb.AppendLine(source);
        return sb.ToString();
    }

    /// <summary>
    /// Enumerates the markdown files owned by <see cref="Scope"/>: the repo-root
    /// <c>README.md</c> plus every unclaimed <c>docs/</c> file for the core scope,
    /// or the fixture's own <c>docs/&lt;package&gt;</c> subtree otherwise.
    /// </summary>
    private IEnumerable<string> EnumerateOwnedMarkdown(string docsRoot, string repoRoot)
    {
        if (Scope.IsCore)
        {
            var claimed = Scope.ClaimedPackageDocsRoots
                .Select(r => NormalizeRelative(r))
                .Where(r => r.StartsWith("docs/", System.StringComparison.Ordinal))
                .Select(r => r["docs/".Length..])
                .ToArray();

            foreach (var file in Directory.EnumerateFiles(docsRoot, "*.md", SearchOption.AllDirectories))
            {
                var rel = Path.GetRelativePath(docsRoot, file).Replace('\\', '/');
                bool isClaimed = claimed.Any(c =>
                    rel.Equals(c, System.StringComparison.Ordinal) ||
                    rel.StartsWith(c + "/", System.StringComparison.Ordinal));
                if (!isClaimed)
                {
                    yield return file;
                }
            }

            var readme = Path.Combine(repoRoot, "README.md");
            if (File.Exists(readme))
            {
                yield return readme;
            }

            yield break;
        }

        foreach (var root in Scope.PackageDocsRoots)
        {
            var dir = Path.Combine(repoRoot, NormalizeRelative(root).Replace('/', Path.DirectorySeparatorChar));
            if (!Directory.Exists(dir))
            {
                continue;
            }

            foreach (var file in Directory.EnumerateFiles(dir, "*.md", SearchOption.AllDirectories))
            {
                yield return file;
            }
        }
    }

    /// <summary>
    /// Produces the stable snippet-name prefix for a file: its path relative to
    /// <c>docs/</c> (e.g. <c>lattice.membership/identity-directory-providers.md</c>),
    /// or <c>README.md</c> for the repo-root readme.
    /// </summary>
    private static string SnippetFilePrefix(string file, string docsRoot, string repoRoot)
    {
        var readme = Path.Combine(repoRoot, "README.md");
        if (string.Equals(Path.GetFullPath(file), Path.GetFullPath(readme), System.StringComparison.OrdinalIgnoreCase))
        {
            return "README.md";
        }

        return Path.GetRelativePath(docsRoot, file).Replace('\\', '/');
    }

    private static string NormalizeRelative(string path) => path.Replace('\\', '/').Trim('/');

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
            {{BuildUsingHeader()}}
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
    /// Builds the snippet wrapper's <c>using</c> header. Universal BCL namespaces
    /// are always emitted; product and framework namespaces are emitted only when
    /// they resolve in the consuming project's reference closure, so a package
    /// project that does not reference every sibling package still compiles.
    /// </summary>
    private static string BuildUsingHeader()
    {
        var sb = new StringBuilder();
        foreach (var ns in AlwaysUsings)
        {
            sb.Append("using ").Append(ns).AppendLine(";");
        }

        var available = AvailableNamespaces();
        foreach (var ns in OptionalUsings)
        {
            if (available.Contains(ns))
            {
                sb.Append("using ").Append(ns).AppendLine(";");
            }
        }

        return sb.ToString();
    }

    private static readonly string[] AlwaysUsings =
    {
        "System",
        "System.Collections.Generic",
        "System.Linq",
        "System.Text",
        "System.Text.Json",
        "System.Threading",
        "System.Threading.Tasks",
    };

    private static readonly string[] OptionalUsings =
    {
        "Orleans",
        "Orleans.Hosting",
        "Orleans.Lattice",
        "Orleans.Lattice.BPlusTree",
        "Orleans.Lattice.Membership",
        "Orleans.Lattice.Auth",
        "Orleans.Lattice.Storage.AzureTable",
        "Orleans.Lattice.Replication",
        "Orleans.Lattice.Replication.Grpc",
        "Orleans.Lattice.Api.State",
        "Orleans.Lattice.Api.State.Grpc",
        "Orleans.Lattice.Api.Data",
        "Orleans.Lattice.Api.Data.Grpc",
        "Orleans.Lattice.Api.Mcp",
        "Microsoft.AspNetCore.Builder",
        "Microsoft.Extensions.DependencyInjection",
        "Microsoft.Extensions.Hosting",
    };

    private static HashSet<string>? _availableNamespaces;

    /// <summary>
    /// The set of fully-qualified namespace names present across the current
    /// reference closure, computed once per process by walking the merged global
    /// namespace of a probe compilation built from the same references used to
    /// compile snippets.
    /// </summary>
    private static HashSet<string> AvailableNamespaces()
    {
        if (_availableNamespaces is not null)
        {
            return _availableNamespaces;
        }

        var probe = CSharpCompilation.Create("DocsSnippetNamespaceProbe", references: GetMetadataReferences());
        var set = new HashSet<string>(System.StringComparer.Ordinal);
        CollectNamespaces(probe.GlobalNamespace, set);
        return _availableNamespaces = set;
    }

    private static void CollectNamespaces(INamespaceSymbol ns, HashSet<string> into)
    {
        if (!ns.IsGlobalNamespace)
        {
            into.Add(ns.ToDisplayString());
        }

        foreach (var child in ns.GetNamespaceMembers())
        {
            CollectNamespaces(child, into);
        }
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

    private static IReadOnlyList<MetadataReference> _metadataReferences = null!;

    private static IReadOnlyList<MetadataReference> GetMetadataReferences()
    {
        if (_metadataReferences is not null)
        {
            return _metadataReferences;
        }

        // Touch a couple of universal BCL types so their assemblies are loaded
        // before we enumerate the AppDomain. Product assemblies are picked up
        // from the test output directory below, so the base does not need a
        // compile-time reference to any product package.
        _ = typeof(object);                                 // System.Private.CoreLib
        _ = typeof(System.Linq.Enumerable);                 // System.Linq
        _ = typeof(System.Collections.Generic.HashSet<>);   // System.Collections
        _ = typeof(System.Threading.CancellationToken);     // System.Threading

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
        // picks up product assemblies (Orleans.Lattice.*, Orleans.Core,
        // Orleans.Runtime, ...) that the consuming test project references but
        // that may not yet be loaded into the AppDomain.
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
                var fileName = Path.GetFileName(dll);
                if (!fileName.StartsWith("Microsoft.AspNetCore", System.StringComparison.OrdinalIgnoreCase) &&
                    !fileName.StartsWith("Microsoft.Extensions.", System.StringComparison.OrdinalIgnoreCase))
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

        return _metadataReferences = refs;
    }

    private string FindDocsRoot()
    {
        // Walk up from the test assembly location until we find a 'docs' folder
        // sitting next to the repo's README.md.
        var dir = new DirectoryInfo(
            Path.GetDirectoryName(GetType().Assembly.Location)!);
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
            + GetType().Assembly.Location);
    }
}
