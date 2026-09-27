using System.IO;
using System.Text.RegularExpressions;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Holds in place the arrangement that replaced the byte-identical
/// <c>ContainerCpuGrant</c> mirror and its drift guard (issue #2817): the ONNX
/// embedding companion under <c>apps/embedding-onnx</c> compiles the one
/// canonical copy of the container cgroup readers in
/// <c>src/lattice/Internal/Cgroups</c> directly, linked from its csproj and
/// delivered to its container image through a BuildKit named context.
/// </summary>
/// <remarks>
/// <para>
/// That arrangement rests on one property the core library would otherwise be
/// free to break without noticing: every file in the folder must compile
/// against the base class library alone, because the companion's image has no
/// Orleans, no Lattice, and no package references to offer it. A new
/// <c>using Orleans.Lattice;</c> in that folder builds perfectly well inside
/// <c>src/lattice</c> and breaks only the image. So this fixture compiles the
/// folder in isolation, with nothing but the running shared framework as
/// references and the SDK's implicit usings, and fails on any diagnostic error.
/// </para>
/// <para>
/// It also fails loudly when the folder is empty or lacks the CPU reader, so it
/// cannot pass vacuously after a move, and it proves its own compilation can go
/// red by compiling a planted file that reaches outside the base class library.
/// </para>
/// </remarks>
[TestFixture]
public sealed class CgroupSourcesCompileStandaloneTests
{
    private const string CanonicalFolder = "src/lattice/Internal/Cgroups";

    private const string CgroupsNamespace = "Orleans.Lattice.Internal.Cgroups";

    /// <summary>
    /// The implicit usings shared by <c>Microsoft.NET.Sdk</c> and
    /// <c>Microsoft.NET.Sdk.Web</c>, the two SDKs that compile the folder. The
    /// Web SDK adds more, but a file that relies on one of those would not build
    /// in <c>src/lattice</c>, so the common set is the correct contract.
    /// </summary>
    private const string ImplicitUsings = """
        global using System;
        global using System.Collections.Generic;
        global using System.IO;
        global using System.Linq;
        global using System.Net.Http;
        global using System.Threading;
        global using System.Threading.Tasks;
        """;

    private static readonly Regex MirroredReaderDeclaration = new(
        @"\b(class|struct|record|interface)\s+(ContainerCpuGrant|ContainerMemoryLimit|CgroupFileSystem)\b",
        RegexOptions.Compiled);

    [Test]
    public void Cgroup_sources_compile_against_the_base_class_library_alone()
    {
        var sources = CanonicalSources();

        var diagnostics = Compile(sources.Select(s => (s.Path, s.Text)));

        Assert.That(
            diagnostics,
            Is.Empty,
            $"Every file under {CanonicalFolder} is compiled into the ONNX embedding "
            + "companion's container image, which offers it nothing but the base class "
            + "library. Remove the dependency, or move the code out of the folder:\n  "
            + string.Join("\n  ", diagnostics));
    }

    [Test]
    public void Standalone_compile_rejects_a_source_that_reaches_outside_the_bcl()
    {
        var sources = CanonicalSources().Select(s => (s.Path, s.Text)).Append((
            "Planted.cs",
            $$"""
            namespace {{CgroupsNamespace}};

            internal static class PlantedLatticeDependency
            {
                public static Type Options => typeof(Orleans.Lattice.LatticeOptions);
            }
            """));

        var diagnostics = Compile(sources);

        Assert.That(
            diagnostics,
            Has.Some.Contains("Planted.cs"),
            "A file reaching into the Lattice library must fail the standalone compile, "
            + "or the passing corpus proves nothing.");
    }

    [Test]
    public void Embedding_app_compiles_the_canonical_sources_and_keeps_no_copy()
    {
        var root = HygieneRepository.FindRepoRoot();
        var app = Path.Combine(root, "apps", "embedding-onnx");
        var csproj = File.ReadAllText(Path.Combine(app, "Orleans.Lattice.Embedding.Onnx.Host.csproj"));
        var dockerfile = File.ReadAllText(Path.Combine(app, "Dockerfile"));
        var compose = File.ReadAllText(
            Path.Combine(root, "samples", "RepoContextContainer", "docker-compose.yml"));

        var copies = HygieneRepository.EnumerateFiles(Path.Combine(root, "apps"), "*.cs")
            .Where(file => MirroredReaderDeclaration.IsMatch(File.ReadAllText(file)))
            .Select(file => Path.GetRelativePath(root, file).Replace('\\', '/'))
            .ToList();

        Assert.Multiple(() =>
        {
            Assert.That(
                copies,
                Is.Empty,
                "A cgroup reader is declared under apps/ again. Compile the canonical "
                + $"copy in {CanonicalFolder} instead of mirroring it (issue #2817).");
            Assert.That(
                csproj,
                Does.Contain("../../src/lattice/Internal/Cgroups/"),
                "The companion's csproj must link the canonical cgroup folder.");
            Assert.That(
                dockerfile,
                Does.Contain("COPY --from=cgroups"),
                "The companion's image must receive the canonical folder through the "
                + "`cgroups` named context.");
            Assert.That(
                compose,
                Does.Contain("cgroups: ../../src/lattice/Internal/Cgroups"),
                "The sample's compose build must pass the `cgroups` named context.");
        });
    }

    private static IReadOnlyList<(string Path, string Text)> CanonicalSources()
    {
        var root = HygieneRepository.FindRepoRoot();
        var folder = Path.Combine(root, "src", "lattice", "Internal", "Cgroups");

        var sources = Directory.Exists(folder)
            ? Directory.EnumerateFiles(folder, "*.cs", SearchOption.TopDirectoryOnly)
                .Order(StringComparer.Ordinal)
                .Select(file => (Path: Path.GetFileName(file), Text: File.ReadAllText(file)))
                .ToList()
            : [];

        Assert.That(
            sources.Select(s => s.Path),
            Does.Contain("ContainerCpuGrant.cs"),
            $"{CanonicalFolder} must hold the container CPU reader; if it moved, move "
            + "this guard, the companion's csproj link, its Dockerfile, and the compose "
            + "named context with it.");
        return sources;
    }

    private static List<string> Compile(IEnumerable<(string Path, string Text)> sources)
    {
        var parseOptions = new CSharpParseOptions(LanguageVersion.Latest);
        var trees = sources
            .Select(s => CSharpSyntaxTree.ParseText(s.Text, parseOptions, s.Path))
            .Append(CSharpSyntaxTree.ParseText(ImplicitUsings, parseOptions, "ImplicitUsings.cs"));

        var compilation = CSharpCompilation.Create(
            $"CgroupStandalone_{Guid.NewGuid():N}",
            trees,
            SharedFrameworkReferences(),
            new CSharpCompilationOptions(
                OutputKind.DynamicallyLinkedLibrary,
                nullableContextOptions: NullableContextOptions.Enable));

        return compilation.GetDiagnostics()
            .Where(d => d.Severity == DiagnosticSeverity.Error)
            .Select(d => d.ToString())
            .ToList();
    }

    /// <summary>
    /// The running shared framework (<c>Microsoft.NETCore.App</c>) only - the
    /// trusted platform assemblies that live beside <see cref="object"/>'s own
    /// assembly. The test host's Orleans and Lattice assemblies sit elsewhere on
    /// that list and are excluded, which is the whole point.
    /// </summary>
    private static IEnumerable<MetadataReference> SharedFrameworkReferences()
    {
        var frameworkDirectory = Path.GetDirectoryName(typeof(object).Assembly.Location)!;

        return ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!)
            .Split(Path.PathSeparator)
            .Where(path => string.Equals(
                Path.GetDirectoryName(path), frameworkDirectory, StringComparison.OrdinalIgnoreCase))
            .Select(path => MetadataReference.CreateFromFile(path));
    }
}
