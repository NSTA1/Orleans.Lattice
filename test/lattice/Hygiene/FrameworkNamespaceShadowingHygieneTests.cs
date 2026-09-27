using System.Collections.Concurrent;
using System.Reflection;
using System.Reflection.Metadata;
using System.Reflection.PortableExecutable;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Orleans.Lattice.Testing.Hygiene;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Framework-namespace shadowing hygiene gate (issue #2822). Fails when a
/// namespace declared under <c>src/</c> or <c>test/</c> introduces a segment,
/// below the <c>Orleans.Lattice</c> root, whose name is also a namespace the
/// Orleans framework ships directly under <c>Orleans</c> - for example
/// <c>Orleans.Lattice.Runtime</c>, which shadows <c>Orleans.Runtime</c>.
/// <para>
/// <b>The mechanism.</b> C# binds the leftmost identifier of a relative
/// qualified name by walking the enclosing namespaces outward and stopping at
/// the FIRST one that has a member of that name. Code in
/// <c>Orleans.Lattice.GrainIndex.Tests</c> that writes <c>Runtime.GrainId</c>
/// is looked up in <c>Orleans.Lattice.GrainIndex.Tests</c>,
/// <c>Orleans.Lattice.GrainIndex</c>, <c>Orleans.Lattice</c>, then
/// <c>Orleans</c>, where it finds the framework's <c>Orleans.Runtime</c>.
/// Declaring <c>Orleans.Lattice.Runtime</c> anywhere makes that walk stop one
/// level early, at a namespace with no <c>GrainId</c> in it, and the reference
/// fails to compile. Introducing a namespace is therefore not an additive act.
/// </para>
/// <para>
/// <b>Why a gate.</b> The break it causes is unusually hostile to diagnosis.
/// It happened for real on #2816 / #2821: a new <c>Orleans.Lattice.Runtime</c>
/// namespace in <c>src/lattice/</c> broke eight references in
/// <c>test/lattice.grainindex/</c>, a package the change never touched. It
/// fails at the solution Build step, so no test runs and every leg is red; the
/// offending diff offers no lead; and a per-package build of the changed
/// projects passes, so the ordinary pre-PR scope is structurally blind to it.
/// </para>
/// <para>
/// <b>The rule, exactly.</b> For every declared namespace
/// <c>Orleans.Lattice.S1.S2...Sn</c>, every segment <c>Si</c> (at any depth,
/// not only the first) is compared ordinally against the set of names
/// <c>X</c> for which the framework ships a public type in
/// <c>Orleans.X</c> or in a namespace beneath it. A match makes
/// <c>Orleans.Lattice.S1...Si</c> a shadowing namespace, because it captures
/// the relative reference <c>Si.Anything</c> for all code declared under
/// <c>Orleans.Lattice.S1...S(i-1)</c>. Depth matters: <c>Orleans.Lattice.Api.Runtime</c>
/// captures <c>Runtime.GrainId</c> for everything under <c>Orleans.Lattice.Api</c>.
/// </para>
/// <para>
/// <b>The framework list is reflected, not written down.</b> It is read from
/// the metadata of every <c>Orleans.*</c> assembly (other than our own) deployed
/// beside this test assembly, so an Orleans upgrade that adds a namespace
/// extends the gate without an edit here.
/// </para>
/// <para>
/// <b>Pre-existing shadows are recorded, not renamed.</b> Several shipped
/// namespaces already shadow a framework namespace and compile today, because
/// no code relies on the relative form they capture. Renaming a public
/// namespace is a breaking change outside this gate's remit, so each is listed
/// in <see cref="RecordedShadowingNamespaces"/> with its reason. The record is
/// checked both ways: a new shadow fails the gate, and a recorded one that is no
/// longer declared fails it too, so the list can only shrink deliberately.
/// </para>
/// </summary>
[TestFixture]
public sealed class FrameworkNamespaceShadowingHygieneTests
{
    private const string FirstPartyRoot = "Orleans.Lattice";

    private const string FrameworkRoot = "Orleans";

    /// <summary>
    /// Shadowing namespaces that predate this gate. Keyed by the shadowing
    /// namespace itself - the declared namespace truncated at the colliding
    /// segment - so a new sub-namespace beneath a recorded one does not need a
    /// record of its own: it introduces no new shadow.
    /// </summary>
    private static readonly IReadOnlyDictionary<string, string> RecordedShadowingNamespaces =
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["Orleans.Lattice.Explorer.Core"] =
                "shadows Orleans.Core; the Explorer's shipped core namespace, pre-existing",
            ["Orleans.Lattice.Explorer.Core.Configuration"] =
                "shadows Orleans.Configuration; shipped Explorer namespace, pre-existing",
            ["Orleans.Lattice.Explorer.Tests.Configuration"] =
                "shadows Orleans.Configuration; test-only namespace, pre-existing",
            ["Orleans.Lattice.Explorer.Tests.Hosting"] =
                "shadows Orleans.Hosting; test-only namespace, pre-existing",
            ["Orleans.Lattice.Internal"] =
                "shadows Orleans.Internal; core library internals namespace, pre-existing",
            ["Orleans.Lattice.Storage"] =
                "shadows Orleans.Storage; root of the shipped storage-backend packages, pre-existing",
            ["Orleans.Lattice.Tests.Internal"] =
                "shadows Orleans.Internal; test-only namespace, pre-existing",
            ["Orleans.Lattice.Tests.Storage"] =
                "shadows Orleans.Storage; test-only namespace, pre-existing",
            ["Orleans.Lattice.Vector.Persistence"] =
                "shadows Orleans.Persistence; shipped vector-store namespace, pre-existing",
            ["Orleans.Lattice.Vector.Tests.Persistence"] =
                "shadows Orleans.Persistence; test-only namespace, pre-existing",
        };

    private static readonly Lazy<FrameworkNamespaces> Framework = new(ReadFrameworkNamespaces);

    private static readonly Lazy<DeclarationScan> Declarations = new(ScanDeclarations);

    /// <summary>
    /// No namespace under <c>src/</c> or <c>test/</c> introduces a shadow of an
    /// Orleans framework namespace beyond those recorded.
    /// </summary>
    [Test]
    public void No_first_party_namespace_shadows_an_Orleans_framework_namespace()
    {
        var framework = Framework.Value;
        var scan = Declarations.Value;

        AssertScanIsNotVacuous(framework, scan);

        var unrecorded = FindShadowSites(scan, framework)
            .Where(static pair => !RecordedShadowingNamespaces.ContainsKey(pair.Key))
            .OrderBy(static pair => pair.Key, StringComparer.Ordinal)
            .Select(pair => Describe(pair.Key, pair.Value, framework))
            .ToList();

        Assert.That(
            unrecorded,
            Is.Empty,
            "A first-party namespace shadows an Orleans framework namespace. C# binds the leftmost "
                + "identifier of a relative name such as 'Runtime.GrainId' by walking the enclosing "
                + "namespaces outward and stopping at the FIRST one that has a member of that name. "
                + "Code under 'Orleans.Lattice.*' reaches the framework's 'Orleans.Runtime' by falling "
                + "through 'Orleans.Lattice'; declaring 'Orleans.Lattice.Runtime' stops the walk there, "
                + "at a namespace with no 'GrainId', and every such reference in EVERY package fails to "
                + "compile - including packages your change does not touch, which is why a per-package "
                + "build passes and only the solution build (or this gate) sees it (issues #2816, #2822). "
                + "Rename the offending segment (for example 'Orleans.Lattice.Hosting' -> "
                + "'Orleans.Lattice.HostEnvironment'). Do not record it as an exemption: the records "
                + "exist only for shipped namespaces that predate this gate."
                + Environment.NewLine
                + string.Join(Environment.NewLine, unrecorded));
    }

    /// <summary>
    /// Every recorded shadow is still declared and still shadows a framework
    /// namespace, so an exemption cannot outlive the namespace it excuses.
    /// </summary>
    [Test]
    public void Recorded_shadowing_namespaces_are_all_still_declared()
    {
        var framework = Framework.Value;
        var scan = Declarations.Value;

        AssertScanIsNotVacuous(framework, scan);

        var live = FindShadowSites(scan, framework);
        var stale = RecordedShadowingNamespaces.Keys
            .Where(name => !live.ContainsKey(name))
            .OrderBy(static name => name, StringComparer.Ordinal)
            .ToList();

        Assert.That(
            stale,
            Is.Empty,
            "A recorded shadowing namespace is no longer declared under src/ or test/, or no longer "
                + "shadows a framework namespace. Remove its entry from RecordedShadowingNamespaces; a "
                + "record that describes nothing is an exemption waiting to excuse a new violation."
                + Environment.NewLine
                + string.Join(Environment.NewLine, stale));
    }

    /// <summary>
    /// The positive control on the detector: the motivating namespace, a
    /// descendant of it, and the same segment at depth are all flagged.
    /// </summary>
    [Test]
    public void Control_the_detector_flags_the_motivating_namespace_at_every_depth()
    {
        var segments = new HashSet<string>(StringComparer.Ordinal) { "Runtime", "Internal" };

        Assert.Multiple(() =>
        {
            Assert.That(FindShadows("Orleans.Lattice.Runtime", segments),
                Is.EqualTo(new[] { "Orleans.Lattice.Runtime" }),
                "The namespace that broke #2821 must be flagged; if it is not, every green is vacuous.");
            Assert.That(FindShadows("Orleans.Lattice.Runtime.Internal", segments),
                Is.EqualTo(new[] { "Orleans.Lattice.Runtime", "Orleans.Lattice.Runtime.Internal" }),
                "A descendant shadows exactly as its parent does, and a second colliding segment is a "
                    + "second shadow.");
            Assert.That(FindShadows("Orleans.Lattice.Api.Runtime", segments),
                Is.EqualTo(new[] { "Orleans.Lattice.Api.Runtime" }),
                "A colliding segment below the first one captures 'Runtime.X' for every file under "
                    + "Orleans.Lattice.Api, so depth must not exempt it.");
        });
    }

    /// <summary>
    /// The negative control: namespaces that capture nothing are not flagged,
    /// including the framework's own namespace and near-miss spellings.
    /// </summary>
    [Test]
    public void Control_the_detector_ignores_namespaces_that_capture_nothing()
    {
        var segments = new HashSet<string>(StringComparer.Ordinal) { "Runtime" };

        Assert.Multiple(() =>
        {
            Assert.That(FindShadows("Orleans.Lattice.Primitives", segments), Is.Empty);
            Assert.That(FindShadows("Orleans.Lattice", segments), Is.Empty,
                "The first-party root itself is not below the root.");
            Assert.That(FindShadows("Orleans.Runtime", segments), Is.Empty,
                "Extending a framework namespace merges with it; it does not shadow it.");
            Assert.That(FindShadows("Orleans.LatticeRuntime.Runtime", segments), Is.Empty,
                "Only the Orleans.Lattice root is first-party; a prefix match on the string is wrong.");
            Assert.That(FindShadows("Orleans.Lattice.runtime", segments), Is.Empty,
                "C# names are case-sensitive, so a differently-cased segment captures nothing.");
            Assert.That(FindShadows("Orleans.Lattice.RuntimeGrants", segments), Is.Empty,
                "A segment is compared whole, not by prefix.");
        });
    }

    /// <summary>
    /// The declaration reader resolves file-scoped, block, and nested block
    /// namespaces to their full names, and does not read a namespace out of a
    /// string literal or a comment.
    /// </summary>
    [Test]
    public void Control_the_reader_resolves_every_declaration_form_and_skips_literals()
    {
        const string fileScoped = "namespace Orleans.Lattice.Runtime;\ninternal sealed class A { }\n";
        const string nested =
            "namespace Orleans.Lattice\n{\n    namespace Api.Runtime\n    {\n        class B { }\n    }\n}\n";
        const string literals =
            "namespace Orleans.Lattice.Primitives;\n"
            + "// namespace Orleans.Lattice.Runtime;\n"
            + "internal static class C\n{\n"
            + "    private const string Probe = \"\"\"\n"
            + "namespace Orleans.Lattice.Runtime;\n"
            + "\"\"\";\n}\n";

        Assert.Multiple(() =>
        {
            Assert.That(ReadDeclarations(fileScoped).Select(static d => d.Name),
                Is.EqualTo(new[] { "Orleans.Lattice.Runtime" }));
            Assert.That(ReadDeclarations(nested).Select(static d => d.Name),
                Is.EqualTo(new[] { "Orleans.Lattice", "Orleans.Lattice.Api.Runtime" }),
                "A nested block namespace must be composed with its parent, or the colliding segment "
                    + "is read without the root that makes it first-party.");
            Assert.That(ReadDeclarations(nested)[1].Line, Is.EqualTo(3),
                "The line is part of the failure message and must point at the declaration.");
            Assert.That(ReadDeclarations(literals).Select(static d => d.Name),
                Is.EqualTo(new[] { "Orleans.Lattice.Primitives" }),
                "A namespace spelled inside a comment or a raw string literal declares nothing.");
        });
    }

    private static void AssertScanIsNotVacuous(FrameworkNamespaces framework, DeclarationScan scan)
    {
        Assert.That(scan.Failures, Is.Empty,
            "Unreadable is not clean: these tracked files could not be read."
                + Environment.NewLine + string.Join(Environment.NewLine, scan.Failures));

        HygieneDenominator.RequireExamined(
            framework.AssembliesRead,
            nameof(FrameworkNamespaceShadowingHygieneTests),
            "Orleans framework assemblies",
            AppContext.BaseDirectory + " (Orleans.*.dll, excluding Orleans.Lattice*)");
        HygieneDenominator.RequireExamined(
            scan.FilesScanned,
            nameof(FrameworkNamespaceShadowingHygieneTests),
            "C# files",
            "src/ and test/ (every tracked *.cs file beneath them)");
        HygieneDenominator.RequireExamined(
            scan.Declarations.Count(static d => IsFirstParty(d.Name)),
            nameof(FrameworkNamespaceShadowingHygieneTests),
            "Orleans.Lattice namespace declarations",
            "src/ and test/");

        // The anchor is derived from a compiled type rather than written as a
        // literal, so the control follows the framework if GrainId ever moves.
        var anchorNamespace = typeof(GrainId).Namespace!;
        var anchorSegment = anchorNamespace[(FrameworkRoot.Length + 1)..].Split('.')[0];
        Assert.Multiple(() =>
        {
            Assert.That(framework.AssemblyNames, Does.Contain(typeof(GrainId).Assembly.GetName().Name),
                "The assembly defining GrainId was not among those read, so the framework list is "
                    + "incomplete and the gate would miss the very namespace that motivated it.");
            Assert.That(framework.Segments.ContainsKey(anchorSegment), Is.True,
                $"The framework list does not contain '{anchorSegment}' ({anchorNamespace}), the "
                    + "namespace that motivated this gate. The metadata reader has stopped recognising "
                    + "public types; fix it, do not delete this assertion.");
        });
    }

    /// <summary>
    /// Maps each shadowing namespace found in the scan to the declarations that
    /// produce it.
    /// </summary>
    private static Dictionary<string, List<NamespaceSite>> FindShadowSites(
        DeclarationScan scan, FrameworkNamespaces framework)
    {
        var sites = new Dictionary<string, List<NamespaceSite>>(StringComparer.Ordinal);
        foreach (var site in scan.Declarations)
        {
            foreach (var shadow in FindShadows(site.Name, framework.SegmentSet))
            {
                if (!sites.TryGetValue(shadow, out var list))
                {
                    sites[shadow] = list = [];
                }

                list.Add(site);
            }
        }

        return sites;
    }

    /// <summary>
    /// Returns every prefix of <paramref name="declaredNamespace"/> that ends in
    /// a segment, below the first-party root, which names a framework namespace.
    /// </summary>
    /// <param name="declaredNamespace">The fully-qualified declared namespace.</param>
    /// <param name="frameworkSegments">Names <c>X</c> for which <c>Orleans.X</c> is a framework namespace.</param>
    /// <returns>The shadowing namespaces, outermost first; empty when none.</returns>
    internal static List<string> FindShadows(string declaredNamespace, IReadOnlySet<string> frameworkSegments)
    {
        ArgumentNullException.ThrowIfNull(declaredNamespace);
        ArgumentNullException.ThrowIfNull(frameworkSegments);

        var shadows = new List<string>();
        if (!IsFirstParty(declaredNamespace) || declaredNamespace.Length == FirstPartyRoot.Length)
        {
            return shadows;
        }

        var end = FirstPartyRoot.Length;
        while (end < declaredNamespace.Length)
        {
            var start = end + 1;
            var next = declaredNamespace.IndexOf('.', start);
            end = next < 0 ? declaredNamespace.Length : next;
            if (frameworkSegments.Contains(declaredNamespace[start..end]))
            {
                shadows.Add(declaredNamespace[..end]);
            }
        }

        return shadows;
    }

    private static bool IsFirstParty(string name) =>
        name.StartsWith(FirstPartyRoot, StringComparison.Ordinal)
        && (name.Length == FirstPartyRoot.Length || name[FirstPartyRoot.Length] == '.');

    private static string Describe(string shadow, List<NamespaceSite> sites, FrameworkNamespaces framework)
    {
        var segment = shadow[(shadow.LastIndexOf('.') + 1)..];
        var first = sites[0];
        var parent = shadow[..shadow.LastIndexOf('.')];
        return $"{shadow} shadows {FrameworkRoot}.{segment} (shipped in {framework.Segments[segment]}): "
            + $"it captures '{segment}.X' for every file under {parent}. "
            + $"Declared at {first.Path}({first.Line})"
            + (sites.Count > 1 ? $" and {sites.Count - 1} other place(s)." : ".");
    }

    /// <summary>
    /// Reads the namespace declarations in one C# source text, composing nested
    /// block namespaces with their parents.
    /// </summary>
    /// <param name="source">The C# source text.</param>
    /// <returns>Each declaration's full name and 1-based line, in source order.</returns>
    internal static List<(string Name, int Line)> ReadDeclarations(string source)
    {
        ArgumentNullException.ThrowIfNull(source);

        var root = CSharpSyntaxTree.ParseText(source).GetRoot();
        var declarations = new List<(string Name, int Line)>();
        foreach (var node in root.DescendantNodes(static n => n is CompilationUnitSyntax or BaseNamespaceDeclarationSyntax))
        {
            if (node is not BaseNamespaceDeclarationSyntax declaration) continue;

            var name = declaration.Name.ToString();
            for (var parent = declaration.Parent; parent is BaseNamespaceDeclarationSyntax outer; parent = outer.Parent)
            {
                name = outer.Name + "." + name;
            }

            var line = declaration.Name.GetLocation().GetLineSpan().StartLinePosition.Line + 1;
            declarations.Add((name.Replace(" ", string.Empty, StringComparison.Ordinal), line));
        }

        return declarations;
    }

    private static DeclarationScan ScanDeclarations()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var files = HygieneRepository.EnumerateFiles(Path.Combine(repoRoot, "src"), "*.cs")
            .Concat(HygieneRepository.EnumerateFiles(Path.Combine(repoRoot, "test"), "*.cs"))
            .ToList();

        var declarations = new ConcurrentBag<NamespaceSite>();
        var failures = new ConcurrentBag<string>();

        // Parsing is the whole cost of this gate and every file is independent.
        Parallel.ForEach(files, file =>
        {
            var relative = Path.GetRelativePath(repoRoot, file).Replace('\\', '/');
            var text = HygieneFiles.TryReadText(file, out var failure);
            if (text is null)
            {
                failures.Add(relative + ": " + failure);
                return;
            }

            // Cheap prefilter: a file that never spells the keyword declares no
            // namespace, so parsing it cannot change the result.
            if (!text.Contains("namespace", StringComparison.Ordinal)) return;

            foreach (var (name, line) in ReadDeclarations(text))
            {
                declarations.Add(new NamespaceSite(name, relative, line));
            }
        });

        var ordered = declarations
            .OrderBy(static d => d.Path, StringComparer.Ordinal)
            .ThenBy(static d => d.Line)
            .ToList();
        return new DeclarationScan(files.Count, ordered, failures.OrderBy(static f => f, StringComparer.Ordinal).ToList());
    }

    /// <summary>
    /// Reads, from assembly metadata alone, every name <c>X</c> for which a
    /// deployed Orleans framework assembly ships a public type in
    /// <c>Orleans.X</c> or beneath it.
    /// </summary>
    private static FrameworkNamespaces ReadFrameworkNamespaces()
    {
        var segments = new Dictionary<string, string>(StringComparer.Ordinal);
        var assemblyNames = new List<string>();

        foreach (var path in Directory.EnumerateFiles(AppContext.BaseDirectory, FrameworkRoot + ".*.dll")
                     .Order(StringComparer.Ordinal))
        {
            var fileName = Path.GetFileNameWithoutExtension(path);
            if (IsFirstParty(fileName)) continue;

            using var stream = File.OpenRead(path);
            using var pe = new PEReader(stream);
            if (!pe.HasMetadata) continue;

            var reader = pe.GetMetadataReader();
            assemblyNames.Add(reader.GetString(reader.GetAssemblyDefinition().Name));

            foreach (var handle in reader.TypeDefinitions)
            {
                var type = reader.GetTypeDefinition(handle);
                if ((type.Attributes & TypeAttributes.VisibilityMask) != TypeAttributes.Public) continue;

                var ns = reader.GetString(type.Namespace);
                if (!ns.StartsWith(FrameworkRoot + ".", StringComparison.Ordinal)) continue;

                var start = FrameworkRoot.Length + 1;
                var dot = ns.IndexOf('.', start);
                var segment = dot < 0 ? ns[start..] : ns[start..dot];
                segments.TryAdd(segment, fileName);
            }
        }

        return new FrameworkNamespaces(assemblyNames.Count, assemblyNames, segments);
    }

    private sealed record NamespaceSite(string Name, string Path, int Line);

    private sealed record DeclarationScan(int FilesScanned, IReadOnlyList<NamespaceSite> Declarations, IReadOnlyList<string> Failures);

    private sealed record FrameworkNamespaces(int AssembliesRead, IReadOnlyList<string> AssemblyNames, IReadOnlyDictionary<string, string> Segments)
    {
        public IReadOnlySet<string> SegmentSet { get; } = new HashSet<string>(Segments.Keys, StringComparer.Ordinal);
    }
}
