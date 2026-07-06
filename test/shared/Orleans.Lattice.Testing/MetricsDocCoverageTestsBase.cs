using System.Text.RegularExpressions;
using NUnit.Framework;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Testing;

/// <summary>
/// Reusable drift-guard base that asserts every metric instrument declared in a
/// package's source is documented, by its exact dotted name, in each of a supplied
/// set of Markdown reference documents (for example <c>docs/lattice/metrics.md</c>
/// and <c>docs/lattice.dashboards/metrics-to-panel-map.md</c>). A concrete subclass
/// in a package's test project supplies the repository-relative source directories
/// to scan (<see cref="SourceRoots"/>) and the repository-relative paths of the
/// reference docs (<see cref="DocRelativePaths"/>).
/// </summary>
/// <remarks>
/// <para>
/// Enumeration is a deterministic scan of the <c>"orleans.lattice.&lt;...&gt;"</c>
/// string literals in the package's <c>.cs</c> sources, NOT a live
/// <see cref="System.Diagnostics.Metrics.MeterListener"/> snapshot. A live snapshot
/// is order-dependent: instruments whose factories run only when a subsystem starts
/// (for example an internal grain that creates its counters at type-initialisation)
/// are visible only after some sibling test has exercised them, so a snapshot taken
/// inside the owning package's test assembly varies with test order. Scanning the
/// source is order-independent and complete - it sees grain-declared instruments
/// too - which is exactly what a "docs match source" guard needs.
/// </para>
/// <para>
/// Matching is by exact dotted instrument name (the form the docs use in their
/// instrument-column back-ticks); it is deliberately strict and does not expand
/// grouped shorthand such as <c>a.{x, y}.b</c>, so a doc that only mentions an
/// instrument inside a grouped bullet must also carry an explicit row. A
/// non-instrument literal that happens to share the prefix (a meter name, a stream
/// namespace) is excluded via <see cref="NonInstrumentLiterals"/>; a pre-existing
/// documentation backlog is tolerated via <see cref="IntentionallyUndocumented"/>,
/// with the guard still failing for any <em>new</em> instrument.
/// </para>
/// </remarks>
public abstract class MetricsDocCoverageTestsBase
{
    private static readonly Regex SourceLiteralRegex =
        new("\"(orleans\\.lattice(?:\\.[a-z0-9_]+)+)\"", RegexOptions.Compiled);

    private static readonly Regex DocNameRegex =
        new(@"\borleans\.lattice(?:\.[a-z0-9_]+)+\b", RegexOptions.Compiled);

    /// <summary>
    /// Repository-root-relative directories (forward-slash separated) whose
    /// <c>.cs</c> sources declare the instruments under test.
    /// </summary>
    protected abstract IEnumerable<string> SourceRoots { get; }

    /// <summary>
    /// Repository-root-relative paths (forward-slash separated) of the Markdown
    /// documents that must mention every scanned instrument by its exact dotted name.
    /// </summary>
    protected abstract IEnumerable<string> DocRelativePaths { get; }

    /// <summary>
    /// Dotted <c>orleans.lattice.*</c> literals that are NOT meter instruments
    /// (meter names, stream namespaces, activity-source names) and so must be
    /// excluded from the coverage requirement.
    /// </summary>
    protected virtual IReadOnlySet<string> NonInstrumentLiterals { get; } =
        new HashSet<string>(StringComparer.Ordinal);

    /// <summary>
    /// Instrument names that are intentionally (or, for a pre-existing backlog,
    /// temporarily) not required in the documents. Override with a justifying
    /// comment to tolerate a gap.
    /// </summary>
    protected virtual IReadOnlySet<string> IntentionallyUndocumented { get; } =
        new HashSet<string>(StringComparer.Ordinal);

    /// <summary>
    /// Every instrument declared in the package source is mentioned, by its exact
    /// dotted name, in each document listed in <see cref="DocRelativePaths"/>, so a
    /// new instrument cannot ship without a documentation entry.
    /// </summary>
    [Test]
    public void Every_instrument_declared_in_source_is_documented_in_each_reference_doc()
    {
        var root = HygieneRepository.FindRepoRoot();
        var instruments = ScanInstrumentNames(root)
            .Where(n => !NonInstrumentLiterals.Contains(n))
            .Where(n => !IntentionallyUndocumented.Contains(n))
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();

        Assert.That(instruments, Is.Not.Empty,
            "The source scan found no instrument literals - check that SourceRoots points at the package's src directory.");

        var missing = new List<string>();
        foreach (var rel in DocRelativePaths)
        {
            var path = Path.Combine(root, rel.Replace('/', Path.DirectorySeparatorChar));
            Assert.That(File.Exists(path), Is.True, $"Reference doc '{rel}' was not found at '{path}'.");

            var documented = DocumentedNames(File.ReadAllText(path));
            foreach (var name in instruments)
            {
                if (!documented.Contains(name))
                {
                    missing.Add($"{name}  (missing from {rel})");
                }
            }
        }

        missing.Sort(StringComparer.Ordinal);
        Assert.That(missing, Is.Empty,
            "The following instruments are not documented (by exact dotted name) in every reference doc. " +
            "Add a row / entry naming the instrument; if the literal is not a meter instrument, add it to " +
            "NonInstrumentLiterals; if the omission is a pre-existing backlog, override IntentionallyUndocumented " +
            $"with a justifying comment:{Environment.NewLine}  - " +
            string.Join(Environment.NewLine + "  - ", missing));
    }

    private IReadOnlySet<string> ScanInstrumentNames(string root)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var rel in SourceRoots)
        {
            var dir = Path.Combine(root, rel.Replace('/', Path.DirectorySeparatorChar));
            Assert.That(Directory.Exists(dir), Is.True, $"Source root '{rel}' was not found at '{dir}'.");

            foreach (var file in Directory.EnumerateFiles(dir, "*.cs", SearchOption.AllDirectories))
            {
                foreach (Match m in SourceLiteralRegex.Matches(File.ReadAllText(file)))
                {
                    names.Add(m.Groups[1].Value);
                }
            }
        }

        return names;
    }

    private static HashSet<string> DocumentedNames(string text)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (Match m in DocNameRegex.Matches(text))
        {
            names.Add(m.Value);
        }

        return names;
    }
}
