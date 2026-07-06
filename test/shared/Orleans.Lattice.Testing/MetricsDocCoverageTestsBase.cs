using System.Diagnostics.Metrics;
using System.Text.RegularExpressions;
using NUnit.Framework;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Testing;

/// <summary>
/// Reusable drift-guard base that asserts every instrument published on a single
/// <see cref="System.Diagnostics.Metrics.Meter"/> is documented, by its exact
/// dotted name, in each of a supplied set of Markdown reference documents (for
/// example <c>docs/lattice/metrics.md</c> and
/// <c>docs/lattice.dashboards/metrics-to-panel-map.md</c>). A concrete subclass
/// in a package's test project supplies the meter it owns (via <see cref="Meter"/>),
/// the canonical names of any instruments whose factories are not statically wired
/// at test time (via <see cref="AdditionalInstrumentNames"/>), and the
/// repository-root-relative paths of the docs that must mention every instrument
/// (via <see cref="DocRelativePaths"/>).
/// </summary>
/// <remarks>
/// <para>
/// This mirrors the shared <see cref="MeterDashboardCoverageTestsBase"/>: the
/// reflection-and-filesystem logic lives here once so every package reuses it by
/// construction, and a package that ships a new instrument fails CI unless it also
/// documents it. Matching is by exact dotted instrument name (the form the docs
/// use in their instrument-column back-ticks); it is deliberately strict and does
/// not expand grouped shorthand such as <c>a.{x, y}.b</c>, so a doc that only
/// mentions an instrument inside a grouped bullet must also carry an explicit row.
/// </para>
/// <para>
/// A pre-existing documentation backlog can be tolerated by overriding
/// <see cref="IntentionallyUndocumented"/> with the offending names and a
/// justifying comment; the guard still fails for any <em>new</em> instrument, which
/// is the drift it exists to prevent.
/// </para>
/// </remarks>
public abstract class MetricsDocCoverageTestsBase
{
    private static readonly Regex InstrumentNameRegex =
        new(@"\borleans\.lattice(?:\.[a-z0-9_]+)+\b", RegexOptions.Compiled);

    /// <summary>
    /// The live meter that owns the instruments under test. A snapshot
    /// <see cref="MeterListener"/> enumerates every instrument published on it.
    /// </summary>
    protected abstract Meter Meter { get; }

    /// <summary>
    /// Repository-root-relative paths (forward-slash separated) of the Markdown
    /// documents that must mention every instrument on <see cref="Meter"/> by its
    /// exact dotted name.
    /// </summary>
    protected abstract IEnumerable<string> DocRelativePaths { get; }

    /// <summary>
    /// Canonical dotted names of instruments whose factories are not statically
    /// wired at test time (observable gauges created only when the owning
    /// subsystem starts), so a snapshot <see cref="MeterListener"/> does not see
    /// them. Override to include them in the coverage guard.
    /// </summary>
    protected virtual IEnumerable<string> AdditionalInstrumentNames => Array.Empty<string>();

    /// <summary>
    /// Canonical dotted names of instruments that are intentionally (or, for a
    /// pre-existing backlog, temporarily) not required in the documents. Override
    /// with a justifying comment to tolerate a gap.
    /// </summary>
    protected virtual IReadOnlySet<string> IntentionallyUndocumented { get; } =
        new HashSet<string>(StringComparer.Ordinal);

    /// <summary>
    /// Every instrument published on <see cref="Meter"/> is mentioned, by its exact
    /// dotted name, in each document listed in <see cref="DocRelativePaths"/>, so a
    /// new instrument cannot ship without a documentation entry.
    /// </summary>
    [Test]
    public void Every_instrument_on_the_meter_is_documented_in_each_reference_doc()
    {
        var root = HygieneRepository.FindRepoRoot();
        var instruments = InstrumentNames()
            .Where(n => !IntentionallyUndocumented.Contains(n))
            .ToList();

        Assert.That(instruments, Is.Not.Empty,
            "The meter published no instruments - the subclass likely failed to force the owning " +
            "type-initialiser (touch a public static field on the declaring type from the Meter getter).");

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
            "Add a row / entry naming the instrument, or - if the omission is deliberate - override " +
            $"IntentionallyUndocumented with a justifying comment:{Environment.NewLine}  - " +
            string.Join(Environment.NewLine + "  - ", missing));
    }

    private IEnumerable<string> InstrumentNames()
    {
        var names = new HashSet<string>(StringComparer.Ordinal);

        // Force the owning type-initialiser so statically-wired instruments are
        // published before the snapshot listener enumerates them.
        var meter = Meter;

        using (var listener = new MeterListener())
        {
            listener.InstrumentPublished = (instrument, _) =>
            {
                if (ReferenceEquals(instrument.Meter, meter))
                {
                    names.Add(instrument.Name);
                }
            };
            listener.Start();
        }

        foreach (var name in AdditionalInstrumentNames)
        {
            names.Add(name);
        }

        return names;
    }

    private static HashSet<string> DocumentedNames(string text)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (Match m in InstrumentNameRegex.Matches(text))
        {
            names.Add(m.Value);
        }

        return names;
    }
}
