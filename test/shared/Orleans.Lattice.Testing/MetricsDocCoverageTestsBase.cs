using System.Text.RegularExpressions;
using NUnit.Framework;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Testing;

/// <summary>
/// Reusable drift-guard base that asserts a package's metric documentation and its
/// source agree in <em>both</em> directions: every instrument declared in the
/// package's source is documented by its exact dotted name in each of a supplied set
/// of Markdown reference documents (for example <c>docs/lattice/metrics.md</c> and
/// <c>docs/lattice.dashboards/metrics-to-panel-map.md</c>), and every instrument name
/// those documents mention is declared somewhere in the repository's source. A
/// concrete subclass in a package's test project supplies the repository-relative
/// source directories to scan (<see cref="SourceRoots"/>) and the
/// repository-relative paths of the reference docs (<see cref="DocRelativePaths"/>).
/// </summary>
/// <remarks>
/// <para>
/// Enumeration is a deterministic scan of the package's <c>.cs</c> sources for
/// string literals matching <see cref="InstrumentNamePrefix"/> followed by dotted
/// segments, NOT a live
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
/// <para>
/// <b>The reverse direction is asserted separately and is not symmetric with the
/// forward one.</b> The forward test asks whether each declared instrument reaches
/// the docs, and so is scoped to the package's own <see cref="SourceRoots"/>. The
/// reverse test asks whether each documented name corresponds to a real declaration,
/// which is a question about the repository rather than about one package: the
/// shared panel map carries rows for every publishing package at once, so comparing
/// its contents against a single package's sources would report every other
/// package's rows as orphans. The reverse denominator is therefore
/// <see cref="ReverseScanRoots"/>, defaulting to the whole of <c>src</c>, and it is
/// the raw literal set <em>before</em> <see cref="NonInstrumentLiterals"/> filtering
/// so that a documented meter name such as <c>orleans.lattice.replication</c>
/// resolves against the constant that declares it.
/// </para>
/// </remarks>
public abstract class MetricsDocCoverageTestsBase
{
    private Regex? _sourceLiteralRegex;
    private Regex? _docNameRegex;

    /// <summary>
    /// The dotted prefix every instrument under test shares. Defaults to
    /// <c>orleans.lattice</c>, the core meter's naming scheme. A package whose
    /// instruments are published under a different prefix - for example the
    /// repository-context surface's <c>repocontext.*</c> - overrides this, without
    /// which the scan matches nothing in that package and the guard cannot cover it.
    /// </summary>
    protected virtual string InstrumentNamePrefix => "orleans.lattice";

    private Regex SourceLiteralRegex =>
        _sourceLiteralRegex ??= new Regex(
            "\"(" + Regex.Escape(InstrumentNamePrefix) + "(?:\\.[a-z0-9_]+)+)\"", RegexOptions.Compiled);

    private Regex DocNameRegex =>
        _docNameRegex ??= new Regex(
            @"\b" + Regex.Escape(InstrumentNamePrefix) + @"(?:\.[a-z0-9_]+)+\b", RegexOptions.Compiled);

    /// <summary>
    /// Repository-root-relative directories (forward-slash separated) whose
    /// <c>.cs</c> sources declare the instruments under test.
    /// </summary>
    protected abstract IEnumerable<string> SourceRoots { get; }

    /// <summary>
    /// Repository-root-relative directories (forward-slash separated) scanned to
    /// decide whether a name mentioned in a reference doc is declared anywhere in the
    /// repository. Defaults to the whole of <c>src</c>, which is what the reverse
    /// direction needs: the reference docs are shared across packages, so a
    /// package-scoped denominator would report a sibling package's documented
    /// instruments as orphans. Narrow it only in a fixture whose docs are genuinely
    /// private to one package.
    /// </summary>
    protected virtual IEnumerable<string> ReverseScanRoots => new[] { "src" };

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
    /// Names that a reference doc may mention without any corresponding declaration
    /// in source, keyed by name with the reason as the value. Deliberately a map
    /// rather than a set: an exemption that does not say why it exists is
    /// indistinguishable from one nobody has revisited, and the reverse direction
    /// exists precisely to stop a doc entry outliving its instrument.
    /// </summary>
    /// <remarks>
    /// Empty by default, and empty today across every enrolled package, which is a
    /// measured fact rather than an aspiration: the wildcard family prefixes that a
    /// naive scan reports (<c>orleans.lattice.wal</c> lifted out of the prose form
    /// <c>orleans.lattice.wal.*</c>) are excluded structurally instead, by
    /// <see cref="IsTruncatedFamilyPrefix"/>, because they are an artefact of the
    /// match ending early rather than a documentation defect. Resolving them
    /// structurally keeps this map available for genuine exemptions, where a
    /// non-empty entry is a signal rather than noise.
    /// </remarks>
    protected virtual IReadOnlyDictionary<string, string> IntentionallyDocumentedWithoutDeclaration { get; } =
        new Dictionary<string, string>(StringComparer.Ordinal);

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
            $"The source scan found no '{InstrumentNamePrefix}.*' instrument literals - check that SourceRoots "
            + "points at the package's src directory and that InstrumentNamePrefix matches its naming scheme. "
            + "This is the anti-vacuity floor: a scan that silently matches nothing must fail here rather than "
            + "report a fully documented package it never examined.");

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

    /// <summary>
    /// Every instrument name mentioned in a reference doc is declared somewhere in
    /// source, so a renamed or deleted instrument cannot leave a documentation row
    /// standing that describes a series no host will ever emit.
    /// </summary>
    /// <remarks>
    /// The complement of
    /// <see cref="Every_instrument_declared_in_source_is_documented_in_each_reference_doc"/>.
    /// Without it the pair is only half a guard: the forward direction cannot fail
    /// for a name that no longer exists in source, because a deleted instrument
    /// simply drops out of its own denominator. The failure that motivates this is
    /// silent in the worst way - a reader consults the panel map, finds the row,
    /// writes the query, and reads the resulting empty series as a measured zero.
    /// </remarks>
    [Test]
    public void Every_instrument_named_in_a_reference_doc_is_declared_in_source()
    {
        var root = HygieneRepository.FindRepoRoot();
        var declared = ScanLiterals(root, ReverseScanRoots, nameof(ReverseScanRoots));

        Assert.That(declared, Is.Not.Empty,
            $"The reverse scan found no '{InstrumentNamePrefix}.*' literals anywhere under ReverseScanRoots "
            + $"({string.Join(", ", ReverseScanRoots)}) - so every documented name would be reported as an "
            + "orphan, or, had the docs also been empty, none would. This is the anti-vacuity floor on the "
            + "denominator: a scan that silently matches nothing must fail here rather than pass by comparing "
            + "one empty set against another.");

        var orphans = new List<string>();
        var examinedNames = 0;
        foreach (var rel in DocRelativePaths)
        {
            var path = Path.Combine(root, rel.Replace('/', Path.DirectorySeparatorChar));
            Assert.That(File.Exists(path), Is.True, $"Reference doc '{rel}' was not found at '{path}'.");

            var lines = File.ReadAllLines(path);
            var namesInDoc = 0;
            for (var i = 0; i < lines.Length; i++)
            {
                foreach (Match m in DocNameRegex.Matches(lines[i]))
                {
                    if (IsTruncatedFamilyPrefix(lines[i], m))
                    {
                        continue;
                    }

                    namesInDoc++;
                    if (declared.Contains(m.Value) || IntentionallyDocumentedWithoutDeclaration.ContainsKey(m.Value))
                    {
                        continue;
                    }

                    orphans.Add($"{m.Value}  ({rel}:{i + 1})");
                }
            }

            Assert.That(namesInDoc, Is.GreaterThan(0),
                $"Reference doc '{rel}' mentions no '{InstrumentNamePrefix}.*' name at all. Either the document "
                + "stopped documenting this package's instruments, or DocNameRegex stopped matching it. Both are "
                + "failures: a doc the reverse direction cannot read is a doc it cannot guard, and passing here "
                + "would report a clean document that was never examined.");

            examinedNames += namesInDoc;
        }

        TestContext.Out.WriteLine(
            $"Reverse direction examined {examinedNames} documented '{InstrumentNamePrefix}.*' mention(s) "
            + $"against {declared.Count} literal(s) declared under {string.Join(", ", ReverseScanRoots)}.");

        orphans.Sort(StringComparer.Ordinal);
        Assert.That(orphans, Is.Empty,
            "The following names are documented but declared nowhere in source. An instrument was most likely "
            + "renamed or removed without its documentation row following; correct or delete the row. If the "
            + "mention is deliberate, add it to IntentionallyDocumentedWithoutDeclaration with the reason:"
            + $"{Environment.NewLine}  - " + string.Join(Environment.NewLine + "  - ", orphans));
    }

    /// <summary>
    /// Every entry in <see cref="IntentionallyDocumentedWithoutDeclaration"/> is
    /// still undeclared, so an exemption cannot outlive the gap it excuses and go on
    /// masking a real orphan under the same name.
    /// </summary>
    [Test]
    public void Every_documented_without_declaration_exemption_is_still_needed()
    {
        if (IntentionallyDocumentedWithoutDeclaration.Count == 0)
        {
            Assert.Pass("No exemptions are declared, so none can be stale.");
        }

        var root = HygieneRepository.FindRepoRoot();
        var declared = ScanLiterals(root, ReverseScanRoots, nameof(ReverseScanRoots));

        var stale = IntentionallyDocumentedWithoutDeclaration
            .Where(e => declared.Contains(e.Key))
            .Select(e => $"{e.Key}  (reason given: {e.Value})")
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();

        Assert.That(stale, Is.Empty,
            "The following names are exempted from the reverse direction but are now declared in source, so the "
            + "exemption is doing nothing except suppressing a future orphan of the same name. Remove them:"
            + $"{Environment.NewLine}  - " + string.Join(Environment.NewLine + "  - ", stale));

        var unreasoned = IntentionallyDocumentedWithoutDeclaration
            .Where(e => string.IsNullOrWhiteSpace(e.Value))
            .Select(e => e.Key)
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();

        Assert.That(unreasoned, Is.Empty,
            "The following exemptions carry no reason. State why the name is documented without a declaration:"
            + $"{Environment.NewLine}  - " + string.Join(Environment.NewLine + "  - ", unreasoned));
    }

    /// <summary>
    /// Whether a doc match is the leading portion of a wildcard or grouped family
    /// form rather than an instrument name in its own right.
    /// </summary>
    /// <param name="line">The line the match was found on.</param>
    /// <param name="match">The match.</param>
    /// <returns><see langword="true"/> when the match should not be checked.</returns>
    /// <remarks>
    /// <see cref="DocNameRegex"/> consumes dotted <c>[a-z0-9_]</c> segments, so the
    /// prose form <c>orleans.lattice.wal.*</c> yields the match
    /// <c>orleans.lattice.wal</c>: the <c>*</c> is not a legal segment character, so
    /// the match ends one segment early and the word boundary is satisfied by the
    /// following dot. The result looks exactly like an orphaned instrument name and
    /// is nothing of the kind. The test is deliberately narrow - only <c>.*</c> and
    /// <c>.{</c>, the two family forms the documents actually use - rather than
    /// "followed by a dot", which would also swallow a genuine orphan that happened
    /// to end a sentence.
    /// </remarks>
    private static bool IsTruncatedFamilyPrefix(string line, Capture match)
    {
        var next = match.Index + match.Length;
        return next + 1 < line.Length
            && line[next] == '.'
            && (line[next + 1] == '*' || line[next + 1] == '{');
    }

    private IReadOnlySet<string> ScanInstrumentNames(string root)
        => ScanLiterals(root, SourceRoots, nameof(SourceRoots));

    private IReadOnlySet<string> ScanLiterals(string root, IEnumerable<string> relativeRoots, string rootsMemberName)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        var examined = 0;
        foreach (var rel in relativeRoots)
        {
            var dir = Path.Combine(root, rel.Replace('/', Path.DirectorySeparatorChar));
            Assert.That(Directory.Exists(dir), Is.True,
                $"{rootsMemberName} entry '{rel}' was not found at '{dir}'.");

            foreach (var file in Directory.EnumerateFiles(dir, "*.cs", SearchOption.AllDirectories))
            {
                examined++;
                foreach (Match m in SourceLiteralRegex.Matches(File.ReadAllText(file)))
                {
                    names.Add(m.Groups[1].Value);
                }
            }
        }

        Assert.That(examined, Is.GreaterThan(0),
            $"{rootsMemberName} ({string.Join(", ", relativeRoots)}) contains no .cs file at all, so the scan "
            + "examined nothing. Fail rather than report an empty result for a set that was never read.");

        return names;
    }

    private HashSet<string> DocumentedNames(string text)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (Match m in DocNameRegex.Matches(text))
        {
            names.Add(m.Value);
        }

        return names;
    }
}
