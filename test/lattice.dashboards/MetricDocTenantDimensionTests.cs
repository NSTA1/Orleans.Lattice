using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Dashboards.Tests;

/// <summary>
/// Asserts that every instrument row in
/// <c>docs/lattice.dashboards/metrics-to-panel-map.md</c> documents the
/// universal <c>tenant</c> dimension in its tag column.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why the tag column and not the row's presence.</b> Every other
/// doc-coverage gate in this repository answers "does this instrument have a
/// row?". None reads the tag column, so a row whose tag set is wrong is
/// indistinguishable from a correct one. The tag column is what a reader uses to
/// construct a <i>series identity</i>: a query keyed on the three tags a row
/// lists for a four-tag instrument matches no real series, and the failure
/// presents not as a missing label but as every series on that instrument
/// reported as changed, or as absent. Issue #2997 was filed after a run-over-run
/// acceptance diff built from this column mis-matched an instrument's entire
/// population and reported it as a system change.
/// </para>
/// <para>
/// <b>The expectation is universal, so there is no list to hand-author.</b>
/// <c>LatticeTenantLabel</c> is the single source of the derived <c>tenant</c>
/// dimension and every instrument carries it, on tenancy-on and tenancy-off
/// clusters alike. That is not prose here: it is enforced by
/// <c>TenantMetricDimensionHygieneTests.Every_metric_emission_site_carries_the_tenant_dimension</c>
/// and, for observable instruments, by
/// <c>Every_observable_instrument_registration_wires_the_tenant_dimension</c>.
/// This gate therefore compares against a rule rather than against an expected
/// text list. A per-row expected-tag list would reproduce the very defect it
/// guards - a hand-authored population that drifts - inside its own remedy.
/// </para>
/// <para>
/// <b>The platform sentinel is not an exemption from the dimension.</b> Issue
/// #2997 assumed the defect count was the omitting rows "minus the
/// <c>PlatformSentinelInstruments</c> exemption list", on the premise that
/// genuinely exempt instruments should not list <c>tenant</c>. That premise is
/// false, and the issue's own harm argument is what refutes it:
/// <c>LatticeTenantLabel.Platform</c> is a key/value pair setting
/// <c>tenant</c> to the reserved value <c>_platform_</c>, so a sentinel series
/// still carries the label and its identity is still N+1 tags. A sentinel is
/// exempt from carrying a <i>varying</i> tenant, never from carrying the
/// dimension. The exemption list reduces this gate's population by exactly
/// zero, which is why this fixture does not consult it and needs no
/// cross-project reference to reach it.
/// </para>
/// <para>
/// <b>Two grammars, and why the parser must tell them apart.</b> The tag column
/// is written two ways: a comma-separated list of keys
/// (<c>`tree`, `shard`</c>), and a semicolon-separated list of key/domain
/// claims (<c>`outcome` = `armed`, `empty`; `phase` = ...</c>). Splitting the
/// second on commas yields its tag <i>values</i> as though they were keys, so a
/// naive parser reports <c>empty</c> as a documented tag. The discriminator is
/// the presence of <c>=</c>. <see cref="TheParserReadsKeysNotValuesInTheDomainGrammar"/>
/// pins that behaviour on literal input, because a parser that silently
/// mis-reads one grammar would let this gate pass for the wrong reason - the
/// failure mode where a check's name is accurate, its message would be
/// accurate, and only its assertion is wrong.
/// </para>
/// <para>
/// <b>Loud when it matches nothing.</b> A gate whose scan finds no work reports
/// the same green as one that checked everything, so the scan's population is
/// asserted separately by <see cref="TheScanDiscoversInstrumentRows"/> and
/// <see cref="TheScanCoversEveryInstrumentTable"/>. The latter exists because a
/// table-walking parser that fails to reset at a table boundary silently adopts
/// the previous table's column layout; that bug was present in the throwaway
/// probe this fixture replaced and was caught only by two parsers disagreeing.
/// </para>
/// </remarks>
[TestFixture]
public sealed class MetricDocTenantDimensionTests
{
    /// <summary>The documentation file whose tag column this gate reads.</summary>
    private const string PanelMapRelativePath = "docs/lattice.dashboards/metrics-to-panel-map.md";

    /// <summary>The universal dimension every instrument row must document.</summary>
    private const string TenantTagKey = "tenant";

    /// <summary>
    /// A floor on the instrument rows the scan must find. The real count is far
    /// higher; this is a vacuity guard, not a census, so it is deliberately loose
    /// enough that ordinary additions and removals never touch it and tight
    /// enough that a parse which silently stops matching fails.
    /// </summary>
    private const int MinimumInstrumentRows = 300;

    /// <summary>
    /// A floor on the number of distinct instrument tables the scan must cover.
    /// </summary>
    private const int MinimumInstrumentTables = 5;

    /// <summary>One scanned instrument row.</summary>
    /// <param name="Line">The 1-based line number.</param>
    /// <param name="TableLine">The 1-based line of the table's header row.</param>
    /// <param name="Instrument">The instrument named in the first cell.</param>
    /// <param name="TagCell">The raw text of the tag cell.</param>
    /// <param name="TagKeys">The tag keys parsed from <paramref name="TagCell"/>.</param>
    internal sealed record InstrumentRow(
        int Line,
        int TableLine,
        string Instrument,
        string TagCell,
        IReadOnlyList<string> TagKeys);

    // ------------------------------------------------------------- the gate

    /// <summary>
    /// Every instrument row must name the <c>tenant</c> dimension in its tag
    /// column.
    /// </summary>
    [Test]
    public void EveryInstrumentRowDocumentsTheTenantDimension()
    {
        var missing = ScannedRows()
            .Where(static row => !row.TagKeys.Contains(TenantTagKey, StringComparer.Ordinal))
            .Select(static row => $"{PanelMapRelativePath}:{row.Line} {row.Instrument} -> [{row.TagCell}]")
            .ToList();

        Assert.That(missing, Is.Empty,
            "Every Orleans.Lattice instrument carries the derived `tenant` dimension, so a row that omits it " +
            "documents a series identity narrower than the real one and any query built from it matches nothing. " +
            "Add `tenant` to the tag column. A platform-sentinel instrument is not exempt: it carries `tenant` " +
            "with the reserved `_platform_` value, so its identity includes the label too.\n"
            + string.Join("\n", missing));
    }

    /// <summary>
    /// No instrument row may claim an empty tag set.
    /// </summary>
    /// <remarks>
    /// Subsumed by <see cref="EveryInstrumentRowDocumentsTheTenantDimension"/> -
    /// a row claiming no tags cannot name <c>tenant</c> - but kept separate
    /// because the two say different things to whoever reads the failure. An
    /// omission is an incomplete claim; <c>(none)</c> is an affirmative false
    /// one, and forty rows carried it when this gate was written.
    /// </remarks>
    [Test]
    public void NoInstrumentRowClaimsAnEmptyTagSet()
    {
        var empty = ScannedRows()
            .Where(static row => row.TagKeys.Count == 0 || EmptyClaimRegex.IsMatch(row.TagCell))
            .Select(static row => $"{PanelMapRelativePath}:{row.Line} {row.Instrument} -> [{row.TagCell}]")
            .ToList();

        Assert.That(empty, Is.Empty,
            "A row claiming `(none)`, `-` or an empty tag set asserts the instrument has no dimensions at all. " +
            "No instrument in this repository does: every one carries `tenant`. Replace the claim with the real " +
            "tag set.\n" + string.Join("\n", empty));
    }

    // --------------------------------------------------- loud-on-empty scan

    /// <summary>The row scan must find a population to check.</summary>
    [Test]
    public void TheScanDiscoversInstrumentRows()
    {
        Assert.That(ScannedRows(), Has.Count.GreaterThanOrEqualTo(MinimumInstrumentRows),
            $"The instrument-row scan of {PanelMapRelativePath} found too few rows to be reading the document. " +
            "A gate whose scan matches nothing reports the same green as one that checked everything.");
    }

    /// <summary>The scan must reach every instrument table, not just the first.</summary>
    [Test]
    public void TheScanCoversEveryInstrumentTable()
    {
        var tables = ScannedRows().Select(static row => row.TableLine).Distinct().ToList();

        Assert.That(tables, Has.Count.GreaterThanOrEqualTo(MinimumInstrumentTables),
            $"The scan covered {tables.Count} table(s). The document splits its instruments across several " +
            "tables whose column layouts differ, so a parser that stops at the first - or that carries one " +
            "table's column index into the next - checks a subset while appearing to check everything.");
    }

    /// <summary>Every scanned row must yield at least one tag key.</summary>
    [Test]
    public void EveryScannedRowYieldsTagKeys()
    {
        var unparsed = ScannedRows()
            .Where(static row => row.TagKeys.Count == 0)
            .Select(static row => $"{PanelMapRelativePath}:{row.Line} {row.Instrument} -> [{row.TagCell}]")
            .ToList();

        Assert.That(unparsed, Is.Empty,
            "A tag cell that parses to no keys is either an empty claim or a cell this parser cannot read. " +
            "Either way the gate is not checking that row.\n" + string.Join("\n", unparsed));
    }

    // ------------------------------------------------------ positive controls

    /// <summary>
    /// The detector must report a row that omits the dimension.
    /// </summary>
    /// <remarks>
    /// The real document satisfies the gate, so without this the suite could not
    /// distinguish "every row documents the dimension" from "the detector never
    /// fires". It runs the detector over literal input instead of perturbing the
    /// document.
    /// </remarks>
    [Test]
    public void ADetectorRunOverARowMissingTheDimensionReportsIt()
    {
        var keys = ParseTagKeys("`tree`, `shard`");

        Assert.That(keys, Is.EqualTo(new[] { "tree", "shard" }));
        Assert.That(keys.Contains(TenantTagKey, StringComparer.Ordinal), Is.False,
            "A row listing only `tree` and `shard` must be seen as omitting the tenant dimension.");
    }

    /// <summary>A row that names the dimension must be accepted.</summary>
    [Test]
    public void ADetectorRunOverACompliantRowAcceptsIt()
    {
        Assert.That(ParseTagKeys("`tree`, `shard`, `tenant`").Contains(TenantTagKey, StringComparer.Ordinal), Is.True);
    }

    /// <summary>
    /// The parser must read keys, not values, in the key/domain grammar.
    /// </summary>
    /// <remarks>
    /// This is the assertion that stops the gate passing for the wrong reason.
    /// Splitting <c>`outcome` = `armed`, `empty`</c> on commas yields
    /// <c>outcome</c>, <c>armed</c> and <c>empty</c> as though all three were tag
    /// keys. Such a parser would report a tenant dimension on any row whose tag
    /// <i>values</i> happened to include the word, and would miss it on rows
    /// where it is a genuine key.
    /// </remarks>
    [Test]
    public void TheParserReadsKeysNotValuesInTheDomainGrammar()
    {
        Assert.Multiple(static () =>
        {
            Assert.That(ParseTagKeys("`outcome` = `armed`, `empty`, `faulted`"),
                Is.EqualTo(new[] { "outcome" }),
                "the domain grammar lists one key and its value domain, not four keys");

            Assert.That(ParseTagKeys("`arm` = `retire`, `ingest`; `kind` = `stalled`"),
                Is.EqualTo(new[] { "arm", "kind" }),
                "semicolons separate key/domain claims in the domain grammar");

            Assert.That(ParseTagKeys("`tenant` = `_platform_` (cluster aggregate)"),
                Is.EqualTo(new[] { TenantTagKey }),
                "a parenthetical qualifier is not a tag key");

            Assert.That(ParseTagKeys("`outcome` = `armed`, `tenant`").Contains(TenantTagKey, StringComparer.Ordinal),
                Is.False,
                "a tag VALUE spelled 'tenant' must not satisfy the dimension check");
        });
    }

    /// <summary>An unbackticked key must still be read.</summary>
    /// <remarks>
    /// The rows disagreed on backticks when this gate was written, and the issue
    /// is explicit that a gate reporting cosmetic differences as defects gets
    /// switched off. Normalisation happens in the parser so the gate never sees
    /// the difference.
    /// </remarks>
    [Test]
    public void TheParserNormalisesBacktickedAndBareKeys()
    {
        Assert.That(ParseTagKeys("`tree`, `kind`, tenant"),
            Is.EqualTo(new[] { "tree", "kind", TenantTagKey }));
    }

    /// <summary>An empty claim must parse to no keys.</summary>
    [Test]
    public void TheParserReadsAnEmptyClaimAsNoKeys()
    {
        Assert.Multiple(static () =>
        {
            Assert.That(ParseTagKeys("(none)"), Is.Empty);
            Assert.That(ParseTagKeys("none"), Is.Empty);
            Assert.That(ParseTagKeys("-"), Is.Empty);
        });
    }

    // ---------------------------------------------------------------- scan

    private static readonly Regex EmptyClaimRegex = new(
        @"^\(?\s*(none|-|n/a)\s*\)?$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static readonly Regex BacktickedRegex = new(
        "`([^`]+)`",
        RegexOptions.CultureInvariant);

    /// <summary>
    /// Parses the tag keys from one tag cell, in either grammar.
    /// </summary>
    internal static IReadOnlyList<string> ParseTagKeys(string cell)
    {
        var trimmed = cell.Trim();
        if (trimmed.Length == 0 || EmptyClaimRegex.IsMatch(trimmed))
        {
            return [];
        }

        // The key/domain grammar states "key = value, value"; splitting it on
        // commas would yield its values as keys. Semicolons separate the claims.
        var separator = trimmed.Contains('=', StringComparison.Ordinal) ? ';' : ',';
        var keys = new List<string>();

        foreach (var entry in trimmed.Split(separator))
        {
            var text = entry.Trim();
            if (text.Length == 0)
            {
                continue;
            }

            var match = BacktickedRegex.Match(text);
            var token = match.Success ? match.Groups[1].Value : text;

            // Drop a value domain and any parenthetical qualifier.
            var equals = token.IndexOf('=', StringComparison.Ordinal);
            if (equals >= 0)
            {
                token = token[..equals];
            }

            var paren = token.IndexOf('(', StringComparison.Ordinal);
            if (paren >= 0)
            {
                token = token[..paren];
            }

            token = token.Trim().Trim('`', '*', ' ');
            if (token.Length > 0 && !EmptyClaimRegex.IsMatch(token))
            {
                keys.Add(token);
            }
        }

        return keys;
    }

    private static IReadOnlyList<InstrumentRow> ScannedRows() => ScannedRowsLazy.Value;

    private static readonly Lazy<IReadOnlyList<InstrumentRow>> ScannedRowsLazy = new(ScanCore);

    private static IReadOnlyList<InstrumentRow> ScanCore()
    {
        var root = HygieneRepository.FindRepoRoot();
        var path = Path.Combine(root, PanelMapRelativePath.Replace('/', Path.DirectorySeparatorChar));
        var lines = File.ReadAllLines(path);
        var rows = new List<InstrumentRow>();

        var tagIndex = -1;
        var tableLine = 0;

        for (var i = 0; i < lines.Length; i++)
        {
            var line = lines[i].TrimEnd();

            // A table ends at the first line that is not a row. Resetting here is
            // what stops a later table - the tenant-value legend in particular -
            // being read with this table's column layout.
            if (!line.StartsWith('|'))
            {
                tagIndex = -1;
                tableLine = 0;
                continue;
            }

            var cells = line.Split('|');
            if (cells.Length < 4)
            {
                continue;
            }

            var first = cells[1].Trim();

            if (string.Equals(first, "Instrument", StringComparison.Ordinal))
            {
                tagIndex = Array.FindIndex(cells, cell => string.Equals(cell.Trim(), "Tags", StringComparison.Ordinal));
                tableLine = tagIndex >= 0 ? i + 1 : 0;
                continue;
            }

            if (tagIndex < 0 || tagIndex >= cells.Length || first.StartsWith("---", StringComparison.Ordinal))
            {
                continue;
            }

            if (!first.StartsWith('`'))
            {
                continue;
            }

            var instrument = first.Trim('`', ' ');

            // Every instrument name is dotted. The document also carries tables
            // keyed on single-word values, which are not instruments.
            if (!instrument.Contains('.', StringComparison.Ordinal))
            {
                continue;
            }

            var cell = cells[tagIndex].Trim();
            rows.Add(new InstrumentRow(i + 1, tableLine, instrument, cell, ParseTagKeys(cell)));
        }

        return rows;
    }
}
