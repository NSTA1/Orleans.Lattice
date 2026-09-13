using System.Text.RegularExpressions;

namespace Orleans.Lattice.Dashboards.Tests;

/// <summary>
/// Asserts that no bundled Grafana panel reads a byte series under a name form the
/// Prometheus exporter does not emit.
/// </summary>
/// <remarks>
/// <para>
/// The defect this gate exists to catch (issue #2941) is a panel reading
/// <c>orleans_lattice_storage_wal_stored_bytes_bytes_total</c>. No exporter produces
/// that series, so the panel is permanently blank; and because a blank panel and a
/// genuinely zero metric look identical to an operator, the panel reports an
/// absence that cannot be interpreted. That is the same defect as an unprimed
/// instrument, moved one layer out.
/// </para>
/// <para>
/// <b>The measured rule.</b> OpenTelemetry.Exporter.Prometheus.AspNetCore
/// 1.15.3-beta.1 - the version every host in this repository pins - sanitizes
/// <c>.</c> to <c>_</c>, then appends the unit suffix <b>only if the name does not
/// already end with it</b>, then appends <c>_total</c> for a monotonic counter. It
/// was established by scraping a live endpoint with a synthetic control instrument
/// declaring unit <c>By</c> under a name <i>not</i> ending in <c>_bytes</c>, which
/// received the suffix. Without that control the absence of the doubled form would
/// have been a failed search rather than a measurement: every <c>By</c> instrument
/// declared under <c>src/</c> is already named <c>*_bytes</c>, so the repository's
/// own instruments cannot distinguish "the exporter does not double the suffix"
/// from "the exporter appends nothing at all".
/// </para>
/// <para>
/// <b>Why this is inventoried rather than counted.</b> The obvious check - grep the
/// dashboards for the one token the issue named - returns a clean single occurrence
/// and is blind to every sibling site. The issue named one panel; the repository
/// held a second reference on the same panel and the gate that should have caught
/// both vouched for the doubled form itself. This gate therefore enumerates every
/// declared byte instrument, derives the forms no exporter can emit for each, and
/// checks the dashboards against that derived set, so a site is covered by
/// construction rather than by having been noticed.
/// </para>
/// <para>
/// <b>Relationship to the sibling name gate.</b>
/// <c>DashboardJsonTests</c> asserts that every instrument token a panel reads
/// resolves to a live instrument. It resolved the doubled form because its
/// forward-mapping generator registered that form for every instrument
/// unconditionally - a generator permissive enough to vouch for a series nothing
/// emits. That generator is now narrowed, so the sibling gate reddens on this
/// defect too; this gate states the invariant directly and carries the detector
/// controls, so the sibling's silence is not the only evidence.
/// </para>
/// </remarks>
[TestFixture]
public sealed class DashboardByteSeriesNameTests
{
    /// <summary>
    /// Floor on the byte instruments the source scan must inventory, so a scan that
    /// silently stops finding declarations fails instead of passing vacuously.
    /// </summary>
    private const int MinimumByteInstruments = 30;

    /// <summary>
    /// Floor on the byte instruments a bundled panel must actually reference in a
    /// form this gate judged emittable. This is the known-positive control for a
    /// verdict that is an absence: a detector that resolved no byte instruments at
    /// all would report no violations, exactly as a clean repository does.
    /// </summary>
    private const int MinimumReferencedByteInstruments = 22;

    /// <summary>
    /// A byte instrument whose name already carries the suffix, used to build the
    /// synthetic arms. Asserted to exist and to still carry it.
    /// </summary>
    private const string ControlInstrument = "orleans.lattice.storage.wal.stored_bytes";

    /// <summary>Matches any Prometheus-shaped token that mentions bytes.</summary>
    private static readonly Regex ByteTokenRegex =
        new(@"\b[A-Za-z_][A-Za-z0-9_]*_bytes[A-Za-z0-9_]*\b", RegexOptions.Compiled);

    /// <summary>The suffixes an exporter may append after the unit segment.</summary>
    private static readonly string[] SeriesSuffixes =
        [string.Empty, "_total", "_count", "_sum", "_bucket"];

    private static readonly Lazy<IReadOnlyDictionary<string, string>> InventoryLazy =
        new(BuildInventory, isThreadSafe: true);

    private static readonly Lazy<IReadOnlyList<ScannedReference>> ReferencesLazy =
        new(CollectReferences, isThreadSafe: true);

    /// <summary>
    /// Every declared instrument this gate treats as a byte instrument, mapped to
    /// its declared unit.
    /// </summary>
    /// <remarks>
    /// Membership is the union of two criteria, not either one alone. An instrument
    /// declaring unit <c>By</c> is one because the exporter will append the suffix;
    /// an instrument already <i>named</i> <c>*_bytes</c> is one whatever unit it
    /// declares, because a panel author can write the doubled form by hand for a
    /// name that looks byte-shaped, and a unit-only inventory would not cover it.
    /// </remarks>
    private static IReadOnlyDictionary<string, string> Inventory => InventoryLazy.Value;

    [Test]
    public void Scan_inventories_every_byte_instrument_and_reads_every_declared_unit()
    {
        Assert.That(
            DeclaredInstruments.UnitUnresolved,
            Is.Empty,
            "A declaration whose argument list could not be read has an unknown unit, not an absent "
            + "one, so it cannot be excluded from this inventory on the strength of its unit.");

        Assert.That(
            Inventory,
            Has.Count.GreaterThanOrEqualTo(MinimumByteInstruments),
            "The source scan inventoried fewer byte instruments than the repository is known to "
            + "declare, so an empty violation list would say more about the scan than the panels.");

        Assert.That(
            Inventory,
            Does.ContainKey(ControlInstrument),
            $"The control instrument '{ControlInstrument}' is no longer inventoried, so the "
            + "synthetic arms of this fixture no longer prove anything.");

        Assert.That(
            ControlInstrument.Replace('.', '_'),
            Does.EndWith("_bytes"),
            $"The control instrument '{ControlInstrument}' no longer carries the '_bytes' suffix in "
            + "its own name, so the doubled form built from it is no longer the defect shape.");
    }

    /// <summary>
    /// The gate. No panel may read a byte series under a name the exporter cannot
    /// produce.
    /// </summary>
    [Test]
    public void No_bundled_panel_reads_a_byte_series_the_exporter_cannot_emit()
    {
        var assessment = Assess(ReferencesLazy.Value);

        // Known-positive control, inside the test whose verdict is an absence.
        Assert.That(
            assessment.ReferencedInstruments,
            Has.Count.GreaterThanOrEqualTo(MinimumReferencedByteInstruments),
            "The detector matched almost no byte instrument to any panel, so its empty violation "
            + "list is not evidence of clean dashboards - it is evidence that the detector is not "
            + "resolving tokens at all.");

        Assert.That(
            assessment.Violations,
            Is.Empty,
            "A bundled panel reads a byte series under a name form no exporter emits. The series "
            + "does not exist, so the panel renders permanently blank - which an operator cannot "
            + "distinguish from a metric that is genuinely zero.\n"
            + string.Join("\n", assessment.Violations));
    }

    /// <summary>
    /// Known-positive control: a synthetic panel reading the doubled form is
    /// flagged. A detector that has not been shown to fire is not evidence.
    /// </summary>
    [Test]
    public void Detector_flags_a_synthetic_panel_reading_a_doubled_bytes_series()
    {
        var doubled = ControlInstrument.Replace('.', '_') + "_bytes_total";
        var assessment = Assess([new ScannedReference(doubled, "SyntheticControl")]);

        Assert.That(
            assessment.Violations,
            Has.Count.EqualTo(1),
            "The detector did not flag a panel reading the doubled byte form, so its silence on the "
            + "real dashboards means nothing.");

        Assert.That(assessment.Violations[0], Does.Contain(doubled));
    }

    /// <summary>
    /// Adversarial arm: the same detector, the same instrument, the measured form.
    /// Without this, a detector that flagged every byte token would pass the control
    /// above while proving nothing.
    /// </summary>
    [Test]
    public void Detector_accepts_a_synthetic_panel_reading_the_measured_form()
    {
        var measured = ControlInstrument.Replace('.', '_') + "_total";
        var assessment = Assess([new ScannedReference(measured, "SyntheticControl")]);

        Assert.That(
            assessment.Violations,
            Is.Empty,
            "The detector flagged the form the pinned exporter was measured to emit, so it is "
            + "returning the same verdict regardless of input.");

        Assert.That(
            assessment.ReferencedInstruments,
            Has.Count.EqualTo(1),
            "The correctly named series was neither flagged nor counted as resolved, so it was "
            + "silently dropped rather than judged.");
    }

    /// <summary>
    /// Every byte token a dashboard reads is accounted for in exactly one outcome,
    /// so a token the gate declines to judge cannot hide among tokens it never saw.
    /// </summary>
    [Test]
    public void Every_scanned_byte_token_is_accounted_for_in_exactly_one_outcome()
    {
        var refs = ReferencesLazy.Value;
        var assessment = Assess(refs);

        var distinct = refs.Select(r => r.Token).Distinct(StringComparer.Ordinal).Count();

        Assert.That(
            assessment.Emittable.Count + assessment.Violations.Count + assessment.Unattributed.Count,
            Is.EqualTo(distinct),
            "The outcome categories do not sum to the distinct byte tokens scanned, so at least one "
            + "token is being dropped between the scan and the verdict.\nunattributed: "
            + string.Join(", ", assessment.Unattributed));
    }

    /// <summary>
    /// The series names the measured exporter rule produces for one instrument.
    /// </summary>
    private static IReadOnlyList<string> EmittableForms(string dotted)
    {
        var underscored = dotted.Replace('.', '_');

        // The measured rule: append the unit segment only when the name does not
        // already carry it. Applying it unconditionally is precisely the defect.
        var based = underscored.EndsWith("_bytes", StringComparison.Ordinal)
            ? underscored
            : underscored + "_bytes";

        return [.. SeriesSuffixes.Select(s => based + s).Distinct(StringComparer.Ordinal)];
    }

    /// <summary>
    /// The byte-shaped series names no exporter emits for one instrument: those
    /// produced by appending the unit segment a second time.
    /// </summary>
    private static IReadOnlyList<string> UnemittableForms(string dotted)
    {
        var underscored = dotted.Replace('.', '_');

        if (!underscored.EndsWith("_bytes", StringComparison.Ordinal))
        {
            // The suffix is appended once and the doubled form is not reachable by
            // any rule, measured or otherwise, so there is nothing to forbid.
            return [];
        }

        return [.. SeriesSuffixes.Select(s => underscored + "_bytes" + s).Distinct(StringComparer.Ordinal)];
    }

    private static Assessment Assess(IReadOnlyList<ScannedReference> references)
    {
        var violations = new List<string>();
        var emittable = new List<string>();
        var unattributed = new List<string>();
        var referenced = new HashSet<string>(StringComparer.Ordinal);

        var forbidden = new Dictionary<string, string>(StringComparer.Ordinal);
        var allowed = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (var dotted in Inventory.Keys)
        {
            foreach (var form in UnemittableForms(dotted))
            {
                forbidden[form] = dotted;
            }

            foreach (var form in EmittableForms(dotted))
            {
                // A longer instrument name can generate a form that is also a
                // shorter one's form; first writer wins, which is enough to
                // attribute the token to a real instrument.
                allowed.TryAdd(form, dotted);
            }
        }

        foreach (var group in references
            .GroupBy(r => r.Token, StringComparer.Ordinal)
            .OrderBy(g => g.Key, StringComparer.Ordinal))
        {
            var token = group.Key;
            var where = string.Join(", ", group.Select(r => r.Dashboard).Distinct(StringComparer.Ordinal));

            if (forbidden.TryGetValue(token, out var owner))
            {
                violations.Add(
                    $"{token} (read by {where}) doubles the unit segment on '{owner}', whose name "
                    + "already ends in '_bytes'. The pinned exporter appends the segment only when "
                    + "it is absent, so nothing emits this series.");
                continue;
            }

            if (allowed.TryGetValue(token, out var resolved))
            {
                emittable.Add(token);
                referenced.Add(resolved);
                continue;
            }

            // Not attributable to any inventoried byte instrument. The sibling name
            // gate owns that verdict; restating it here in this gate's vocabulary
            // would give a confident wrong explanation for an unrelated defect.
            unattributed.Add(token);
        }

        return new Assessment(violations, emittable, unattributed, referenced);
    }

    private static IReadOnlyDictionary<string, string> BuildInventory()
    {
        var inventory = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (var (dotted, unit) in DeclaredInstruments.UnitByDottedName)
        {
            if (string.Equals(unit, "By", StringComparison.Ordinal)
                || dotted.Replace('.', '_').EndsWith("_bytes", StringComparison.Ordinal))
            {
                inventory[dotted] = unit;
            }
        }

        return inventory;
    }

    private static IReadOnlyList<ScannedReference> CollectReferences()
    {
        var references = new List<ScannedReference>();

        foreach (var kind in LatticeDashboards.All)
        {
            var json = LatticeDashboards.GetGrafanaDashboardJson(kind);

            foreach (Match match in ByteTokenRegex.Matches(json))
            {
                references.Add(new ScannedReference(match.Value, kind.ToString()));
            }
        }

        return references;
    }

    private readonly record struct ScannedReference(string Token, string Dashboard);

    private sealed record Assessment(
        IReadOnlyList<string> Violations,
        IReadOnlyList<string> Emittable,
        IReadOnlyList<string> Unattributed,
        IReadOnlySet<string> ReferencedInstruments);
}
