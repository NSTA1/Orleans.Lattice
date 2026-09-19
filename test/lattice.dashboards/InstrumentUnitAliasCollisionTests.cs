namespace Orleans.Lattice.Dashboards.Tests;

/// <summary>
/// Asserts that no declared instrument embeds its own unit's <i>raw alias</i> at
/// the end of its name, where the Prometheus exporter will append the unit's
/// mapped form after it and export a doubled suffix.
/// </summary>
/// <remarks>
/// <para>
/// The defect this gate exists to catch (issue #2920) is a histogram declared as
/// <c>orleans.lattice.replication.apply.dependency_wait_ms</c> with unit
/// <c>ms</c>, which exported as
/// <c>orleans_lattice_replication_apply_dependency_wait_ms_milliseconds_bucket</c>.
/// Any query naming the undoubled series matches nothing, and a panel that
/// matches nothing renders an empty chart - indistinguishable from a healthy
/// zero.
/// </para>
/// <para>
/// <b>The measured rule.</b> OpenTelemetry.Exporter.Prometheus.AspNetCore
/// 1.15.3-beta.1 - the version every host in this repository pins - appends the
/// suffix that the declared unit <i>maps</i> to, and suppresses that append only
/// when the name already ends in the <b>mapped</b> form. It does not compare
/// against the raw alias. This was established by scraping a live endpoint with
/// matched subject/control pairs: <c>probe.apply.dependency_wait_ms</c> (unit
/// <c>ms</c>) exported <c>..._ms_milliseconds_bucket</c> while its control
/// <c>probe.apply.dependency_wait</c> exported <c>..._milliseconds_bucket</c>,
/// and <c>probe.apply.parked_milliseconds</c> exported with the suffix
/// suppressed. The name this repair adopts was measured too rather than
/// predicted: <c>probe.apply.dependency_wait_fixed</c> (unit <c>ms</c>) exported
/// as <c>probe_apply_dependency_wait_fixed_milliseconds_bucket</c>. The same run
/// reproduced the independently established <c>By</c> result of issue #2941, so
/// a correct verdict here is evidence the harness was faithful rather than
/// merely self-consistent.
/// </para>
/// <para>
/// <b>Why this is inventoried rather than counted.</b> Grepping for the one name
/// the issue named returns a single occurrence and is blind to every sibling
/// site: an occurrence count reports a second instance as absent. This gate
/// instead enumerates <b>every</b> declared instrument under <c>src/</c>, derives
/// each one's exported suffix from its own declared unit, and tests the name
/// against that derived value, so a site is covered by construction rather than
/// by having been noticed.
/// </para>
/// <para>
/// <b>The rule is strictly one-directional.</b> It asserts that a name must not
/// end in its unit's raw alias. It deliberately does <i>not</i> assert the
/// converse - that a name must end in the mapped form - because the repository's
/// dominant convention is the opposite: 62 of the 63 <c>ms</c> instruments carry
/// no unit token at all and let the exporter supply one. A bidirectional rule
/// would redden on all 62 and would itself be a new instance of the arity defect
/// it was written to catch.
/// </para>
/// <para>
/// <b>Relationship to the sibling gates.</b> <c>DashboardBucketUnitSuffixTests</c>
/// checks that a panel's token agrees with its instrument's declared unit; it is
/// green both before and after this defect is fixed, because the panel and the
/// instrument agreed on the doubled form. <c>DashboardByteSeriesNameTests</c>
/// checks panel references for the <c>By</c> family. Both are
/// <i>panel-reference</i> gates and neither can see a redundant unit token in the
/// declaration itself, which is the position this gate occupies.
/// </para>
/// <para>
/// <b>Deliberately out of scope.</b> A name ending in some <i>other</i> unit's
/// token than the one it declares (say <c>_s</c> under unit <c>ms</c>) is a
/// related but distinct defect with a different remedy. No instrument in the
/// repository currently exhibits it; folding it in here would widen this gate
/// past the defect it was written for.
/// </para>
/// </remarks>
[TestFixture]
public sealed class InstrumentUnitAliasCollisionTests
{
    /// <summary>
    /// Floor on the total declarations the source scan must inventory, so a scan
    /// that silently stops finding declarations fails instead of reporting a
    /// clean repository. A verdict of "no violations" over an empty population is
    /// the failure mode this gate is most exposed to, because it is
    /// indistinguishable from success.
    /// </summary>
    /// <remarks>
    /// Set from the measured population (372 at the time of writing), not
    /// estimated. A floor guessed comfortably low tolerates exactly the silent
    /// loss it was placed to catch: the first draft of this gate used 250, which
    /// would have reported a clean repository after a third of the declarations
    /// stopped being scanned.
    /// </remarks>
    private const int MinimumDeclarations = 300;

    /// <summary>
    /// Floor on the declarations that are actually <i>in scope</i> - those whose
    /// declared unit maps to a suffix the exporter appends. The total floor above
    /// does not imply this one: a parser that read every name but no unit would
    /// clear the total and leave this at zero, reporting a clean result over a
    /// population it never assembled.
    /// </summary>
    /// <remarks>
    /// Also set from the measured population (108). An occurrence-style regex
    /// scan written while investigating issue #2920 found only 85, because it
    /// could not see a unit passed positionally; the shared parser this gate
    /// reuses resolves those. The gap is the reason this floor is derived from
    /// the parser's own count rather than from a hand scan.
    /// </remarks>
    private const int MinimumSuffixBearingDeclarations = 90;

    /// <summary>
    /// Units whose declared alias differs from the segment the exporter appends.
    /// A unit absent from this map, from <see cref="UnitsAppendingNothing"/>, and
    /// not a UCUM annotation is <b>unclassified</b> and fails the gate rather than
    /// being skipped, so a newly introduced unit cannot quietly fall outside the
    /// population.
    /// </summary>
    /// <remarks>
    /// Every entry was measured against the pinned exporter with a matched
    /// control - an instrument declaring the same unit under a name that does
    /// <i>not</i> end in the mapped form. The control is what discriminates
    /// "appends the mapped segment" from "appends nothing", which the subject
    /// alone cannot: a name already ending in <c>_percent</c> exports as
    /// <c>_percent</c> either way. The control for <c>%</c> emitted no
    /// <c>probe_quota_burst_bucket</c>, which is how <c>percent</c> is known to
    /// be appended rather than assumed.
    /// </remarks>
    private static readonly IReadOnlyDictionary<string, string> MappedSuffixByUnit =
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["ms"] = "milliseconds",
            ["s"] = "seconds",
            ["By"] = "bytes",
            ["%"] = "percent",
        };

    /// <summary>
    /// Non-annotation units the pinned exporter appends <b>nothing</b> for, so no
    /// name can collide with them. Classified explicitly rather than omitted,
    /// because "known to append nothing" and "not yet looked at" must not be the
    /// same state.
    /// </summary>
    /// <remarks>
    /// <c>1</c> is here on measurement, not on inference: the control
    /// <c>probe.leaf.tombstone</c> declaring unit <c>1</c> exported as
    /// <c>probe_leaf_tombstone_bucket</c>, not <c>probe_leaf_tombstone_ratio_bucket</c>.
    /// An earlier draft of this gate mapped <c>1</c> to <c>ratio</c> by analogy
    /// with the other units and was wrong; the verdict happened to be unchanged,
    /// which is precisely why an unmeasured mapping can sit in a gate unnoticed.
    /// </remarks>
    private static readonly IReadOnlySet<string> UnitsAppendingNothing =
        new HashSet<string>(StringComparer.Ordinal) { "1" };

    /// <summary>
    /// The instrument whose collision issue #2920 reported, retained as the
    /// regression anchor. Asserted to still be declared and to still carry unit
    /// <c>ms</c>, so the fix cannot be silently reverted by a rename.
    /// </summary>
    private const string RepairedInstrument = "orleans.lattice.replication.apply.dependency_wait";

    [Test]
    public void Scan_inventories_the_declarations_it_judges()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                DeclaredInstruments.Unresolved,
                Is.Empty,
                "A declaration whose name could not be resolved to a literal is an instrument this "
                + "gate silently stops covering.");

            Assert.That(
                DeclaredInstruments.UnitUnresolved,
                Is.Empty,
                "A declaration whose argument list could not be read has an unknown unit, not an "
                + "absent one, so it cannot be excluded from this population on the strength of it.");

            Assert.That(
                DeclaredInstruments.UnitByDottedName,
                Has.Count.GreaterThanOrEqualTo(MinimumDeclarations),
                $"The source scan inventoried {DeclaredInstruments.UnitByDottedName.Count} declarations, "
                + $"fewer than the {MinimumDeclarations} the repository is known to declare, so an empty "
                + "violation list would say more about the scan than about the instruments.");

            Assert.That(
                SuffixBearing().Count,
                Is.GreaterThanOrEqualTo(MinimumSuffixBearingDeclarations),
                $"The scan resolved names but only {SuffixBearing().Count} units that map to an "
                + $"appended suffix, fewer than the {MinimumSuffixBearingDeclarations} expected, so "
                + "the population this gate judges was never actually assembled.");
        });
    }

    [Test]
    public void Every_declared_unit_is_classified()
    {
        var unclassified = DeclaredInstruments.UnitByDottedName
            .Where(p => !IsClassified(p.Value))
            .Select(p => $"{p.Key} declares unit '{p.Value}'")
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();

        Assert.That(
            unclassified,
            Is.Empty,
            "A unit this gate cannot classify is one it cannot judge. Add it to MappedSuffixByUnit "
            + "with the segment the exporter appends, rather than letting it fall outside the "
            + $"population:{Environment.NewLine}  "
            + string.Join($"{Environment.NewLine}  ", unclassified.Take(40)));
    }

    [Test]
    public void No_instrument_name_ends_in_its_own_units_raw_alias()
    {
        var violations = Collide(DeclaredInstruments.UnitByDottedName);

        Assert.That(
            violations,
            Is.Empty,
            $"Of {SuffixBearing().Count} declarations whose unit maps to an exported suffix, the "
            + $"following end in the raw alias instead, so the exporter appends the mapped form after "
            + $"it and the undoubled series is never emitted:{Environment.NewLine}  "
            + string.Join($"{Environment.NewLine}  ", violations));
    }

    [Test]
    public void The_repaired_instrument_is_still_declared_under_its_undoubled_name()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                DeclaredInstruments.UnitByDottedName,
                Does.ContainKey(RepairedInstrument),
                $"'{RepairedInstrument}' is no longer declared, so the issue #2920 regression anchor "
                + "no longer anchors anything.");

            Assert.That(
                DeclaredInstruments.UnitByDottedName.GetValueOrDefault(RepairedInstrument),
                Is.EqualTo("ms"),
                $"'{RepairedInstrument}' no longer declares unit 'ms', so it no longer exercises the "
                + "alias-collision shape this gate guards.");
        });
    }

    /// <summary>
    /// The positive control. A detector that never fires would pass the gate above
    /// over any repository at all, so it is shown to fire on a constructed
    /// violation.
    /// </summary>
    [Test]
    public void Detector_flags_a_synthetic_name_that_ends_in_its_raw_alias()
    {
        var synthetic = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["synthetic.apply.dependency_wait_ms"] = "ms",
        };

        Assert.That(
            Collide(synthetic),
            Has.Count.EqualTo(1),
            "The detector did not flag a name ending in its own unit's raw alias, so its silence on "
            + "the repository is not evidence of anything.");
    }

    /// <summary>
    /// The negative control. A detector that flagged everything would also pass the
    /// positive control above while proving nothing, so it is shown <i>not</i> to
    /// fire on the three shapes that are correct.
    /// </summary>
    [Test]
    public void Detector_accepts_the_shapes_that_export_cleanly()
    {
        var clean = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            // The dominant convention: no unit token, exporter supplies one.
            ["synthetic.apply.duration"] = "ms",

            // Already the mapped form, so the exporter suppresses its append.
            ["synthetic.apply.parked_milliseconds"] = "ms",

            // A UCUM annotation unit, for which nothing is appended.
            ["synthetic.apply.entries"] = "{entry}",

            // Unit '1', measured to append nothing, so no name can collide.
            ["synthetic.leaf.tombstone_ratio"] = "1",

            // No declared unit at all.
            ["synthetic.apply.runs"] = string.Empty,
        };

        Assert.That(
            Collide(clean),
            Is.Empty,
            "The detector flagged a correctly shaped declaration, so it is reporting its own breadth "
            + "rather than the defect.");
    }

    /// <summary>
    /// The vacuity control. The gate must not be able to report success over an
    /// empty population, which is the way a derived gate most commonly rots.
    /// </summary>
    [Test]
    public void Gate_refuses_an_empty_population()
    {
        var empty = new Dictionary<string, string>(StringComparer.Ordinal);

        Assert.Multiple(() =>
        {
            Assert.That(
                Collide(empty),
                Is.Empty,
                "An empty population yields no violations, which is exactly why the count floors "
                + "below - not this verdict - are what make the gate non-vacuous.");

            Assert.That(
                empty.Count,
                Is.LessThan(MinimumDeclarations),
                "The floor asserted by Scan_inventories_the_declarations_it_judges must exceed an "
                + "empty population, or that assertion could not reject one.");

            Assert.That(
                MinimumSuffixBearingDeclarations,
                Is.GreaterThan(0),
                "A zero floor on the in-scope population would admit a scan that read no units.");
        });
    }

    /// <summary>
    /// Applies the one-directional rule to a population, returning one message per
    /// colliding declaration.
    /// </summary>
    private static List<string> Collide(IReadOnlyDictionary<string, string> population)
    {
        var violations = new List<string>();

        foreach (var (dotted, unit) in population.OrderBy(p => p.Key, StringComparer.Ordinal))
        {
            if (!MappedSuffixByUnit.TryGetValue(unit, out var mapped))
            {
                continue;
            }

            var underscored = dotted.Replace('.', '_');

            // Already the mapped form: the exporter suppresses its append.
            if (underscored.EndsWith("_" + mapped, StringComparison.Ordinal))
            {
                continue;
            }

            if (underscored.EndsWith("_" + unit, StringComparison.Ordinal))
            {
                violations.Add(
                    $"{dotted} (unit '{unit}') exports as '{underscored}_{mapped}', because the "
                    + $"exporter suppresses its append only for the mapped form '_{mapped}', never "
                    + $"for the alias '_{unit}'. Drop the '_{unit}' from the name.");
            }
        }

        return violations;
    }

    private static bool IsClassified(string unit) =>
        unit.Length == 0
        || unit.StartsWith('{')
        || UnitsAppendingNothing.Contains(unit)
        || MappedSuffixByUnit.ContainsKey(unit);

    private static List<KeyValuePair<string, string>> SuffixBearing() =>
        DeclaredInstruments.UnitByDottedName
            .Where(p => MappedSuffixByUnit.ContainsKey(p.Value))
            .ToList();
}
