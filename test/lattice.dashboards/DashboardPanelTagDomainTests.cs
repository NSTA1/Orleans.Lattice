using System.Diagnostics.Metrics;
using System.Reflection;
using System.Text.Json;
using System.Text.RegularExpressions;
using Orleans.Lattice;
using Orleans.Lattice.Dashboards;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Dashboards.Tests;

/// <summary>
/// Asserts that every literal tag value a bundled Grafana panel names in a
/// label matcher is a value the instrument behind that matcher can actually
/// emit, and that every value it <i>can</i> emit is either covered by a panel
/// or declared here as a deliberate omission.
/// </summary>
/// <remarks>
/// <para>
/// Issue #2854 is the failure this guards. A panel (and its documentation)
/// named the hydration admission outcome <c>admitted_immediately</c>, read off
/// the C# field name <c>SnapshotHydrationAdmittedImmediately</c> rather than off
/// that field's <i>value</i>, which has been <c>immediate</c> since the gate
/// landed. Nothing rendered visibly wrong, because a matcher naming a value no
/// series carries simply selects nothing. The harm was that the same panel
/// description told the reader, in capitals, that a missing line means the build
/// did not land - so the artefacts agreed on a confident wrong verdict.
/// </para>
/// <para>
/// Issue #2520 is the amplifier. <c>or vector(0)</c> renders an absent series as
/// a healthy zero, so a matcher that can never match composes into a panel that
/// reports measured health for something that was never measured.
/// </para>
/// <para>
/// The emittable set is derived from the emission sites in <c>src/</c> - the
/// <c>.Add(</c> / <c>.Record(</c> calls on the instrument field - and never from
/// a second hand-maintained list, because a hand-maintained list is edited by
/// the same person who would have fixed the panel, so it drifts in lockstep and
/// proves nothing. Where the derivation cannot reach a value the fixture fails
/// loudly rather than quietly narrowing its own scope: an unresolved expression
/// is reported with its file and line, and the scan is asserted non-empty in the
/// manner of <c>MeterFieldDeclarationOrderTests</c>, so this guard cannot go
/// vacuous and report green.
/// </para>
/// <para>
/// Scope is declared, not assumed. <see cref="OpenDomainTags"/> names the tags
/// whose value space is unbounded at runtime (a tree id, a tenant, a peer) and
/// therefore has no closed set to check.
/// <see cref="InstrumentsWithoutImperativeEmission"/> names the instruments
/// whose tag values are produced inside an observable-instrument callback, which
/// this resolver deliberately does not follow; each entry is re-checked, so an
/// instrument that later grows an imperative emission site fails the guard
/// rather than staying silently excluded.
/// </para>
/// </remarks>
[TestFixture]
public sealed class DashboardPanelTagDomainTests
{
    /// <summary>
    /// Tags whose value space is open at runtime, so no closed emittable set
    /// exists to check a panel matcher against. Every one of these is either a
    /// caller-supplied identity or a dashboard-variable-driven selector. A
    /// matcher on one of them is skipped by name, deliberately and visibly,
    /// rather than by failing to parse.
    /// </summary>
    private static readonly IReadOnlySet<string> OpenDomainTags =
        new HashSet<string>(StringComparer.Ordinal)
        {
            "cluster",      // deployment identity, supplied by the collector
            "instance",     // silo address, supplied by the collector
            "job",          // scrape job, supplied by the collector
            "tree",         // caller-chosen tree name
            "tenant",       // caller-chosen tenant id
            "peer",         // remote region id, configured per deployment
            "region",       // remote region id, configured per deployment
            "index",        // caller-chosen index name
            "view",         // caller-chosen view name
            "scope",        // caller-chosen scope name
            "tree_count",   // participant cardinality, an unbounded integer
            "le",           // Prometheus histogram bucket boundary
            "quantile",     // Prometheus summary quantile
        };

    /// <summary>
    /// Instruments whose tag values are produced inside an observable-instrument
    /// measurement callback rather than at an imperative <c>.Add(</c> /
    /// <c>.Record(</c> site. The resolver does not follow callback delegates, so
    /// these are out of scope - stated here rather than left to look like
    /// coverage. <see cref="Declared_observable_exclusions_still_have_no_imperative_emission_site"/>
    /// re-checks every entry, so one that later grows an imperative site fails
    /// this guard instead of remaining silently excluded.
    /// </summary>
    private static readonly IReadOnlyDictionary<string, string> InstrumentsWithoutImperativeEmission =
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["orleans.lattice.replication.peer.last_contact_seconds"] =
                "Observable gauge. The direction tag is stamped inside "
                + "ReplicationPeerStats.ObserveLastContactSeconds, reached only through the "
                + "gauge's measurement callback; the resolver does not follow callback "
                + "delegates. Its domain is the two const strings DirectionInbound / "
                + "DirectionOutbound on LatticeReplicationMetrics.",
        };

    /// <summary>
    /// Values an instrument can emit that a panel deliberately does not chart,
    /// keyed by <c>dashboard|panel|instrument|tag</c>. The declared set must
    /// match the computed omission exactly, so adding a new arm to an
    /// instrument reddens this guard and forces a decision about whether the
    /// panel should chart it - which is precisely the miss in issue #2768,
    /// where a new <c>undelivered</c> arm was left uncovered by a panel whose
    /// allow-list nobody revisited.
    /// </summary>
    private static readonly IReadOnlyDictionary<string, string[]> DeliberateOmissions =
        new Dictionary<string, string[]>(StringComparer.Ordinal)
        {
            // The annotation and the non-committed panels below select the
            // complement of "committed" with outcome!="committed", so their only
            // omission is the committed arm itself.
            ["AtomicWrites|annotation:Atomic write outcomes|orleans.lattice.atomic_write.completed|outcome"] =
                ["committed"],
            ["AtomicWrites|2|orleans.lattice.atomic_write.completed|outcome"] =
                ["committed"],
            ["AtomicWrites|8|orleans.lattice.atomic_write.completed|outcome"] =
                ["committed"],
            ["Overview|18|orleans.lattice.atomic_write.completed|outcome"] =
                ["committed"],

            // Panel 10 charts the cross-tree coordinator's only failure arm
            // (precondition_failed) and the complement of committed beside it.
            ["AtomicWrites|10|orleans.lattice.atomic_write.cross_tree.completed|outcome"] =
                ["committed"],

            // Panel 7 charts committed throughput only; the failure arms are
            // charted by panel 2 on the same dashboard.
            ["AtomicWrites|7|orleans.lattice.atomic_write.completed|outcome"] =
                ["compensated", "failed", "shutdown_refused"],

            // Panel 135 charts reclaiming passes only; the blocked arm is
            // charted on the replication dashboard (panel 70) and the remaining
            // arms are diagnostic rather than operational.
            ["CommitPath|135|orleans.lattice.wal.gc.passes|outcome"] =
                ["blocked", "failed", "idle", "no_consumer", "unclassified"],

            // Panel 70 is the blocked-pass panel, the mirror of panel 135.
            ["Replication|70|orleans.lattice.wal.gc.passes|outcome"] =
                ["failed", "idle", "no_consumer", "reclaimed", "unclassified"],

            // Panel 143 charts both arms on its unfiltered target, and adds a
            // second target narrowed to outcome="withheld" so the withheld arm
            // can be split by trigger (issue #2883). Only the narrowed target
            // carries a matcher, so the restored arm is omitted from the
            // matcher-bearing set while still being drawn on the graph. It is
            // deliberately not split by trigger: withheld permits are fungible,
            // so the restored arm carries no trigger tag and a per-trigger
            // level is not derivable.
            ["CommitPath|143|orleans.lattice.wal.replay.permit_adaptations|outcome"] =
                ["restored"],

            // Panel 2783 charts the terminal reactivation arms. "attempted" is
            // the denominator arm (one per touch issued, not a terminal state)
            // and is charted by its own target on the same panel without a
            // matcher, so it is not part of this allow-list. The three arms
            // added by issue #2938 - completed, faulted and unresolvable - are
            // charted on this matcher rather than declared omitted: they are
            // terminal outcomes like the four already there, and completed in
            // particular has to be legible beside healed, because the gap
            // between them is what separates a sweep that reaches the leaf
            // from one that clears the pin.
            //
            // The five drive arms (issue #2692) record what a touch achieved
            // rather than that it was issued, and are charted with their full
            // interpretation on panel 2692 below.
            ["Replication|2783|orleans.lattice.wal.gc.blocked_leaf_reactivations|outcome"] =
                [
                    "attempted", "drove_already_driving", "drove_lifted",
                    "drove_memory_refused", "drove_no_advance", "drove_not_driven",
                ],

            // Panel 2692 is the converse of 2783: it charts only the five drive
            // verdicts (issue #2692), so the reachability arms it omits are the
            // ones 2783 exists to chart. The two panels partition the domain
            // between them, and the rate panel 71 carries no outcome matcher at
            // all, so every arm remains charted somewhere.
            //
            // This list grew by three (completed, faulted, unresolvable) when
            // issue #2938 armed them. That growth is the whole hazard of a
            // declared-omission list: it is a statement about the complement of
            // a domain, so widening the domain silently makes every existing
            // list short without touching a line of it. Nothing here changed,
            // and this entry was wrong the moment #2938 merged.
            ["Replication|2692|orleans.lattice.wal.gc.blocked_leaf_reactivations|outcome"] =
                [
                    "abandoned", "attempted", "completed", "faulted", "healed",
                    "rearmed", "undelivered", "unresolvable",
                ],

            // Panel 34 alerts on entry into saturation only. The healthy,
            // throttled, and unknown transitions are charted by the
            // unfiltered saturation panels on the commit-path dashboard.
            ["Overview|34|orleans.lattice.wal.saturation.transitions|state"] =
                ["healthy", "throttled", "unknown"],
        };

    private static readonly Regex SelectorRegex =
        new(@"(?<token>orleans_lattice[a-z0-9_]*)\{(?<body>[^}]*)\}", RegexOptions.Compiled);

    private static readonly Regex MatcherRegex =
        new("(?<tag>[a-z_][a-z0-9_]*)\\s*(?<op>=~|!~|!=|=)\\s*\"(?<value>[^\"]*)\"", RegexOptions.Compiled);

    private static readonly Regex LiteralAlternationRegex =
        new(@"^[a-z0-9_]+(\|[a-z0-9_]+)*$", RegexOptions.Compiled);

    /// <summary>One literal label matcher found on one dashboard target.</summary>
    private sealed record FilterSite(
        string Dashboard,
        string Site,
        string InstrumentToken,
        string Tag,
        bool Negated,
        IReadOnlyList<string> Values)
    {
        public string Key(string dottedInstrument) => $"{Dashboard}|{Site}|{dottedInstrument}|{Tag}";
    }

    /// <summary>The derived emittable set for one instrument/tag pair.</summary>
    private sealed record TagDomain(IReadOnlySet<string> Values, IReadOnlyList<string> Problems);

    // ---------------------------------------------------------------- tests

    /// <summary>
    /// Fails when the panel scan finds nothing to check. A repository-wide gate
    /// that silently matches nothing is worse than no gate, because it reports
    /// green; this is the same anti-vacuity arm
    /// <c>MeterFieldDeclarationOrderTests</c> carries.
    /// </summary>
    [Test]
    public void Panel_scan_finds_literal_tag_matchers_to_check()
    {
        var sites = ScanFilterSites();

        Assert.Multiple(() =>
        {
            Assert.That(sites, Is.Not.Empty,
                "The dashboard scan found no literal tag matcher at all. Either every panel now "
                + "selects with dashboard variables only, or SelectorRegex/MatcherRegex have "
                + "drifted from the dashboard JSON. Until this finds sites again the guard is "
                + "silently vacuous and proves nothing.");

            Assert.That(sites.Select(s => s.Dashboard).Distinct().Count(), Is.GreaterThanOrEqualTo(3),
                "Literal tag matchers were found on fewer than three dashboards. The bundled set "
                + "carries them on the atomic-write, commit-path, overview, and replication "
                + "dashboards, so a narrower result means the scan is missing files.");

            Assert.That(sites.Select(s => s.InstrumentToken).Distinct().Count(), Is.GreaterThanOrEqualTo(5),
                "Fewer than five distinct instruments carry a literal tag matcher. The scan has "
                + "narrowed; check SelectorRegex against the dashboard JSON.");
        });
    }

    /// <summary>
    /// The load-bearing arm: every literal value a panel names must be a value
    /// the instrument can actually emit.
    /// </summary>
    [Test]
    public void Every_tag_value_a_panel_names_is_a_value_its_instrument_can_emit()
    {
        var (checkedSites, violations, problems) = Evaluate();

        Assert.That(problems, Is.Empty,
            "The emittable set for an instrument/tag pair a panel filters on could not be derived "
            + "from src/. The guard refuses to narrow its own scope silently: either extend the "
            + "resolver, or declare the instrument in InstrumentsWithoutImperativeEmission with a "
            + "reason.\n" + string.Join("\n", problems));

        Assert.That(checkedSites, Is.GreaterThan(0),
            "No instrument/tag pair was actually checked, so this arm passed vacuously.");

        Assert.That(violations, Is.Empty,
            "A dashboard panel filters on a tag value its instrument can never emit. The matcher "
            + "selects zero series, and with an `or vector(0)` compensation that renders as a "
            + "confident healthy zero (issue #2520) for something that was never measured. Read "
            + "the emitted literal off the emission site's value, not off the C# field name "
            + "(issue #2854).\n" + string.Join("\n", violations));
    }

    /// <summary>
    /// The converse arm: a value an instrument can emit must be charted by the
    /// panel that filters that instrument, or declared as a deliberate omission.
    /// </summary>
    [Test]
    public void Every_emittable_tag_value_is_charted_or_declared_as_omitted()
    {
        var sites = ScanFilterSites();
        var mismatches = new List<string>();
        var declaredKeys = new HashSet<string>(StringComparer.Ordinal);
        var compared = 0;

        foreach (var group in GroupSitesByPanelInstrumentTag(sites))
        {
            var domain = ResolveDomain(group.Token, group.Tag);
            if (domain is null || domain.Problems.Count > 0) continue;

            var dotted = TokenToDottedName(group.Token)!;
            var key = $"{group.Dashboard}|{group.Site}|{dotted}|{group.Tag}";

            var covered = group.Covered(domain.Values);

            var omitted = domain.Values.Except(covered, StringComparer.Ordinal)
                .OrderBy(v => v, StringComparer.Ordinal).ToArray();

            compared++;
            if (omitted.Length == 0) continue;

            declaredKeys.Add(key);
            if (!DeliberateOmissions.TryGetValue(key, out var declared))
            {
                mismatches.Add(
                    $"{key}: charts {string.Join(",", covered.OrderBy(v => v, StringComparer.Ordinal))} "
                    + $"but the instrument can also emit {string.Join(",", omitted)}, which no "
                    + "target on this panel selects and no entry declares.");
                continue;
            }

            var declaredSet = declared.OrderBy(v => v, StringComparer.Ordinal).ToArray();
            if (!declaredSet.SequenceEqual(omitted, StringComparer.Ordinal))
            {
                mismatches.Add(
                    $"{key}: declared omission [{string.Join(",", declaredSet)}] does not match the "
                    + $"actual omission [{string.Join(",", omitted)}].");
            }
        }

        var stale = DeliberateOmissions.Keys.Except(declaredKeys, StringComparer.Ordinal)
            .OrderBy(k => k, StringComparer.Ordinal).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(compared, Is.GreaterThan(0),
                "No panel/instrument/tag group was compared against an emittable set, so this arm "
                + "passed vacuously.");

            Assert.That(mismatches, Is.Empty,
                "A panel's tag allow-list no longer matches what its instrument can emit. An arm "
                + "added to the instrument and not to the panel is invisible on the dashboard - "
                + "the miss in issue #2768. Either widen the panel, or record the omission in "
                + "DeliberateOmissions with a comment saying why it is not charted.\n"
                + string.Join("\n", mismatches));

            Assert.That(stale, Is.Empty,
                "DeliberateOmissions carries an entry for a panel/instrument/tag group that no "
                + "longer omits anything (or no longer exists). A stale waiver silently widens "
                + "this guard's blind spot; delete it.\n" + string.Join("\n", stale));
        });
    }

    /// <summary>
    /// Known-positive control. A detector that has not been shown to fire is
    /// not evidence, so this drives the same comparison the repository arm uses
    /// against a synthetic panel naming a value the atomic-write counter cannot
    /// emit, and asserts it is reported - and that a genuinely emittable value
    /// beside it is not.
    /// </summary>
    [Test]
    public void Detector_flags_a_synthetic_panel_naming_an_unemittable_value()
    {
        var domain = ResolveDomain("orleans_lattice_atomic_write_completed_total", "outcome");
        Assert.That(domain, Is.Not.Null, "The control's instrument/tag pair no longer resolves.");
        Assert.That(domain!.Problems, Is.Empty, string.Join("\n", domain.Problems));

        var planted = new FilterSite(
            "SyntheticProbe", "9001", "orleans_lattice_atomic_write_completed_total", "outcome",
            Negated: false, Values: ["committed", "admitted_immediately"]);

        var flagged = planted.Values.Where(v => !domain.Values.Contains(v)).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(flagged, Is.EqualTo(new[] { "admitted_immediately" }),
                "The comparison did not flag a value the instrument cannot emit. This is the "
                + "issue #2854 shape - a literal read off a C# field name rather than its value - "
                + "and a detector that cannot flag it proves nothing about the repository arm.");

            Assert.That(domain.Values, Does.Contain("committed"),
                "A genuinely emittable value was not recognised, so the repository arm would "
                + "report false violations.");
        });
    }

    /// <summary>
    /// Resolver control. Pins the derived domain of one instrument per emission
    /// shape the resolver claims to handle, so a regression in the derivation
    /// surfaces here - as a wrong set - rather than downstream as an arm that
    /// silently stops being checked.
    /// </summary>
    [Test]
    public void Resolver_derives_the_known_domain_of_each_emission_shape()
    {
        Assert.Multiple(() =>
        {
            // Inline string literals assigned to a local, read at the .Add site.
            AssertDomain("orleans_lattice_atomic_write_completed_total", "outcome",
                ["committed", "compensated", "failed", "shutdown_refused"]);

            // Same shape, a two-arm ternary - and the instrument whose panel
            // filtered on two values it can never emit.
            AssertDomain("orleans_lattice_atomic_write_cross_tree_completed_total", "outcome",
                ["committed", "precondition_failed"]);

            // KeyValuePair constants passed straight to .Add.
            AssertDomain("orleans_lattice_wal_replay_permit_adaptations_total", "outcome",
                ["restored", "withheld"]);

            // One forwarding hop: callers pass a KeyValuePair constant into a
            // private helper that performs the .Add. Since issue #2938 the tag
            // is produced by a total mapping over the outcome enum rather than
            // by a hand-written list, so the resolver's ability to follow the
            // hop is what keeps a newly added arm in scope for the charting
            // gate below. Issue #2692 added a second such mapping over a second
            // enum, so the domain is now the union of three disjoint groups:
            // four lifecycle arms, four terminal arms, and five drive verdicts.
            // All thirteen are derivable; a drop here means the hop stopped
            // resolving for one of the two mappings.
            AssertDomain("orleans_lattice_wal_gc_blocked_leaf_reactivations_total", "outcome",
                [
                    "abandoned", "attempted", "completed", "drove_already_driving",
                    "drove_lifted", "drove_memory_refused", "drove_no_advance",
                    "drove_not_driven", "faulted", "healed", "rearmed",
                    "undelivered", "unresolvable",
                ]);

            // Collection-built tag list plus a static string-returning helper
            // resolved across files.
            AssertDomain("orleans_lattice_wal_saturation_transitions_total", "state",
                ["healthy", "saturated", "throttled", "unknown"]);
        });
    }

    /// <summary>
    /// Re-checks every declared observable exclusion, so an instrument that
    /// later grows an imperative emission site fails this guard rather than
    /// staying silently out of scope behind a stale entry.
    /// </summary>
    [Test]
    public void Declared_observable_exclusions_still_have_no_imperative_emission_site()
    {
        var fields = InstrumentFields();
        var known = TokenFormsLazy.Value;
        var wrong = new List<string>();

        foreach (var (dotted, reason) in InstrumentsWithoutImperativeEmission)
        {
            Assert.That(reason, Is.Not.Empty, $"{dotted} is excluded without a stated reason.");

            // An observable instrument is created when its owning subsystem
            // starts, so it is not a static field a reflection snapshot can see.
            // What must still exist is the name constant, otherwise the entry
            // names an instrument this repository no longer declares and the
            // exclusion is quietly protecting nothing.
            if (!known.ContainsKey(dotted.Replace('.', '_')))
            {
                wrong.Add(
                    $"{dotted}: declared excluded, but neither meter declares an instrument or a "
                    + "public const ...Name by that name. Delete the stale exclusion.");
                continue;
            }

            if (fields.TryGetValue(dotted, out var field) && FindEmissionSites(field.FieldName).Count > 0)
            {
                wrong.Add(
                    $"{dotted}: declared excluded as callback-only, but src/ now contains an "
                    + $"imperative {field.FieldName}.Add(/.Record( site. Remove the exclusion so "
                    + "its tag domain is checked.");
            }
        }

        Assert.That(wrong, Is.Empty, string.Join("\n", wrong));
    }

    private static void AssertDomain(string token, string tag, string[] expected)
    {
        var domain = ResolveDomain(token, tag);
        Assert.That(domain, Is.Not.Null, $"{token}{{{tag}}} no longer resolves to an instrument.");
        Assert.That(domain!.Problems, Is.Empty,
            $"{token}{{{tag}}} could not be resolved:\n" + string.Join("\n", domain.Problems));
        Assert.That(domain.Values.OrderBy(v => v, StringComparer.Ordinal).ToArray(),
            Is.EqualTo(expected),
            $"The derived emittable set for {token}{{{tag}}} changed. If an arm was added or "
            + "renamed in src/, update the panels and this control together.");
    }

    // ----------------------------------------------------------- evaluation

    private static (int Checked, List<string> Violations, List<string> Problems) Evaluate()
    {
        var violations = new List<string>();
        var problems = new List<string>();
        var checkedSites = 0;

        foreach (var site in ScanFilterSites())
        {
            var dotted = TokenToDottedName(site.InstrumentToken);
            if (dotted is null)
            {
                problems.Add(
                    $"{site.Dashboard}|{site.Site}: token '{site.InstrumentToken}' does not map to "
                    + "any instrument on either meter.");
                continue;
            }

            if (InstrumentsWithoutImperativeEmission.ContainsKey(dotted)) continue;

            var domain = ResolveDomain(site.InstrumentToken, site.Tag);
            if (domain is null)
            {
                problems.Add($"{site.Dashboard}|{site.Site}: no instrument field declares {dotted}.");
                continue;
            }

            if (domain.Problems.Count > 0)
            {
                problems.Add($"{dotted}{{{site.Tag}}} ({site.Dashboard}|{site.Site}):\n  "
                    + string.Join("\n  ", domain.Problems));
                continue;
            }

            checkedSites++;
            foreach (var value in site.Values.Where(v => !domain.Values.Contains(v)))
            {
                violations.Add(
                    $"{site.Dashboard}|{site.Site}: matcher {site.Tag}"
                    + (site.Negated ? "!=" : "=") + $"\"{value}\" on {dotted} names a value the "
                    + "instrument never emits. It can emit: "
                    + string.Join(", ", domain.Values.OrderBy(v => v, StringComparer.Ordinal)) + ".");
            }
        }

        return (checkedSites, violations, problems);
    }

    private sealed record SiteGroup(
        string Dashboard, string Site, string Token, string Tag, IReadOnlyList<FilterSite> Sites)
    {
        /// <summary>
        /// The set of emittable values this panel's matchers actually select.
        /// Each matcher contributes independently - a positive one covers the
        /// values it names, a negated one covers the complement - and the panel
        /// covers the union, because its targets are drawn on one graph. Mixing
        /// the two forms is therefore read exactly rather than skipped: a panel
        /// that charts <c>outcome="failed"</c> beside <c>outcome!="committed"</c>
        /// is the common shape once a breakdown series sits next to a total.
        /// </summary>
        public IReadOnlySet<string> Covered(IReadOnlySet<string> domain)
        {
            var covered = new HashSet<string>(StringComparer.Ordinal);

            foreach (var site in Sites)
            {
                if (site.Negated) covered.UnionWith(domain.Except(site.Values, StringComparer.Ordinal));
                else covered.UnionWith(site.Values.Where(domain.Contains));
            }

            return covered;
        }
    }

    private static List<SiteGroup> GroupSitesByPanelInstrumentTag(IEnumerable<FilterSite> sites)
        => sites
            .GroupBy(s => (s.Dashboard, s.Site, s.InstrumentToken, s.Tag))
            .Select(g => new SiteGroup(
                g.Key.Dashboard, g.Key.Site, g.Key.InstrumentToken, g.Key.Tag, g.ToArray()))
            .ToList();

    // -------------------------------------------------------- dashboard scan

    private static List<FilterSite> ScanFilterSites()
    {
        var sites = new List<FilterSite>();

        foreach (var kind in LatticeDashboards.All)
        {
            using var doc = JsonDocument.Parse(LatticeDashboards.GetGrafanaDashboardJson(kind));
            WalkJson(doc.RootElement, kind.ToString(), "dashboard", sites);
        }

        return sites;
    }

    private static void WalkJson(JsonElement node, string dashboard, string site, List<FilterSite> sink)
    {
        switch (node.ValueKind)
        {
            case JsonValueKind.Object:
                site = SiteLabel(node) ?? site;
                foreach (var property in node.EnumerateObject())
                {
                    if (property.NameEquals("expr") || property.NameEquals("query"))
                    {
                        if (property.Value.ValueKind == JsonValueKind.String)
                        {
                            ExtractMatchers(property.Value.GetString()!, dashboard, site, sink);
                            continue;
                        }
                    }
                    WalkJson(property.Value, dashboard, site, sink);
                }
                break;

            case JsonValueKind.Array:
                foreach (var item in node.EnumerateArray()) WalkJson(item, dashboard, site, sink);
                break;
        }
    }

    /// <summary>
    /// Names the enclosing site of a matcher: a panel by its numeric id, or an
    /// annotation by its name. Annotations carry queries too, and the one on the
    /// atomic-write dashboard is exactly the kind of selector this guard exists
    /// to check, so it must not be skipped for lacking a panel id.
    /// </summary>
    private static string? SiteLabel(JsonElement node)
    {
        if (node.TryGetProperty("id", out var id) && node.TryGetProperty("targets", out _))
        {
            if (id.ValueKind == JsonValueKind.Number) return id.GetRawText();
        }

        if (node.TryGetProperty("expr", out _)
            && node.TryGetProperty("name", out var name)
            && name.ValueKind == JsonValueKind.String)
        {
            return "annotation:" + name.GetString();
        }

        return null;
    }

    private static void ExtractMatchers(string expr, string dashboard, string site, List<FilterSite> sink)
    {
        foreach (Match selector in SelectorRegex.Matches(expr))
        {
            var token = selector.Groups["token"].Value;

            foreach (Match matcher in MatcherRegex.Matches(selector.Groups["body"].Value))
            {
                var tag = matcher.Groups["tag"].Value;
                var op = matcher.Groups["op"].Value;
                var raw = matcher.Groups["value"].Value;

                if (OpenDomainTags.Contains(tag)) continue;

                // Dashboard-variable-driven matchers name no literal.
                if (raw.Contains('$', StringComparison.Ordinal)) continue;

                // Only plain alternations of literal values are decidable; a
                // matcher using real regex syntax describes a pattern, not a set.
                if (!LiteralAlternationRegex.IsMatch(raw)) continue;

                sink.Add(new FilterSite(
                    dashboard, site, token, tag,
                    Negated: op is "!=" or "!~",
                    Values: raw.Split('|')));
            }
        }
    }

    // -------------------------------------------------- instrument resolution

    private static readonly Lazy<IReadOnlyDictionary<string, (Type Owner, string FieldName)>> InstrumentFieldsLazy =
        new(BuildInstrumentFields);

    private static IReadOnlyDictionary<string, (Type Owner, string FieldName)> InstrumentFields()
        => InstrumentFieldsLazy.Value;

    /// <summary>
    /// Maps each instrument's canonical dotted name onto the field that declares
    /// it, by reading the live <see cref="Instrument.Name"/> off the field value
    /// rather than inferring it from the field's name. Issue #2854 was caused by
    /// exactly that inference, so the mapping this guard runs on must not repeat
    /// it.
    /// </summary>
    private static Dictionary<string, (Type Owner, string FieldName)> BuildInstrumentFields()
    {
        var map = new Dictionary<string, (Type, string)>(StringComparer.Ordinal);

        foreach (var owner in new[] { typeof(LatticeMetrics), typeof(LatticeReplicationMetrics) })
        {
            foreach (var field in owner.GetFields(BindingFlags.Public | BindingFlags.Static))
            {
                if (field.GetValue(null) is Instrument instrument)
                {
                    map[instrument.Name] = (owner, field.Name);
                }
            }
        }

        return map;
    }

    private static string? TokenToDottedName(string token)
        => TokenFormsLazy.Value.TryGetValue(token, out var dotted) ? dotted : null;

    private static readonly Lazy<IReadOnlyDictionary<string, string>> TokenFormsLazy = new(BuildTokenForms);

    /// <summary>
    /// Maps every canonical Prometheus form of an instrument name back onto the
    /// .NET dotted name. The mapping is built forward (dotted name to forms),
    /// never by parsing a token, because the exporter turns both dots and
    /// embedded underscores into underscores and the reverse direction is
    /// genuinely ambiguous - <c>orleans.lattice.atomic_write.completed</c> and
    /// a hypothetical <c>orleans.lattice.atomic.write.completed</c> share one
    /// token. This is the same forward discipline
    /// <c>DashboardJsonTests</c> uses, and for the same reason.
    /// </summary>
    private static Dictionary<string, string> BuildTokenForms()
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);

        void AddForms(string dotted)
        {
            var underscored = dotted.Replace('.', '_');
            string[] units = ["", "_milliseconds", "_seconds", "_bytes"];
            string[] suffixes = ["", "_total", "_bucket", "_count", "_sum"];

            foreach (var unit in units)
            {
                foreach (var suffix in suffixes)
                {
                    map.TryAdd(underscored + unit + suffix, dotted);
                }
            }
        }

        foreach (var dotted in InstrumentFields().Keys) AddForms(dotted);
        foreach (var dotted in InstrumentsWithoutImperativeEmission.Keys) AddForms(dotted);

        // Observable instruments are created only when their owning subsystem
        // starts, so a snapshot of the static fields does not see them. Both
        // meter classes publish the canonical name of every such instrument as a
        // public const string ...Name field.
        foreach (var owner in new[] { typeof(LatticeMetrics), typeof(LatticeReplicationMetrics) })
        {
            foreach (var field in owner.GetFields(BindingFlags.Public | BindingFlags.Static))
            {
                if (!field.IsLiteral || field.FieldType != typeof(string)) continue;
                if (!field.Name.EndsWith("Name", StringComparison.Ordinal)) continue;
                if (field.GetRawConstantValue() is not string value) continue;
                if (!value.StartsWith("orleans.lattice", StringComparison.Ordinal)) continue;
                if (value is "orleans.lattice" or "orleans.lattice.replication") continue;
                AddForms(value);
            }
        }

        return map;
    }

    // ------------------------------------------------------ domain resolution

    private static readonly Dictionary<string, TagDomain?> DomainCache = new(StringComparer.Ordinal);

    private static TagDomain? ResolveDomain(string token, string tag)
    {
        var cacheKey = token + "\u0000" + tag;
        lock (DomainCache)
        {
            if (DomainCache.TryGetValue(cacheKey, out var cached)) return cached;
        }

        var computed = ResolveDomainCore(token, tag);
        lock (DomainCache)
        {
            DomainCache[cacheKey] = computed;
        }
        return computed;
    }

    private static TagDomain? ResolveDomainCore(string token, string tag)
    {
        var dotted = TokenToDottedName(token);
        if (dotted is null) return null;
        if (!InstrumentFields().TryGetValue(dotted, out var field)) return null;

        var values = new HashSet<string>(StringComparer.Ordinal);
        var problems = new List<string>();
        var sites = FindEmissionSites(field.FieldName);

        if (sites.Count == 0)
        {
            problems.Add(
                $"No {field.FieldName}.Add(/.Record( site found under src/. If this instrument is "
                + "observable, declare it in InstrumentsWithoutImperativeEmission with a reason.");
            return new TagDomain(values, problems);
        }

        foreach (var (file, argsStart) in sites)
        {
            var text = SourceText(file);
            var args = SplitArguments(text, argsStart);
            var bound = false;

            foreach (var arg in args)
            {
                var resolved = ResolveTagArgument(file, arg, argsStart, tag, depth: 0);
                if (resolved.Count == 0) continue;
                bound = true;
                values.UnionWith(resolved);
            }

            if (!bound)
            {
                // A site that stamps no value for this tag is only a problem when
                // no site does; an instrument may carry the tag on some arms only.
                continue;
            }
        }

        if (values.Count == 0)
        {
            problems.Add(
                $"Found {sites.Count} emission site(s) for {field.FieldName}, but could not derive "
                + $"a single value for tag '{tag}' from any of them. Extend the resolver rather "
                + "than narrowing the guard.");
        }

        return new TagDomain(values, problems);
    }

    // --------------------------------------------------------- source corpus

    private static readonly Lazy<IReadOnlyDictionary<string, string>> SourcesLazy = new(LoadSources);

    private static IReadOnlyDictionary<string, string> Sources() => SourcesLazy.Value;

    private static string SourceText(string file) => Sources()[file];

    private static Dictionary<string, string> LoadSources()
    {
        var root = HygieneRepository.FindRepoRoot();
        var src = Path.Combine(root, "src");
        var map = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (var file in HygieneRepository.EnumerateFiles(src, "*.cs"))
        {
            map[file] = File.ReadAllText(file);
        }

        return map;
    }

    /// <summary>
    /// Locates every imperative emission site for an instrument field, returning
    /// the index just past the opening parenthesis of each call.
    /// </summary>
    /// <remarks>
    /// Memoized by field name. The scan is a pure function of the source corpus,
    /// which is itself loaded once and cached, so repeating it is only cost. The
    /// doc arity gate resolves many (instrument, tag) pairs to answer a single
    /// generic arity claim, which turns this from a handful of corpus walks into
    /// several hundred.
    /// </remarks>
    private static List<(string File, int ArgsStart)> FindEmissionSites(string fieldName)
    {
        lock (EmissionSiteCache)
        {
            if (EmissionSiteCache.TryGetValue(fieldName, out var cached))
            {
                return cached;
            }
        }

        var sites = new List<(string, int)>();

        foreach (var (file, text) in Sources())
        {
            foreach (var verb in new[] { ".Add(", ".Record(" })
            {
                var needle = fieldName + verb;
                var at = text.IndexOf(needle, StringComparison.Ordinal);
                while (at >= 0)
                {
                    if (at == 0 || !IsIdentifierChar(text[at - 1]))
                    {
                        sites.Add((file, at + needle.Length));
                    }
                    at = text.IndexOf(needle, at + needle.Length, StringComparison.Ordinal);
                }
            }
        }

        lock (EmissionSiteCache)
        {
            EmissionSiteCache[fieldName] = sites;
        }

        return sites;
    }

    private static readonly Dictionary<string, List<(string File, int ArgsStart)>> EmissionSiteCache =
        new(StringComparer.Ordinal);

    private static bool IsIdentifierChar(char c) => char.IsLetterOrDigit(c) || c == '_';

    // -------------------------------------------------------- expression work

    /// <summary>
    /// Splits the argument list that starts at <paramref name="start"/> (just
    /// past an opening parenthesis) into top-level argument expressions,
    /// tracking nesting and string literals so a comma inside either does not
    /// split.
    /// </summary>
    private static List<string> SplitArguments(string text, int start)
    {
        var args = new List<string>();
        var depth = 0;
        var begin = start;
        var i = start;

        while (i < text.Length)
        {
            var c = text[i];

            if (c == '"')
            {
                i = SkipString(text, i);
                continue;
            }

            if (c is '(' or '[' or '<' or '{')
            {
                if (c != '<' || LooksLikeGenericOpen(text, i)) depth++;
            }
            else if (c is ')' or ']' or '>' or '}')
            {
                if (c == ')' && depth == 0)
                {
                    args.Add(text[begin..i]);
                    return args;
                }
                if (c != '>' || depth > 0) depth--;
            }
            else if (c == ',' && depth == 0)
            {
                args.Add(text[begin..i]);
                begin = i + 1;
            }

            i++;
        }

        return args;
    }

    private static bool LooksLikeGenericOpen(string text, int i)
    {
        // A '<' opens a generic argument list only when it follows an
        // identifier; otherwise it is a comparison and must not affect depth.
        return i > 0 && IsIdentifierChar(text[i - 1]);
    }

    private static int SkipString(string text, int i)
    {
        i++;
        while (i < text.Length)
        {
            if (text[i] == '\\') { i += 2; continue; }
            if (text[i] == '"') return i + 1;
            i++;
        }
        return i;
    }

    private static readonly Regex KvpConstructionRegex =
        new(@"new(?:\s+KeyValuePair\s*<[^>]*>)?\s*\(", RegexOptions.Compiled);

    private static readonly Regex IdentifierPathRegex =
        new(@"\b(?<owner>[A-Z][A-Za-z0-9_]*)\.(?<member>[A-Za-z_][A-Za-z0-9_]*)\b(?!\s*\()",
            RegexOptions.Compiled);

    private static readonly Regex CallRegex =
        new(@"\b(?:(?<owner>[A-Z][A-Za-z0-9_]*)\.)?(?<name>[A-Za-z_][A-Za-z0-9_]*)\s*\(",
            RegexOptions.Compiled);

    private static readonly Regex StringLiteralRegex =
        new("\"(?<v>[^\"\\\\]*)\"", RegexOptions.Compiled);

    /// <summary>
    /// Resolves the tag values an argument at a tag position contributes for
    /// <paramref name="tag"/>. Returns an empty set when the argument binds a
    /// different tag; an argument this resolver cannot read at all is reported
    /// upstream as a missing domain rather than silently treated as empty.
    /// </summary>
    private static IReadOnlyCollection<string> ResolveTagArgument(
        string file, string arg, int siteIndex, string tag, int depth)
    {
        var values = new HashSet<string>(StringComparer.Ordinal);
        if (depth > 3) return values;

        arg = arg.Trim();
        if (arg.Length == 0) return values;

        // Shape 1: an inline KeyValuePair construction, target-typed or not.
        foreach (Match ctor in KvpConstructionRegex.Matches(arg))
        {
            var parts = SplitArguments(arg, ctor.Index + ctor.Length);
            if (parts.Count < 2) continue;
            if (!string.Equals(ResolveConstString(parts[0].Trim()), tag, StringComparison.Ordinal)) continue;
            values.UnionWith(ResolveValueExpression(file, parts[1], siteIndex, depth + 1));
        }
        if (values.Count > 0) return values;

        // Shape 2: a KeyValuePair constant referenced by name. Reflected off the
        // live field value, so the tag key is the one the process really stamps.
        foreach (Match path in IdentifierPathRegex.Matches(arg))
        {
            if (ResolveKvpConstant(path.Groups["owner"].Value, path.Groups["member"].Value) is not { } kvp) continue;
            if (!string.Equals(kvp.Key, tag, StringComparison.Ordinal)) continue;
            if (kvp.Value is string s) values.Add(s);
        }
        if (values.Count > 0) return values;

        // Shape 3: a collection of tags, built in a local and splatted.
        var identifier = arg.EndsWith(".ToArray()", StringComparison.Ordinal)
            ? arg[..^".ToArray()".Length].Trim()
            : arg;

        if (IsSimpleIdentifier(identifier))
        {
            // 3a: a local whose initialiser carries the tag bindings.
            if (FindLocalInitialiser(file, identifier, siteIndex) is { } init)
            {
                values.UnionWith(ResolveTagArgument(file, init, siteIndex, tag, depth + 1));
                foreach (var added in FindCollectionAdditions(file, identifier, siteIndex))
                {
                    values.UnionWith(ResolveTagArgument(file, added, siteIndex, tag, depth + 1));
                }
                if (values.Count > 0) return values;
            }

            // 3b: a parameter of the enclosing helper - hop to its callers.
            values.UnionWith(ResolveThroughParameter(file, identifier, siteIndex, tag, depth));
            if (values.Count > 0) return values;
        }

        // Shape 4: a call returning a tag, or a ternary mixing calls and
        // constants. Resolve the callee bodies.
        foreach (Match call in CallRegex.Matches(arg))
        {
            var name = call.Groups["name"].Value;
            if (name is "new" or "nameof" or "typeof") continue;
            foreach (var body in FindMethodBodies(name))
            {
                values.UnionWith(ResolveTagArgument(body.File, body.Body, body.Index, tag, depth + 1));
            }
        }

        return values;
    }

    /// <summary>
    /// Resolves an expression in a tag <i>value</i> position to the literal
    /// strings it can produce.
    /// </summary>
    private static IReadOnlyCollection<string> ResolveValueExpression(
        string file, string expr, int siteIndex, int depth)
    {
        var values = new HashSet<string>(StringComparer.Ordinal);
        if (depth > 4) return values;

        expr = expr.Trim();
        if (expr.Length == 0) return values;

        // A literal, or a set of them in a ternary or switch expression.
        foreach (Match literal in StringLiteralRegex.Matches(expr))
        {
            values.Add(literal.Groups["v"].Value);
        }
        if (values.Count > 0) return values;

        // A const string field. Only consulted when the expression carries no
        // literal of its own, so an unrelated const used as a predicate argument
        // beside real literals cannot leak in as a tag value.
        foreach (Match path in IdentifierPathRegex.Matches(expr))
        {
            if (ResolveConstString(path.Value) is { } constant) values.Add(constant);
        }
        if (values.Count > 0) return values;

        if (IsSimpleIdentifier(expr))
        {
            if (FindLocalInitialiser(file, expr, siteIndex) is { } init)
            {
                values.UnionWith(ResolveValueExpression(file, init, siteIndex, depth + 1));
                if (values.Count > 0) return values;
            }

            values.UnionWith(ResolveValueThroughParameter(file, expr, siteIndex, depth));
            if (values.Count > 0) return values;
        }

        foreach (Match call in CallRegex.Matches(expr))
        {
            var name = call.Groups["name"].Value;
            if (name is "new" or "nameof" or "typeof") continue;
            foreach (var body in FindMethodBodies(name))
            {
                values.UnionWith(ResolveValueExpression(body.File, body.Body, body.Index, depth + 1));
            }
        }

        return values;
    }

    private static bool IsSimpleIdentifier(string text)
        => text.Length > 0 && (char.IsLetter(text[0]) || text[0] == '_') && text.All(IsIdentifierChar);

    // ------------------------------------------------------ reflection lookup

    private static readonly Lazy<IReadOnlyDictionary<string, Type>> TypesLazy = new(BuildTypeIndex);

    private static Dictionary<string, Type> BuildTypeIndex()
    {
        var map = new Dictionary<string, Type>(StringComparer.Ordinal);

        foreach (var assembly in new[] { typeof(LatticeMetrics).Assembly, typeof(LatticeReplicationMetrics).Assembly })
        {
            foreach (var type in assembly.GetTypes())
            {
                map.TryAdd(type.Name, type);
            }
        }

        return map;
    }

    private static string? ResolveConstString(string path)
    {
        var dot = path.LastIndexOf('.');
        if (dot <= 0) return null;

        if (!TypesLazy.Value.TryGetValue(path[..dot], out var owner)) return null;

        var field = owner.GetField(path[(dot + 1)..],
            BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static);

        if (field is null || !field.IsLiteral || field.FieldType != typeof(string)) return null;
        return field.GetRawConstantValue() as string;
    }

    private static KeyValuePair<string, object?>? ResolveKvpConstant(string ownerName, string memberName)
    {
        if (!TypesLazy.Value.TryGetValue(ownerName, out var owner)) return null;

        var field = owner.GetField(memberName,
            BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static);

        if (field is null || field.FieldType != typeof(KeyValuePair<string, object?>)) return null;
        return (KeyValuePair<string, object?>)field.GetValue(null)!;
    }

    // ------------------------------------------------------ syntax navigation

    /// <summary>
    /// Finds the member declaration enclosing <paramref name="index"/>, relying
    /// on the repository's consistent four-space member indentation, and returns
    /// its name, parameter list, and body span.
    /// </summary>
    private static (string Name, IReadOnlyList<string> Parameters, int BodyStart, int BodyEnd)? FindEnclosingMember(
        string text, int index)
    {
        var declaration = MemberDeclarationRegex.Matches(text)
            .Cast<Match>()
            .Where(m => m.Index < index)
            .OrderByDescending(m => m.Index)
            .FirstOrDefault();

        if (declaration is null) return null;

        var parameters = SplitArguments(text, declaration.Index + declaration.Length);
        var afterParameters = declaration.Index + declaration.Length;
        var depth = 0;

        while (afterParameters < text.Length)
        {
            var c = text[afterParameters];
            if (c == '"') { afterParameters = SkipString(text, afterParameters); continue; }
            if (c == '(') depth++;
            if (c == ')')
            {
                if (depth == 0) { afterParameters++; break; }
                depth--;
            }
            afterParameters++;
        }

        var bodyStart = text.IndexOf('{', afterParameters);
        var arrow = text.IndexOf("=>", afterParameters, StringComparison.Ordinal);

        int end;
        if (arrow >= 0 && (bodyStart < 0 || arrow < bodyStart))
        {
            bodyStart = arrow + 2;
            end = text.IndexOf(';', bodyStart);
            if (end < 0) return null;
        }
        else
        {
            if (bodyStart < 0) return null;
            end = MatchBrace(text, bodyStart);
            if (end < 0) return null;
        }

        if (index < declaration.Index || index > end) return null;

        return (declaration.Groups["name"].Value, parameters, bodyStart, end);
    }

    private static readonly Regex MemberDeclarationRegex = new(
        @"(?m)^ {4}(?:(?:public|private|internal|protected|static|async|override|virtual|sealed|partial|new|unsafe|extern)\s+)+"
        + @"[A-Za-z_][A-Za-z0-9_<>\[\],\.\? ]*?\b(?<name>[A-Za-z_][A-Za-z0-9_]*)\s*\(",
        RegexOptions.Compiled);

    private static int MatchBrace(string text, int open)
    {
        var depth = 0;
        for (var i = open; i < text.Length; i++)
        {
            var c = text[i];
            if (c == '"') { i = SkipString(text, i) - 1; continue; }
            if (c == '{') depth++;
            else if (c == '}')
            {
                depth--;
                if (depth == 0) return i;
            }
        }
        return -1;
    }

    /// <summary>
    /// Returns the initialiser expression of a local (or field) declaration of
    /// <paramref name="name"/> that is visible from <paramref name="siteIndex"/>.
    /// </summary>
    private static string? FindLocalInitialiser(string file, string name, int siteIndex)
    {
        var text = SourceText(file);
        var pattern = new Regex(
            @"(?:\bvar\b|[A-Za-z_][A-Za-z0-9_<>,\.\?\[\] ]*?)\s+" + Regex.Escape(name) + @"\s*=(?!=)",
            RegexOptions.None);

        Match? best = null;
        var member = FindEnclosingMember(text, siteIndex);
        var lowerBound = member?.BodyStart ?? 0;

        foreach (Match match in pattern.Matches(text))
        {
            if (match.Index >= siteIndex) continue;
            if (match.Index < lowerBound) continue;
            if (best is null || match.Index > best.Index) best = match;
        }

        if (best is null) return null;

        var end = FindStatementEnd(text, best.Index + best.Length);
        return end < 0 ? null : text[(best.Index + best.Length)..end];
    }

    private static IEnumerable<string> FindCollectionAdditions(string file, string name, int siteIndex)
    {
        var text = SourceText(file);
        var needle = name + ".Add(";
        var at = text.IndexOf(needle, StringComparison.Ordinal);

        while (at >= 0)
        {
            if (at < siteIndex && (at == 0 || !IsIdentifierChar(text[at - 1])))
            {
                foreach (var arg in SplitArguments(text, at + needle.Length)) yield return arg;
            }
            at = text.IndexOf(needle, at + needle.Length, StringComparison.Ordinal);
        }
    }

    private static int FindStatementEnd(string text, int start)
    {
        var depth = 0;
        for (var i = start; i < text.Length; i++)
        {
            var c = text[i];
            if (c == '"') { i = SkipString(text, i) - 1; continue; }
            if (c is '(' or '[' or '{') depth++;
            else if (c is ')' or ']' or '}') depth--;
            else if (c == ';' && depth <= 0) return i;
        }
        return -1;
    }

    /// <summary>
    /// Hops one level outward: the argument is a parameter of the helper that
    /// performs the emission, so the values come from what its callers pass at
    /// that position. This is the shape <c>LatticeWalGcScheduler</c> uses for
    /// both WAL GC counters.
    /// </summary>
    private static IReadOnlyCollection<string> ResolveThroughParameter(
        string file, string name, int siteIndex, string tag, int depth)
    {
        var values = new HashSet<string>(StringComparer.Ordinal);

        foreach (var (callFile, argument) in EnumerateCallerArguments(file, name, siteIndex))
        {
            values.UnionWith(ResolveTagArgument(callFile.File, argument, callFile.Index, tag, depth + 1));
        }

        return values;
    }

    private static IReadOnlyCollection<string> ResolveValueThroughParameter(
        string file, string name, int siteIndex, int depth)
    {
        var values = new HashSet<string>(StringComparer.Ordinal);

        foreach (var (callFile, argument) in EnumerateCallerArguments(file, name, siteIndex))
        {
            values.UnionWith(ResolveValueExpression(callFile.File, argument, callFile.Index, depth + 1));
        }

        return values;
    }

    private static IEnumerable<((string File, int Index) Call, string Argument)> EnumerateCallerArguments(
        string file, string parameterName, int siteIndex)
    {
        var text = SourceText(file);
        var member = FindEnclosingMember(text, siteIndex);
        if (member is null) yield break;

        var parameters = member.Value.Parameters;
        var position = -1;

        for (var i = 0; i < parameters.Count; i++)
        {
            var trimmed = parameters[i].Trim();
            if (trimmed.Length == 0) continue;
            var lastSpace = trimmed.LastIndexOf(' ');
            if (lastSpace < 0) continue;
            if (string.Equals(trimmed[(lastSpace + 1)..], parameterName, StringComparison.Ordinal))
            {
                position = i;
                break;
            }
        }

        if (position < 0) yield break;

        var name = member.Value.Name;
        var needle = name + "(";

        foreach (var (callFile, callText) in Sources())
        {
            var declarations = DeclarationSpans(callFile, name);
            var at = callText.IndexOf(needle, StringComparison.Ordinal);

            while (at >= 0)
            {
                var isCall = (at == 0 || !IsIdentifierChar(callText[at - 1]))
                    && !declarations.Any(span => at >= span.Start && at < span.End);

                if (isCall)
                {
                    var arguments = SplitArguments(callText, at + needle.Length);
                    if (position < arguments.Count)
                    {
                        yield return ((callFile, at), arguments[position]);
                    }
                }

                at = callText.IndexOf(needle, at + needle.Length, StringComparison.Ordinal);
            }
        }
    }

    /// <summary>
    /// Spans of the member declarations named <paramref name="name"/> in a file,
    /// so the declaration's own parameter list is not mistaken for a call site
    /// when hopping outward to callers.
    /// </summary>
    private static IReadOnlyList<(int Start, int End)> DeclarationSpans(string file, string name)
    {
        var spans = new List<(int, int)>();

        foreach (Match declaration in MemberDeclarationRegex.Matches(SourceText(file)))
        {
            if (string.Equals(declaration.Groups["name"].Value, name, StringComparison.Ordinal))
            {
                spans.Add((declaration.Index, declaration.Index + declaration.Length));
            }
        }

        return spans;
    }

    private readonly record struct MethodBody(string File, string Body, int Index);

    private static readonly Dictionary<string, List<MethodBody>> MethodBodyCache = new(StringComparer.Ordinal);

    /// <summary>
    /// Returns the bodies of every method declared under <c>src/</c> with the
    /// given name. Resolution is by simple name, which is an approximation, but
    /// an over-wide candidate set can only add values a same-named method could
    /// produce - and the fixture fails loudly rather than silently when a real
    /// binding is missed.
    /// </summary>
    private static IReadOnlyList<MethodBody> FindMethodBodies(string name)
    {
        lock (MethodBodyCache)
        {
            if (MethodBodyCache.TryGetValue(name, out var cached)) return cached;
        }

        var bodies = new List<MethodBody>();

        foreach (var (file, text) in Sources())
        {
            foreach (Match declaration in MemberDeclarationRegex.Matches(text))
            {
                if (!string.Equals(declaration.Groups["name"].Value, name, StringComparison.Ordinal)) continue;

                var member = FindEnclosingMember(text, declaration.Index + declaration.Length);
                if (member is null) continue;

                bodies.Add(new MethodBody(file, text[member.Value.BodyStart..member.Value.BodyEnd], member.Value.BodyStart));
            }
        }

        lock (MethodBodyCache)
        {
            MethodBodyCache[name] = bodies;
        }

        return bodies;
    }

    // -------------------------------------------- shared with the doc arity gate

    /// <summary>
    /// The source-derived armed value set for one instrument/tag pair, addressed
    /// by the instrument's canonical dotted name.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Exposed so that <c>MetricDocArmArityTests</c> can check a documentation
    /// row's arity claim against the same derivation this guard already runs on,
    /// rather than against a second resolver written beside it. Sharing one
    /// derivation is the whole point: two independently written scanners that
    /// agree are a coincidence that decays, whereas one scanner used twice cannot
    /// disagree with itself, so the documented arity and the charted domain can
    /// never drift apart while both still pass.
    /// </para>
    /// <para>
    /// Returns <see langword="null"/> rather than an empty set for an instrument
    /// this resolver cannot read, so a caller can tell "no value is armed" from
    /// "no value could be derived". Collapsing those two would let an unreadable
    /// instrument read as a zero-arm one, which is the silent-absence failure
    /// mode this whole family of guards exists to remove.
    /// </para>
    /// </remarks>
    /// <param name="dottedName">The instrument's canonical dotted name.</param>
    /// <param name="tag">The tag key whose armed values are wanted.</param>
    /// <returns>The armed values, or <see langword="null"/> when undecidable.</returns>
    internal static IReadOnlySet<string>? ArmedValues(string dottedName, string tag)
    {
        if (!InstrumentFields().ContainsKey(dottedName))
        {
            return null;
        }

        var domain = ResolveDomain(dottedName.Replace('.', '_'), tag);
        return domain is null || domain.Values.Count == 0 ? null : domain.Values;
    }
}
