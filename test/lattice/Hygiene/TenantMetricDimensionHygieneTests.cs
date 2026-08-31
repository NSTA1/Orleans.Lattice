using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Repository-wide gate for the derived <c>tenant</c> metric dimension. Asserts
/// that every metric emission site in every package carries the dimension, and
/// pins the set of instruments that carry the constant platform sentinel - the
/// documented "unscopable" list of series that are not attributable to any one
/// tenant.
/// </summary>
/// <remarks>
/// <para>
/// The dimension is <b>always emitted</b>, on tenancy-on and tenancy-off clusters
/// alike, so a dashboard panel or a named telemetry query is byte-identical in
/// both deployment modes. That guarantee only holds if no emission site is ever
/// added without the label, which is what this gate enforces - the same shape as
/// the em-dash, mojibake, and integration-category hygiene gates.
/// </para>
/// <para>
/// Every assertion here is a deterministic file scan: nothing depends on timing,
/// ordering, or a running cluster.
/// </para>
/// </remarks>
[TestFixture]
public sealed class TenantMetricDimensionHygieneTests
{
    /// <summary>
    /// Emission sites that pass a pre-built tag collection (an array, a
    /// <c>TagList</c>, or a <c>List</c>) rather than inline tags. The dimension is
    /// wired in where the collection is built, once, instead of at each call - so
    /// the call itself legitimately names no tenant tag.
    /// <para>
    /// Each entry records where the collection is built and how it classifies, and
    /// <see cref="Every_pre_built_tag_collection_is_wired_to_the_tenant_dimension"/>
    /// proves the owning file really does wire the dimension.
    /// </para>
    /// </summary>
    private static readonly (string File, string TagExpression, bool PlatformSentinel)[] PreBuiltTagCollections =
    [
        // (tree, shard, tenant) built once per activation.
        ("src/lattice/BPlusTree/Grains/ShardRootGrain.Hotness.cs", "GetMetricTags()", false),
        ("src/lattice/BPlusTree/Grains/ShardRootGrain.Hotness.cs", "tags", false),
        // (tree, shard, tenant) built per emission. Both sites are cold - the
        // throttled leaf-access model flush and the once-per-activation
        // leaf-cache pre-warm - so they build their own array rather than
        // sharing the hot path's activation cache.
        ("src/lattice/BPlusTree/Grains/ShardRootGrain.LeafAccessTracking.cs", "LeafAccessMetricTags()", false),
        ("src/lattice/BPlusTree/Grains/ShardRootGrain.LeafAccessTracking.cs", "tags", false),
        // (tree, shard, tenant) built once per replay.
        ("src/lattice/BPlusTree/Grains/SnapshotLeafGrain.cs", "tags", false),
        // (tree, state, previous_state, tenant [, partition][, shard]) built per transition.
        ("src/lattice/BPlusTree/Grains/WalSaturationSampler.cs", "tags.ToArray()", false),
        // (operation, tree, tenant, ...) TagList built once per authorization decision.
        ("src/lattice.auth/LatticeAuthDecisionObserver.cs", "tags", false),
        // (tree[, shard], tenant) built once per buffer.
        ("src/lattice.replication/CausalApplyBuffer.cs", "_treeTags", false),
        ("src/lattice.replication/CausalApplyBuffer.cs", "_treeShardTags", false),
        // A tag index spans every tree it covers, so a sweep is a multi-tree
        // aggregate and carries the platform sentinel.
        ("src/lattice/BPlusTree/Grains/TagIndexReconcileGrain.cs", "indexTags", true),
        ("src/lattice/BPlusTree/Grains/TagIndexReconcileGrain.cs", "outcomeTags", true),
        // Per-tenant snapshot measurements: the tenancy meter's own tenant tag.
        ("src/lattice.tenancy/TenantObservabilityGaugeSnapshot.cs", "tags", false),
    ];

    /// <summary>
    /// The documented unscopable list: instruments whose every emission site
    /// carries the constant platform sentinel because the measurement is not
    /// attributable to a single tenant. They still emit the dimension (so one
    /// query shape works everywhere), they just never name a tenant.
    /// </summary>
    /// <remarks>
    /// Grouped by why they are unscopable:
    /// <list type="bullet">
    ///   <item><b>Cross-tree by construction</b> - a cross-tree saga, atomic
    ///   action, backup scope, restore, or tag-index sweep spans an
    ///   operator-chosen set of trees that may belong to different tenants, so no
    ///   single tenant owns the measurement.</item>
    ///   <item><b>Cluster-level</b> - membership resolution and directory search,
    ///   auth snapshot gauges, autoscaler signals, compression-dictionary
    ///   training, and the tenant-count aggregate are properties of the cluster,
    ///   not of any tenant's traffic.</item>
    ///   <item><b>Named-lock and transport</b> - a distributed lock is keyed by a
    ///   caller-chosen name, and an insecure-channel warning is keyed by peer and
    ///   transport; neither carries a tree.</item>
    ///   <item><b>Platform tooling</b> - the repository-context MCP surface meters
    ///   its own usage, which is operator tooling rather than tenant traffic.</item>
    /// </list>
    /// Adding an instrument here is a deliberate, reviewable act: it declares the
    /// series invisible to every tenant-scoped telemetry query.
    /// </remarks>
    private static readonly string[] PlatformSentinelInstruments =
    [
        "ApplyParallelRuns",
        "AtomicActionCompleted",
        "AtomicActionDuration",
        "AtomicActionStep",
        "CaptureFailures",
        "CaptureRetries",
        "CompressionDictionaryTrainedBytesIn",
        "CompressionDictionaryTrainedBytesOut",
        "CompressionDictionaryTrainingRuns",
        "CrossTreeAtomicWriteCompleted",
        "CrossTreeAtomicWriteDuration",
        "CrossTreeAtomicWriteParticipants",
        "CrossTreeFenceDrainWaitMilliseconds",
        "CrossTreeFenceDrainedInFlight",
        "CrossTreeFenceRetries",
        "CrossTreeFenceSelections",
        "DirectorySearchDuration",
        "DirectorySearchHits",
        "DirectorySearchMisses",
        "IncrementalLagAge",
        "IncrementalLagEntries",
        "InsecureChannel",
        "LockAcquireWait",
        "LockAcquired",
        "LockLeaseReclaimed",
        "LockReleased",
        "MerkleWalkAborted",
        "OrphanRowsRemovedCounter",
        "ProviderRetryAttempts",
        "ProviderRetryShortCircuited",
        "ResolutionCacheHits",
        "ResolutionCacheMisses",
        "RestoreDuration",
        "RestoreEntriesApplied",
        "RestoreFailures",
        "RetentionBytesReclaimed",
        "RetentionPruned",
        "SagaCompensations",
        "SagaParticipantAborts",
        "SagaParticipantCommits",
        "SagaParticipantVotes",
        "SagaPhaseDuration",
        "SchedulerOverruns",
        "SchedulerSkipped",
        "SnapshotRebuilds",
        "SweepDurationHistogram",
        "SweepsCounter",
        "TreesMismatchedCounter",
        "TreesProbedCounter",
        "_callsCounter",
        "_replacedCounter",
        "_responseCounter",
    ];

    private static IReadOnlyList<MetricEmissionScanner.EmissionSite>? _sites;

    private static IReadOnlyList<MetricEmissionScanner.EmissionSite> Sites =>
        _sites ??= MetricEmissionScanner.Scan(HygieneRepository.FindRepoRoot());

    [Test]
    public void The_scan_finds_the_metric_emission_surface()
    {
        // Guards every other assertion in this fixture from passing vacuously if
        // the scanner ever stops matching.
        Assert.That(Sites, Has.Count.GreaterThan(250));
    }

    [Test]
    public void Every_metric_emission_site_carries_the_tenant_dimension()
    {
        var missing = new List<string>();
        foreach (var site in Sites)
        {
            if (MetricEmissionScanner.TenantDimension.IsMatch(site.Tags))
            {
                continue;
            }

            if (IsPreBuiltCollection(site, out _))
            {
                continue;
            }

            missing.Add($"{site.RelativePath}:{site.Line}  {site.Instrument}(..., {site.Tags})");
        }

        missing.Sort(StringComparer.Ordinal);
        Assert.That(missing, Is.Empty,
            "Every metric instrument must emit the derived 'tenant' dimension so a telemetry query is " +
            "byte-identical on a tenancy-on and a tenancy-off cluster. Pass " +
            "LatticeTenantLabel.ForTree(<treeId>) where the site has a tree, or LatticeTenantLabel.Platform " +
            "where it does not (and add the instrument to PlatformSentinelInstruments). If the site passes a " +
            "pre-built tag collection, wire the dimension where the collection is built and register the site " +
            $"in PreBuiltTagCollections:{Environment.NewLine}  - " +
            string.Join(Environment.NewLine + "  - ", missing));
    }

    [Test]
    public void Every_pre_built_tag_collection_is_wired_to_the_tenant_dimension()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var unwired = new List<string>();

        foreach (var (file, tagExpression, _) in PreBuiltTagCollections.DistinctBy(static e => e.File))
        {
            var text = File.ReadAllText(Path.Combine(repoRoot, file));
            if (!MetricEmissionScanner.TenantDimension.IsMatch(text))
            {
                unwired.Add($"{file} (collection '{tagExpression}')");
            }
        }

        Assert.That(unwired, Is.Empty,
            "A file whose emission sites pass a pre-built tag collection must wire the derived tenant " +
            $"dimension where the collection is built:{Environment.NewLine}  - " +
            string.Join(Environment.NewLine + "  - ", unwired));
    }

    [Test]
    public void Every_registered_pre_built_tag_collection_is_still_used()
    {
        var stale = PreBuiltTagCollections
            .Where(entry => !Sites.Any(site =>
                string.Equals(site.RelativePath, entry.File, StringComparison.Ordinal)
                && site.Tags.Contains(entry.TagExpression, StringComparison.Ordinal)))
            .Select(static entry => $"{entry.File} :: {entry.TagExpression}")
            .OrderBy(static s => s, StringComparer.Ordinal)
            .ToList();

        Assert.That(stale, Is.Empty,
            "PreBuiltTagCollections names an emission site that no longer exists; remove the stale entry so " +
            $"the exemption list cannot silently widen:{Environment.NewLine}  - " +
            string.Join(Environment.NewLine + "  - ", stale));
    }

    [Test]
    public void Every_registered_path_is_platform_neutral()
    {
        // The registry is compared ordinally against a scanned repo-relative
        // path. Path.GetRelativePath yields '\' on Windows and '/' elsewhere,
        // so a registry entry written with a backslash matches on a developer
        // machine and matches nothing in Linux CI - every exemption lookup
        // silently becomes a miss, and the failure only ever surfaces on one
        // platform. Pin the separator so that divergence fails everywhere.
        var offenders = PreBuiltTagCollections
            .Select(static entry => entry.File)
            .Where(static file => file.Contains('\\', StringComparison.Ordinal))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(static f => f, StringComparer.Ordinal)
            .ToList();

        Assert.That(
            offenders,
            Is.Empty,
            "A registered path must use '/' so it compares equal on every platform:"
            + $"{Environment.NewLine}  - "
            + string.Join(Environment.NewLine + "  - ", offenders));
    }

    [Test]
    public void The_platform_sentinel_instrument_list_matches_the_source()
    {
        var classified = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);

        foreach (var site in Sites)
        {
            if (site.Instrument == "Measurement")
            {
                continue;
            }

            string kind;
            if (IsPreBuiltCollection(site, out var platformCollection))
            {
                kind = platformCollection ? "platform" : "derived";
            }
            else if (MetricEmissionScanner.DerivedTenant.IsMatch(site.Tags))
            {
                kind = "derived";
            }
            else if (MetricEmissionScanner.PlatformSentinel.IsMatch(site.Tags))
            {
                kind = "platform";
            }
            else
            {
                kind = "none";
            }

            if (!classified.TryGetValue(site.Instrument, out var kinds))
            {
                classified[site.Instrument] = kinds = new HashSet<string>(StringComparer.Ordinal);
            }

            kinds.Add(kind);
        }

        var actual = classified
            .Where(static pair => pair.Value.Count == 1 && pair.Value.Contains("platform"))
            .Select(static pair => pair.Key)
            .OrderBy(static name => name, StringComparer.Ordinal)
            .ToList();

        Assert.That(actual, Is.EqualTo(PlatformSentinelInstruments.OrderBy(static n => n, StringComparer.Ordinal).ToList()),
            "The documented unscopable list must match the source exactly. An instrument that only ever emits " +
            "LatticeTenantLabel.Platform is invisible to every tenant-scoped telemetry query, so adding or " +
            "removing one must be a deliberate edit to PlatformSentinelInstruments.");
    }

    [Test]
    public void No_instrument_mixes_a_derived_tenant_with_the_platform_sentinel()
    {
        var mixed = new List<string>();
        var byInstrument = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);

        foreach (var site in Sites)
        {
            if (site.Instrument == "Measurement" || IsPreBuiltCollection(site, out _))
            {
                continue;
            }

            var kind = MetricEmissionScanner.DerivedTenant.IsMatch(site.Tags) ? "derived"
                : MetricEmissionScanner.PlatformSentinel.IsMatch(site.Tags) ? "platform"
                : "none";

            if (!byInstrument.TryGetValue(site.Instrument, out var kinds))
            {
                byInstrument[site.Instrument] = kinds = new HashSet<string>(StringComparer.Ordinal);
            }

            kinds.Add(kind);
        }

        foreach (var (instrument, kinds) in byInstrument)
        {
            if (kinds.Count > 1)
            {
                mixed.Add($"{instrument}: {string.Join(", ", kinds.OrderBy(static k => k, StringComparer.Ordinal))}");
            }
        }

        mixed.Sort(StringComparer.Ordinal);
        Assert.That(mixed, Is.Empty,
            "An instrument that sometimes derives a tenant and sometimes reports the platform sentinel splits " +
            "its own series across two attribution rules, so an operator cannot tell an unattributable " +
            $"measurement from a missed one:{Environment.NewLine}  - " +
            string.Join(Environment.NewLine + "  - ", mixed));
    }

    [Test]
    public void Every_observable_instrument_registration_wires_the_tenant_dimension()
    {
        // An observable gauge's measurements come from a callback, not from an
        // argument list, so the emission-site scan above cannot see them. Every
        // file that registers one must therefore reference the dimension, which
        // is what forces its callback to hand back a tagged Measurement<T>
        // instead of a bare scalar.
        var repoRoot = HygieneRepository.FindRepoRoot();
        var registration = new Regex(
            @"Create(?:ObservableGauge|ObservableCounter|ObservableUpDownCounter)\b",
            RegexOptions.Compiled);

        var unwired = new List<string>();
        var registrars = 0;
        foreach (var path in Directory.EnumerateFiles(Path.Combine(repoRoot, "src"), "*.cs", SearchOption.AllDirectories))
        {
            if (HygieneRepository.HasExcludedSegment(path))
            {
                continue;
            }

            var text = File.ReadAllText(path);
            if (!registration.IsMatch(text))
            {
                continue;
            }

            registrars++;
            if (!MetricEmissionScanner.TenantDimension.IsMatch(text))
            {
                unwired.Add(Path.GetRelativePath(repoRoot, path));
            }
        }

        unwired.Sort(StringComparer.Ordinal);
        Assert.That(registrars, Is.GreaterThan(5), "the observable-instrument scan found nothing to check");
        Assert.That(unwired, Is.Empty,
            "A file that registers an observable instrument must emit the derived tenant dimension from its " +
            "observe callback - return LatticeTenantLabel.PlatformMeasurement(value) for a cluster-level gauge, " +
            "or a Measurement<T> carrying LatticeTenantLabel.ForTree(<treeId>) for a tree-scoped one:" +
            $"{Environment.NewLine}  - " + string.Join(Environment.NewLine + "  - ", unwired));
    }

    [Test]
    public void The_detector_flags_an_emission_site_with_no_tenant_dimension()
    {
        // The smoke-detector-battery test for the gate itself: a gate that can no
        // longer see a violation silently passes forever.
        const string violating = "1, new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId)";
        const string compliant = "1, new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId), "
            + "LatticeTenantLabel.ForTree(treeId)";

        Assert.Multiple(() =>
        {
            Assert.That(MetricEmissionScanner.TenantDimension.IsMatch(violating), Is.False);
            Assert.That(MetricEmissionScanner.TenantDimension.IsMatch(compliant), Is.True);
            Assert.That(MetricEmissionScanner.PlatformSentinel.IsMatch("1, LatticeTenantLabel.Platform"), Is.True);
            Assert.That(MetricEmissionScanner.DerivedTenant.IsMatch("1, LatticeTenantLabel.Platform"), Is.False);
        });
    }

    private static bool IsPreBuiltCollection(MetricEmissionScanner.EmissionSite site, out bool platformSentinel)
    {
        foreach (var (file, tagExpression, platform) in PreBuiltTagCollections)
        {
            if (string.Equals(site.RelativePath, file, StringComparison.Ordinal)
                && string.Equals(site.Tags, tagExpression, StringComparison.Ordinal))
            {
                platformSentinel = platform;
                return true;
            }
        }

        platformSentinel = false;
        return false;
    }
}
