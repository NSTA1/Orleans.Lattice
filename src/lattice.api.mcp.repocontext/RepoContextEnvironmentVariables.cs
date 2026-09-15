using System.Globalization;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Publishes every environment variable the repository-context package itself resolves,
/// so a host that reports its effective configuration can <b>derive</b> that set instead
/// of restating it.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this exists (issue #2460).</b> The host's startup effective-configuration report
/// decided which supplied <c>LATTICE_</c> variables were read by testing them against a
/// hand-maintained list. That list was drawn only from the host's own configuration
/// classes, so every variable this package reads was absent from it and was reported
/// <c>[SUPPLIED BUT NOT READ BY THIS HOST]</c> - the exact inversion of the truth, in a
/// report an operator is invited to trust. A deliberately set throttle was announced as
/// inert while it was visibly steering the runtime.
/// </para>
/// <para>
/// <b>Why a published registry rather than a longer hand-maintained list.</b> Extending
/// the host's list would fix the five known omissions and leave the mechanism that
/// produced them intact: the next key added to
/// <c>RepoContextIndexingOptions</c> would be mislabelled the same way, and nothing would
/// say so. The keys here are the option classes' own constants, so a key that exists is a
/// key that is published.
/// </para>
/// <para>
/// <b>Why prefixes are published separately.</b> The per-repository git settings are a
/// <i>family</i> named after a repository id that is only known at run time, so no
/// exact-match set can ever cover them. A registry that published only exact keys would
/// report a correctly supplied <c>LATTICE_REPOCONTEXT_GIT_MYREPO_URL</c> as unread, which
/// is the same defect one level down. <see cref="Prefixes"/> lets a host recognise the
/// family and say what it does and does not know about a member of it.
/// </para>
/// </remarks>
public static class RepoContextEnvironmentVariables
{
    /// <summary>
    /// The prefix every per-repository git-source setting shares. Membership of this
    /// family is decided by prefix, because the repository id is part of the name.
    /// </summary>
    public const string GitSourceSettingPrefix = RepoContextGitSourceRegistry.SettingPrefix;

    /// <summary>
    /// The background-indexing cadence and retrieval-mode variables, resolved by
    /// <c>RepoContextIndexingOptions</c> and injected into the self-index grain, the
    /// approximate-index build grain and scheduler, and the bootstrap service.
    /// </summary>
    public static IReadOnlyList<string> IndexingKeys { get; } =
    [
        RepoContextIndexingOptions.TickIntervalSecondsKey,
        RepoContextIndexingOptions.ReconcileIntervalSecondsKey,
        RepoContextIndexingOptions.ReconcileJitterSecondsKey,
        RepoContextIndexingOptions.FullWalkIntervalSecondsKey,
        RepoContextIndexingOptions.EmbeddingGapScanIntervalSecondsKey,
        RepoContextIndexingOptions.CoverageDigestAuditIntervalSecondsKey,
        RepoContextIndexingOptions.VectorCacheTtlSecondsKey,
        RepoContextIndexingOptions.TokenizerProfileKey,
        RepoContextIndexingOptions.IndexingRoleKey,
        RepoContextIndexingOptions.SemanticRetrievalKey,
        RepoContextIndexingOptions.AnnIndexSchedulingKey,
        RepoContextIndexingOptions.AnnIndexReclamationKey,
        RepoContextIndexingOptions.AnnSweepIntervalSecondsKey,
    ];

    /// <summary>
    /// The two whole-host git-source variables. The per-repository settings are not
    /// listed here because they are a prefix family; see <see cref="Prefixes"/>.
    /// </summary>
    public static IReadOnlyList<string> GitSourceKeys { get; } =
    [
        RepoContextGitSourceRegistry.ReposVariable,
        RepoContextGitSourceRegistry.StagingRootVariable,
    ];

    /// <summary>
    /// The durable-memory archive variables, resolved by
    /// <c>RepoContextMemoryArchiveOptions</c> and consumed by the archive service and
    /// the startup memory-durability statement.
    /// </summary>
    public static IReadOnlyList<string> MemoryArchiveKeys { get; } =
    [
        RepoContextMemoryArchiveOptions.DirectoryKey,
        RepoContextMemoryArchiveOptions.IntervalSecondsKey,
        RepoContextMemoryArchiveOptions.RestoreKey,
        RepoContextMemoryArchiveOptions.StopTimeoutSecondsKey,
    ];

    /// <summary>
    /// Every exactly-named environment variable this package resolves.
    /// </summary>
    public static IReadOnlyList<string> All { get; } = [.. IndexingKeys, .. GitSourceKeys, .. MemoryArchiveKeys];

    /// <summary>
    /// Every prefix under which this package resolves a family of variables whose full
    /// names are not known until run time.
    /// </summary>
    public static IReadOnlyList<string> Prefixes { get; } = [GitSourceSettingPrefix];

    /// <summary>
    /// Reads every setting in <see cref="All"/> from the process environment and pairs the
    /// resolved value with the default, so a host can report what this package actually
    /// resolved rather than what the raw variable said.
    /// </summary>
    /// <remarks>
    /// The resolved value is the <b>parsed</b> one, which is what makes the report worth
    /// reading: a malformed or unrecognised value falls back to the default everywhere in
    /// this package, so reporting the raw string would state a value that nothing applied.
    /// </remarks>
    /// <returns>One snapshot per key in <see cref="All"/>, in that order.</returns>
    public static IReadOnlyList<RepoContextSettingSnapshot> DescribeResolvedSettings()
    {
        var resolved = RepoContextIndexingOptions.FromEnvironment();
        var defaults = new RepoContextIndexingOptions();
        var gitResolved = RepoContextGitSourceRegistry.FromEnvironment();
        var archiveResolved = RepoContextMemoryArchiveOptions.FromEnvironment();
        var archiveDefaults = new RepoContextMemoryArchiveOptions();

        return
        [
            Snapshot(
                RepoContextIndexingOptions.TickIntervalSecondsKey,
                Seconds(resolved.TickInterval),
                Seconds(defaults.TickInterval)),
            Snapshot(
                RepoContextIndexingOptions.ReconcileIntervalSecondsKey,
                Seconds(resolved.ReconcileInterval),
                Seconds(defaults.ReconcileInterval)),
            Snapshot(
                RepoContextIndexingOptions.ReconcileJitterSecondsKey,
                Seconds(resolved.ReconcileIntervalJitter),
                Seconds(defaults.ReconcileIntervalJitter)),
            Snapshot(
                RepoContextIndexingOptions.FullWalkIntervalSecondsKey,
                Seconds(resolved.FullWalkInterval),
                Seconds(defaults.FullWalkInterval)),
            // Both cadence deadlines are counted in reconcile passes, rounded up and
            // clamped to at least one, so the configured seconds are not the seconds in
            // force. Reported as the enforced cadence for the reason the sweep interval
            // below is: an operator shown the value they supplied would take that as
            // confirmation it applied.
            Snapshot(
                RepoContextIndexingOptions.EmbeddingGapScanIntervalSecondsKey,
                Seconds(resolved.EffectiveEmbeddingGapScanInterval),
                Seconds(defaults.EffectiveEmbeddingGapScanInterval)),
            Snapshot(
                RepoContextIndexingOptions.CoverageDigestAuditIntervalSecondsKey,
                Seconds(resolved.EffectiveCoverageDigestAuditInterval),
                Seconds(defaults.EffectiveCoverageDigestAuditInterval)),
            Snapshot(
                RepoContextIndexingOptions.VectorCacheTtlSecondsKey,
                Seconds(resolved.VectorCacheTtl),
                Seconds(defaults.VectorCacheTtl)),
            Snapshot(
                RepoContextIndexingOptions.TokenizerProfileKey,
                resolved.TokenizerProfile,
                defaults.TokenizerProfile),
            Snapshot(
                RepoContextIndexingOptions.IndexingRoleKey,
                resolved.Role.ToString(),
                defaults.Role.ToString()),
            Snapshot(
                RepoContextIndexingOptions.SemanticRetrievalKey,
                resolved.SemanticRetrieval.ToString(),
                defaults.SemanticRetrieval.ToString()),
            Snapshot(
                RepoContextIndexingOptions.AnnIndexSchedulingKey,
                resolved.AnnIndexScheduling.ToString(),
                defaults.AnnIndexScheduling.ToString()),
            Snapshot(
                RepoContextIndexingOptions.AnnIndexReclamationKey,
                resolved.AnnIndexReclamation.ToString(),
                defaults.AnnIndexReclamation.ToString()),

            // Reported as the effective value, floor applied, because that is the cadence
            // the sweep runs at. An operator who sets ten seconds and is shown ten seconds
            // here would be told the setting took, when it did not.
            Snapshot(
                RepoContextIndexingOptions.AnnSweepIntervalSecondsKey,
                Seconds(resolved.EffectiveAnnSweepInterval),
                Seconds(defaults.EffectiveAnnSweepInterval)),

            // Reported as the resolved repository count rather than the raw list: the
            // list is the opt-in, and how many repositories it actually parsed to is the
            // half an operator cannot otherwise see.
            Snapshot(
                RepoContextGitSourceRegistry.ReposVariable,
                Count(gitResolved.Sources.Count),
                Count(RepoContextGitSourceRegistry.Empty.Sources.Count)),

            // The resolved staging root, which is deliberately the default whenever no
            // repository is git-sourced: the feature is inert then, and a report stating
            // the supplied-but-unused override would claim an effect nothing had.
            Snapshot(
                RepoContextGitSourceRegistry.StagingRootVariable,
                gitResolved.StagingRoot,
                RepoContextGitSourceRegistry.Empty.StagingRoot),

            // The archive directory decides whether the feature exists at all, so it is
            // reported as enabled/disabled rather than as a path: the path is stated in
            // full by the memory-durability statement, which is also the only place that
            // can say whether it is outside the data root, and a bare path here would
            // read as a guarantee that it is.
            Snapshot(
                RepoContextMemoryArchiveOptions.DirectoryKey,
                archiveResolved.IsEnabled ? archiveResolved.Directory! : "(no archive)",
                "(no archive)"),

            // Reported as the effective cadence and budget, floors and ceilings applied,
            // because those are what run. An operator shown the value they supplied would
            // take that as confirmation it applied.
            Snapshot(
                RepoContextMemoryArchiveOptions.IntervalSecondsKey,
                Seconds(archiveResolved.EffectiveInterval),
                Seconds(archiveDefaults.EffectiveInterval)),
            Snapshot(
                RepoContextMemoryArchiveOptions.RestoreKey,
                archiveResolved.RestoreMode.ToString(),
                archiveDefaults.RestoreMode.ToString()),
            Snapshot(
                RepoContextMemoryArchiveOptions.StopTimeoutSecondsKey,
                Seconds(archiveResolved.EffectiveStopTimeout),
                Seconds(archiveDefaults.EffectiveStopTimeout)),
        ];

        static RepoContextSettingSnapshot Snapshot(string name, string resolved, string @default)
            => new(name, resolved, @default, WasDeclared(name));

        // Read from the process environment, which is the same source FromEnvironment()
        // above resolved the value from, so the declaration claim and the value it
        // qualifies cannot disagree (issue #2586). Whitespace counts as absent because
        // every reader in this package treats it that way.
        static bool WasDeclared(string name)
            => !string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(name));

        static string Seconds(TimeSpan value)
            => string.Create(CultureInfo.InvariantCulture, $"{value.TotalSeconds:0.###}s");

        static string Count(int value)
            => string.Create(CultureInfo.InvariantCulture, $"{value} repository(ies)");
    }

    /// <summary>
    /// Whether <paramref name="name"/> belongs to one of the <see cref="Prefixes"/>
    /// families this package resolves.
    /// </summary>
    /// <param name="name">The environment-variable name to test.</param>
    /// <returns><see langword="true"/> when the name is a member of a published family.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is null.</exception>
    public static bool IsPrefixMatched(string name)
    {
        ArgumentNullException.ThrowIfNull(name);

        foreach (var prefix in Prefixes)
        {
            if (name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)
                && name.Length > prefix.Length)
            {
                return true;
            }
        }

        return false;
    }
}
