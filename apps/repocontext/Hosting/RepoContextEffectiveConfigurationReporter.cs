using System.Globalization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// States the configuration this process actually resolved, once, at startup, so a
/// divergence between what the repository says and what the deployment runs is visible in
/// the log rather than undetectable.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this exists (issue #2294).</b> The container's real settings arrive from an
/// untracked compose override, so reading the repository does not tell you what the
/// process runs. The one place that divergence was ever detectable was
/// <c>RepoContextIndexingCadenceReporter</c>, which states its own subsystem's resolved
/// cadence at startup and is the sole reason anybody noticed that a tracked five second
/// reconcile interval was actually running at sixty. That reporter was written for issue
/// #2075 and covers four knobs. This one finishes the pattern for the settings this host
/// resolves, and adds the runtime facts that are not settings at all.
/// </para>
/// <para>
/// <b>It does not replace or absorb the cadence reporter.</b> That one converts wall-clock
/// knobs into the pass counts the reconcile enforces and warns when the arithmetic has
/// silently disabled pruning. Folding a flat census into it would blunt both: a derived
/// consequence with a warning is a different artefact from a list of resolved values, and
/// the census has nothing to say about pass counts.
/// </para>
/// <para>
/// <b>Why the runtime facts matter as much as the settings.</b>
/// <see cref="Environment.ProcessorCount"/> sized the oversubscribed WAL replay gate in
/// issue #2279, it reports whatever <c>DOTNET_PROCESSOR_COUNT</c> says rather than the
/// container's CPU quota, and it appeared nowhere in the process output - so establishing
/// it required inspecting the container and still could not attribute it, because
/// <c>docker inspect</c> shows the union of image and container environment. A process
/// that states its own resolved figure makes that inspection unnecessary rather than
/// merely checkable.
/// </para>
/// <para>
/// <b>Every value states its origin (issue #2586).</b> This report used to print a
/// defaulted value in exactly the shape it prints a declared one, and that cost two full
/// gate runs: a container warned that <c>LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD</c> was
/// unset and then reported <c>= 120s</c> for it seconds later, and the second line won.
/// A warning is read by whoever is watching when it scrolls past; this report is read by
/// whoever later asks what the configuration is, which is the deliberate act of somebody
/// auditing a deployment and is the audience that matters. So every line now carries a
/// provenance marker beside its value, on the same line, because a reader who greps for a
/// variable name must receive the qualification in the same result.
/// </para>
/// <para>
/// This is observability only: it reads and logs. It changes no behaviour, validates
/// nothing, and never fails startup - a malformed setting is reported as malformed and
/// left for the host's own validation to reject, because a reporter that could keep the
/// process down would be a worse defect than the invisibility it removes.
/// </para>
/// </remarks>
/// <param name="resolved">The configuration this host resolved and validated.</param>
/// <param name="configuration">The ambient configuration (environment variables).</param>
/// <param name="logger">The log sink for the startup report.</param>
public sealed class RepoContextEffectiveConfigurationReporter(
    RepoContextHostConfiguration resolved,
    IConfiguration configuration,
    ILogger<RepoContextEffectiveConfigurationReporter> logger) : IHostedService
{
    /// <summary>
    /// The collector facts this report states, defaulting to the running process's own.
    /// </summary>
    /// <remarks>
    /// Settable so that a test can exercise the hazardous combination at all. The garbage
    /// collector reads its configuration once, at process start, so a fixture cannot put
    /// its own process into Workstation GC on an 11 GiB ceiling - which is precisely the
    /// combination this report exists to name, and therefore the one that must not be
    /// covered only by inspection.
    /// </remarks>
    public RepoContextGarbageCollectionFacts GarbageCollection { get; init; }
        = RepoContextGarbageCollection.ReadRuntimeFacts();

    /// <summary>
    /// Emits the effective-configuration report.
    /// </summary>
    /// <param name="cancellationToken">Unused; the report is synchronous.</param>
    /// <returns>A completed task.</returns>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(resolved);
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(logger);

        var settings = Describe();
        var overridden = settings.Count(line => line.Contains("[OVERRIDDEN", StringComparison.Ordinal));
        var defaulted = settings.Count(line => line.Contains(
            RepoContextEffectiveConfiguration.DefaultedMarker, StringComparison.Ordinal));
        var environment = RepoContextEffectiveConfiguration.ReadProcessEnvironment();
        var unread = RepoContextEffectiveConfiguration.DescribeUnreadVariables(
            environment,
            KnownKeys,
            KnownKeyPrefixes);
        var families = RepoContextEffectiveConfiguration.DescribePrefixMatchedVariables(
            environment,
            KnownKeys,
            KnownKeyPrefixes);

        logger.LogInformation(
            "Repository-context effective configuration: {Count} setting(s), of which "
            + "{Overridden} differ(s) from this host's default and {Defaulted} were not "
            + "declared at all; {Unread} supplied LATTICE_ variable(s) are not read by "
            + "this host. Values are as this process resolved them, so this supersedes any "
            + "configuration file when the two disagree. Every value carries its origin on "
            + "its own line - a value nobody supplied is marked "
            + "'(DEFAULTED, not declared)' and must not be read as configured. "
            + "Credential-bearing values are withheld by an allowlist.",
            settings.Count,
            overridden,
            defaulted,
            unread.Count);

        // The scope statement, and not a decoration (issue #2470). Without it this
        // enumeration presents itself as the authority on effective configuration while
        // covering one input channel, so a setting it never had access to reads as a
        // setting it checked and found unset - and the silence is loudest for exactly the
        // settings nobody thought to check, which are the ones this report is worth having
        // for. Absence is only evidence when the boundary it sits inside is stated.
        logger.LogInformation(
            "Repository-context effective configuration: {Scope}",
            RepoContextEffectiveConfiguration.ScopeStatement);

        foreach (var line in settings)
        {
            logger.LogInformation("Repository-context effective configuration: {Setting}", line);
        }

        foreach (var line in families)
        {
            // Information rather than warning: a member of a published prefix family is
            // read, so it is not the #2279 failure. It is reported at all because this
            // host recognises the family and not the member, and saying so is the
            // difference between a bounded claim and the unbounded one issue #2460 was
            // filed about.
            logger.LogInformation("Repository-context effective configuration: {Setting}", line);
        }

        foreach (var line in unread)
        {
            // Warning rather than information: a variable an operator deliberately set and
            // that nothing binds is the exact silent failure of issue #2279, where a
            // compose file could name a knob, the container could carry it, and the value
            // would still never be applied.
            logger.LogWarning("Repository-context effective configuration: {Setting}", line);
        }

        foreach (var line in RepoContextGarbageCollection.DescribeHazards(GarbageCollection, configuration))
        {
            // Warning rather than information, and in this report rather than left to the
            // one the runtime already emits (issue #2596). Orleans logs
            // "Note: Silo not running with ServerGC turned on" at startup; it did so
            // through two failed gate runs and nobody read it, while the runtime separately
            // attributed 172 individual stalls to collector pauses in the same file. The
            // channel was never the problem - a true signal nobody reads is - so the
            // statement is made where an operator asking what this deployment runs will
            // already be looking.
            logger.LogWarning("Repository-context effective configuration: {Setting}", line);
        }

        ReportMemoryDurability();

        return Task.CompletedTask;
    }

    /// <summary>
    /// States where this host's durable agent memory lives and what does and does not
    /// protect it (issue #2601).
    /// </summary>
    /// <remarks>
    /// <para>
    /// Emitted at <b>warning</b> level unconditionally, because the condition it reports
    /// holds unconditionally: agent memory always shares a volume with rebuildable index
    /// state, and no configuration separates them. Downgrading the line once an archive is
    /// configured would say the risk had been removed, when what an archive removes is one
    /// consequence of it and only as far back as its last export.
    /// </para>
    /// <para>
    /// It sits in this report rather than in its own because this is where an operator
    /// asking what a deployment runs is already looking - the same reasoning that moved
    /// the collector hazards here in issue #2596.
    /// </para>
    /// </remarks>
    private void ReportMemoryDurability()
    {
        var statement = RepoContextMemoryDurabilityReport.Describe(
            resolved.DataRoot, resolved.WalDirectory, resolved.SqlitePath);

        foreach (var line in statement.Lines)
        {
            if (statement.IsWarning)
            {
                logger.LogWarning("Repository-context memory durability: {Statement}", line);
            }
            else
            {
                logger.LogInformation("Repository-context memory durability: {Statement}", line);
            }
        }
    }

    /// <summary>
    /// No-op: the reporter holds no resources.
    /// </summary>
    /// <param name="cancellationToken">Unused.</param>
    /// <returns>A completed task.</returns>
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>
    /// The keys this host's own configuration classes declare, as distinct from the ones
    /// the repository-context package publishes.
    /// </summary>
    /// <remarks>
    /// Declared <b>above</b> <see cref="KnownKeys"/> because that property spreads this
    /// one: static initialisers run in declaration order, so the reverse order silently
    /// yields a <see cref="KnownKeys"/> built from a null sequence. This is the same
    /// declaration-order hazard the repository documents for a metrics class's
    /// <c>Meter</c> field.
    /// </remarks>
    private static IReadOnlyList<string> HostKeys { get; } =
    [
        RepoContextHostConfiguration.DurabilityKey,
        RepoContextHostConfiguration.WalProviderKey,
        RepoContextHostConfiguration.GrainStorageKey,
        RepoContextHostConfiguration.RemindersKey,
        RepoContextHostConfiguration.ClusteringKey,
        RepoContextHostConfiguration.DataRootKey,
        RepoContextHostConfiguration.WalDirKey,
        RepoContextHostConfiguration.SqlitePathKey,
        RepoContextHostConfiguration.PostgresConnectionKey,
        RepoContextHostConfiguration.AzureConnectionKey,
        RepoContextHostConfiguration.AzureWalTableKey,
        RepoContextHostConfiguration.EmbeddingEndpointKey,
        RepoContextHostConfiguration.EmbeddingModelKey,
        RepoContextHostConfiguration.EmbeddingDimensionKey,
        RepoContextHostConfiguration.McpPortKey,
        RepoContextHostConfiguration.ClusterIdKey,
        RepoContextHostConfiguration.ServiceIdKey,
        RepoContextHostConfiguration.WorkspaceRootKey,
        RepoContextPinBucketing.PinBucketsKey,
        RepoContextReplayConcurrency.MaxConcurrentReplaysKey,
        RepoContextClaimLeases.MaxLockLeaseSecondsKey,
        RepoContextShutdownBudget.StopGracePeriodKey,
    ];

    /// <summary>
    /// Every environment variable this host resolves whose full name is known at compile
    /// time: this host's own keys, plus the ones the repository-context package publishes
    /// through <see cref="RepoContextEnvironmentVariables.All"/>. Used to decide which
    /// supplied <c>LATTICE_</c> variables are read by nothing.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>The package half is derived, not restated (issue #2460).</b> This list was
    /// previously hand-maintained and drawn only from this host's own configuration
    /// classes, so eleven variables the repository-context package reads were absent from
    /// it and every one of them was reported <c>[SUPPLIED BUT NOT READ BY THIS HOST]</c> -
    /// the inversion of the truth, in a report whose tone invites trust. An operator was
    /// told a throttle they had deliberately set was inert while it was visibly steering
    /// the runtime.
    /// </para>
    /// <para>
    /// The failure was not that somebody forgot a key. It was that this list asserted its
    /// own exhaustiveness over a surface it had no access to, and the guard test that
    /// checked the assertion scanned only this assembly - so the claim was broader than
    /// the check, and the gap between them was invisible from either side. Adding the
    /// missing keys by hand would have fixed the eleven and left that mechanism intact.
    /// </para>
    /// <para>
    /// <b>Names known only at run time are not here.</b> See
    /// <see cref="KnownKeyPrefixes"/>.
    /// </para>
    /// </remarks>
    public static IReadOnlyList<string> KnownKeys { get; } =
    [
        .. HostKeys,
        .. RepoContextEnvironmentVariables.All,
    ];

    /// <summary>
    /// Every prefix under which this host resolves a family of variables whose full names
    /// are only known at run time, because part of the name is a repository id.
    /// </summary>
    /// <remarks>
    /// A member of one of these families is read, so reporting it as unread would be the
    /// same defect <see cref="KnownKeys"/> documents. It is reported as prefix-matched
    /// instead of silently dropped, because prefix membership is a weaker claim than
    /// name-for-name recognition: a mistyped setting inside a recognised family is still a
    /// setting nothing binds, and quietly counting it as read would replace one
    /// unsupported claim with another.
    /// </remarks>
    public static IReadOnlyList<string> KnownKeyPrefixes { get; } =
        RepoContextEnvironmentVariables.Prefixes;

    /// <summary>
    /// Builds the report lines by pairing each resolved value with the value this host
    /// reaches when nothing is supplied. The defaults are <b>derived</b>, by resolving the
    /// same configuration classes against an empty configuration, so this report can never
    /// disagree with the code that applies them.
    /// </summary>
    /// <returns>The rendered setting lines, ordered by name.</returns>
    private IReadOnlyList<string> Describe()
    {
        var empty = new ConfigurationBuilder().Build();
        var defaults = RepoContextHostConfiguration.FromConfiguration(empty);

        var lines = new List<string>
        {
            Line(RepoContextHostConfiguration.DurabilityKey, c => c.Profile.ToString()),
            Line(RepoContextHostConfiguration.WalProviderKey, c => c.Wal.ToString()),
            Line(RepoContextHostConfiguration.GrainStorageKey, c => c.GrainStorage.ToString()),
            Line(RepoContextHostConfiguration.RemindersKey, c => c.Reminders.ToString()),
            Line(RepoContextHostConfiguration.ClusteringKey, c => c.Clustering.ToString()),
            Line(RepoContextHostConfiguration.DataRootKey, c => c.DataRoot),
            Line(RepoContextHostConfiguration.WalDirKey, c => c.WalDirectory),
            Line(RepoContextHostConfiguration.SqlitePathKey, c => c.SqlitePath),
            Line(RepoContextHostConfiguration.PostgresConnectionKey, c => c.PostgresConnectionString),
            Line(RepoContextHostConfiguration.AzureConnectionKey, c => c.AzureConnectionString),
            Line(RepoContextHostConfiguration.AzureWalTableKey, c => c.AzureWalTableName),
            Line(RepoContextHostConfiguration.EmbeddingEndpointKey, c => c.EmbeddingEndpoint.ToString()),
            Line(RepoContextHostConfiguration.EmbeddingModelKey, c => c.EmbeddingModel),
            Line(RepoContextHostConfiguration.EmbeddingDimensionKey, c => Number(c.EmbeddingDimension)),
            Line(RepoContextHostConfiguration.McpPortKey, c => Number(c.McpPort)),
            Line(RepoContextHostConfiguration.ClusterIdKey, c => c.ClusterId),
            Line(RepoContextHostConfiguration.ServiceIdKey, c => c.ServiceId),
            Line(RepoContextHostConfiguration.WorkspaceRootKey, c => c.WorkspaceRoot),

            Knob(
                RepoContextPinBucketing.PinBucketsKey,
                RepoContextPinBucketing.ResolveBucketCount,
                RepoContextPinBucketing.DefaultPinBuckets),
            Knob(
                RepoContextReplayConcurrency.MaxConcurrentReplaysKey,
                RepoContextReplayConcurrency.ResolveMaxConcurrentReplays,
                RepoContextReplayConcurrency.DefaultMaxConcurrentReplays),
            Knob(
                RepoContextClaimLeases.MaxLockLeaseSecondsKey,
                RepoContextClaimLeases.ResolveMaxLeaseSeconds,
                RepoContextClaimLeases.DefaultMaxLockLeaseSeconds),

            // Reported as the declared GRANT rather than as the budget derived from it.
            // The grant is the value an operator sets, and it is the half of the pair
            // this process can actually read - the real stop_grace_period is invisible
            // from inside the container - so a report naming only the derived budget
            // would not answer the question this report exists for: which value did the
            // running process believe.
            DescribeGrant(),

            // A runtime fact rather than a setting, and reported for that reason: nothing
            // in this list would have exposed it. There is no default to compare against,
            // because the value the runtime would have chosen unaided is not observable
            // once DOTNET_PROCESSOR_COUNT has already been applied to it. It carries an
            // explicit RUNTIME FACT marker rather than none: once every setting is
            // qualified, an unqualified line reads as declared, so silence here would
            // reintroduce issue #2586 on the one line that is not a setting at all.
            RepoContextEffectiveConfiguration.DescribeSetting(
                RepoContextEffectiveConfiguration.RuntimeProcessorCountKey,
                Number(Environment.ProcessorCount),
                Number(Environment.ProcessorCount),
                RepoContextSettingProvenance.Runtime),
        };

        // The collector this process actually runs under (issue #2596). Both the declared
        // variables and the resolved facts, because they disagree under two separate
        // mechanisms and only the resolved half says what the process does.
        lines.AddRange(RepoContextGarbageCollection.DescribeSettings(GarbageCollection, configuration));

        // The repository-context package's own settings, resolved by the package rather
        // than by this host (issue #2460 half two). Recognising them in KnownKeys stops
        // them being reported as unread; without a value line here they would instead
        // vanish from the report altogether, which is strictly worse for an operator than
        // being wrongly labelled - a wrong label is at least visible.
        foreach (var setting in RepoContextEnvironmentVariables.DescribeResolvedSettings())
        {
            lines.Add(RepoContextEffectiveConfiguration.DescribeSetting(
                setting.Name,
                setting.Resolved,
                setting.Default,
                setting.WasDeclared
                    ? RepoContextSettingProvenance.Declared
                    : RepoContextSettingProvenance.Defaulted));
        }

        lines.Sort(StringComparer.Ordinal);
        return lines;

        string Line(string key, Func<RepoContextHostConfiguration, string?> read)
            => RepoContextEffectiveConfiguration.DescribeSetting(
                key,
                read(resolved),
                read(defaults),
                Provenance(key));

        // Probed from the configuration the value itself was resolved from, and separately
        // from the resolved-against-default comparison that produces [OVERRIDDEN] (issue
        // #2586). The two are orthogonal: a key nothing declared can resolve away from the
        // pristine default because a neighbouring key moved it, and a key an operator
        // declared can resolve to exactly the default - the case that produced the issue.
        RepoContextSettingProvenance Provenance(string key)
            => RepoContextEffectiveConfiguration.ProvenanceOf(configuration[key]);

        string Knob(string key, Func<IConfiguration, int> read, int fallback)
        {
            // The host's own call site validates and throws; this reporter must not, so a
            // malformed value is reported as malformed and the failure is left to the
            // component that owns it.
            string current;
            try
            {
                current = Number(read(configuration));
            }
            catch (InvalidOperationException ex)
            {
                current = string.Create(CultureInfo.InvariantCulture, $"<invalid: {ex.Message}>");
            }

            return RepoContextEffectiveConfiguration.DescribeSetting(
                key,
                current,
                Number(fallback),
                Provenance(key));
        }

        static string Number(int value) => value.ToString(CultureInfo.InvariantCulture);

        string DescribeGrant()
        {
            // Same discipline as Knob: the host's own call site validates and throws, so
            // this reporter reports a malformed value as malformed rather than taking the
            // process down from inside the diagnostic that was supposed to explain it.
            string current;
            RepoContextSettingProvenance provenance;
            try
            {
                var resolution = RepoContextShutdownBudget.Resolve(configuration);
                current = Seconds(resolution.StopGracePeriod);

                // The flag the resolver already computes, and that this report used to
                // discard - which is the whole of issue #2586. Taken from the resolution
                // rather than re-derived from the configuration so that this line cannot
                // disagree with the startup warning built from the same resolution.
                provenance = resolution.GrantWasDeclared
                    ? RepoContextSettingProvenance.Declared
                    : RepoContextSettingProvenance.Defaulted;
            }
            catch (InvalidOperationException ex)
            {
                current = string.Create(CultureInfo.InvariantCulture, $"<invalid: {ex.Message}>");

                // Declared, not defaulted: the resolver only parses, and so only throws,
                // when a value was actually supplied. Reporting a rejected declaration as
                // defaulted would tell an operator nobody set the variable they are
                // staring at in their own compose file.
                provenance = RepoContextSettingProvenance.Declared;
            }

            return RepoContextEffectiveConfiguration.DescribeSetting(
                RepoContextShutdownBudget.StopGracePeriodKey,
                current,
                Seconds(RepoContextShutdownBudget.DefaultStopGracePeriod),
                provenance);
        }

        static string Seconds(TimeSpan value)
            => string.Create(CultureInfo.InvariantCulture, $"{value.TotalSeconds:0.###}s");
    }
}
