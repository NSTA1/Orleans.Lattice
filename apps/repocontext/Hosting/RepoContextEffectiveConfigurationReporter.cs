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
        var unread = RepoContextEffectiveConfiguration.DescribeUnreadVariables(
            RepoContextEffectiveConfiguration.ReadProcessEnvironment(),
            KnownKeys);

        logger.LogInformation(
            "Repository-context effective configuration: {Count} setting(s), of which "
            + "{Overridden} differ(s) from this host's default; {Unread} supplied "
            + "LATTICE_ variable(s) are not read by this host. Values are as this process "
            + "resolved them, so this supersedes any configuration file when the two "
            + "disagree. Credential-bearing values are withheld by an allowlist.",
            settings.Count,
            overridden,
            unread.Count);

        foreach (var line in settings)
        {
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

        return Task.CompletedTask;
    }

    /// <summary>
    /// No-op: the reporter holds no resources.
    /// </summary>
    /// <param name="cancellationToken">Unused.</param>
    /// <returns>A completed task.</returns>
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>
    /// Every environment variable this host resolves. Used to decide which supplied
    /// <c>LATTICE_</c> variables are read by nothing.
    /// </summary>
    public static IReadOnlyList<string> KnownKeys { get; } =
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
    ];

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

            // A runtime fact rather than a setting, and reported for that reason: nothing
            // in this list would have exposed it. There is no default to compare against,
            // because the value the runtime would have chosen unaided is not observable
            // once DOTNET_PROCESSOR_COUNT has already been applied to it.
            RepoContextEffectiveConfiguration.DescribeSetting(
                RepoContextEffectiveConfiguration.RuntimeProcessorCountKey,
                Number(Environment.ProcessorCount),
                Number(Environment.ProcessorCount)),
        };

        lines.Sort(StringComparer.Ordinal);
        return lines;

        string Line(string key, Func<RepoContextHostConfiguration, string?> read)
            => RepoContextEffectiveConfiguration.DescribeSetting(key, read(resolved), read(defaults));

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

            return RepoContextEffectiveConfiguration.DescribeSetting(key, current, Number(fallback));
        }

        static string Number(int value) => value.ToString(CultureInfo.InvariantCulture);
    }
}
