using Microsoft.Extensions.Options;
using Orleans.Lattice.GrainIndex.Backfill;
using Orleans.Lattice.GrainIndex.Registry;

namespace Orleans.Lattice.GrainIndex.Observability;

/// <summary>
/// The default <see cref="IGrainIndexAdmin"/>: a silo-side service that reads
/// every figure it reports from the index-registry system tree and delegates
/// every control to the index's own backfill activation.
/// </summary>
/// <remarks>
/// <para>
/// It owns no state of its own on purpose. The declaration set comes from
/// options, the stored record and checkpoint from the registry, and the crawl
/// controls from the one activation that owns the crawl cluster-wide - so there
/// is no second bookkeeping store to keep in step, and two silos asked the same
/// question give the same answer.
/// </para>
/// <para>
/// Everything is resolved once in the constructor: the declaration list is
/// snapshotted into a name-keyed lookup and a declaration-ordered name list, so
/// a status call does no scanning to find its index. Nothing here is on a hot
/// path - an administrative call is an operator action - but the lookup also
/// keeps the "unknown index" failure exact rather than approximate.
/// </para>
/// <para>
/// Every await continues on its original context. The surface is a plain
/// silo-side service, so a host is free to call it from inside a grain turn, and
/// a continuation resumed on the thread pool would no longer be holding the
/// ambient Orleans runtime context the calls below run under.
/// </para>
/// </remarks>
internal sealed class GrainIndexAdmin : IGrainIndexAdmin
{
    private readonly Dictionary<string, IGrainIndexDefinition> _definitions;
    private readonly string[] _names;
    private readonly IOptionsMonitor<GrainIndexOptions> _options;
    private readonly IGrainIndexRegistryStore _registry;
    private readonly IGrainKeySourceResolver _keySources;
    private readonly IGrainFactory _grainFactory;

    /// <summary>Initialises the administrative surface.</summary>
    /// <param name="declarations">The silo's declared indexes. Must not be <c>null</c>.</param>
    /// <param name="options">The per-index options monitor. Must not be <c>null</c>.</param>
    /// <param name="registry">The index registry store. Must not be <c>null</c>.</param>
    /// <param name="keySources">Resolves an index's key source, for the population bound. Must not be <c>null</c>.</param>
    /// <param name="grainFactory">Resolves index trees and backfill activations. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public GrainIndexAdmin(
        IOptions<GrainIndexDeclarationOptions> declarations,
        IOptionsMonitor<GrainIndexOptions> options,
        IGrainIndexRegistryStore registry,
        IGrainKeySourceResolver keySources,
        IGrainFactory grainFactory)
    {
        ArgumentNullException.ThrowIfNull(declarations);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(keySources);
        ArgumentNullException.ThrowIfNull(grainFactory);

        var declared = declarations.Value.Definitions;
        _definitions = new Dictionary<string, IGrainIndexDefinition>(declared.Count, StringComparer.Ordinal);
        var names = new List<string>(declared.Count);
        for (var i = 0; i < declared.Count; i++)
        {
            var definition = declared[i];

            // A duplicate name is already rejected by the declaration validator,
            // so the last-one-wins here can only be reached by a host that
            // bypassed validation. Keeping the first keeps the name list and the
            // lookup in step either way.
            if (_definitions.TryAdd(definition.Name, definition))
                names.Add(definition.Name);
        }

        _names = [.. names];
        _options = options;
        _registry = registry;
        _keySources = keySources;
        _grainFactory = grainFactory;
    }

    /// <inheritdoc />
    public IReadOnlyList<string> DeclaredIndexes => _names;

    /// <inheritdoc />
    public Task<GrainIndexStatus> GetStatusAsync(string indexName, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        return BuildStatusAsync(Require(indexName), cancellationToken);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<GrainIndexStatus>> ListStatusAsync(
        CancellationToken cancellationToken = default)
    {
        var statuses = new GrainIndexStatus[_names.Length];
        for (var i = 0; i < _names.Length; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            statuses[i] = await BuildStatusAsync(_definitions[_names[i]], cancellationToken)
                .ConfigureAwait(true);
        }

        return statuses;
    }

    /// <inheritdoc />
    public Task<GrainIndexBackfillStatus> PauseBackfillAsync(
        string indexName,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        cancellationToken.ThrowIfCancellationRequested();
        return Backfill(Require(indexName).Name).PauseAsync();
    }

    /// <inheritdoc />
    public Task<GrainIndexBackfillStatus> ResumeBackfillAsync(
        string indexName,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        cancellationToken.ThrowIfCancellationRequested();
        return Backfill(Require(indexName).Name).ResumeAsync();
    }

    /// <inheritdoc />
    public Task<GrainIndexBackfillStatus> RebuildAsync(
        string indexName,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        cancellationToken.ThrowIfCancellationRequested();

        // A rebuild is the crawl restart the drift gate already schedules, so it
        // delegates to that same primitive rather than reimplementing it: the
        // restart runs under the registry's current fingerprint and re-visits
        // grains the index already records.
        return Backfill(Require(indexName).Name).RestartAsync();
    }

    /// <inheritdoc />
    public Task<GrainIndexBackfillBatchResult> RunBackfillPassAsync(
        string indexName,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        cancellationToken.ThrowIfCancellationRequested();
        return Backfill(Require(indexName).Name).RunBatchAsync();
    }

    private async Task<GrainIndexStatus> BuildStatusAsync(
        IGrainIndexDefinition definition,
        CancellationToken cancellationToken)
    {
        var indexName = definition.Name;
        var options = _options.Get(indexName);
        var descriptor = definition.Describe(options);
        var keyCodecId = GrainIndexKeyCodecIdentity.For(definition.KeyCodec);

        var record = await _registry.ReadAsync(indexName, cancellationToken).ConfigureAwait(true);
        var backfill = await Backfill(indexName).GetStatusAsync().ConfigureAwait(true);
        var total = await ApproximateTotalAsync(indexName, cancellationToken).ConfigureAwait(true);
        var entryCount = await EntryCountAsync(options.TreeName, cancellationToken).ConfigureAwait(true);

        var drift = record is null
            ? GrainIndexDriftStatus.None
            : ToDriftStatus(GrainIndexDriftDetector.Detect(record, descriptor, keyCodecId));

        var progress = new GrainIndexProgress(
            backfill.Visited,
            total,
            GrainIndexBackfillProgressRegistry.PercentComplete(backfill.State, backfill.Visited, total),
            backfill.ResumeAfterKey,
            backfill.FailureMessage);

        return new GrainIndexStatus(
            indexName,
            record?.Descriptor ?? descriptor,
            record is not null,
            record?.Fingerprint ?? default,
            record?.KeyCodecId ?? keyCodecId,
            record?.NeedsBackfill ?? false,
            drift,
            backfill,
            progress,
            entryCount);
    }

    /// <summary>
    /// The population bound the index's key source can offer, or <c>null</c>
    /// when it has no key source or the source cannot bound itself.
    /// </summary>
    private async Task<long?> ApproximateTotalAsync(string indexName, CancellationToken cancellationToken)
    {
        var keySource = _keySources.Resolve(indexName);
        if (keySource is null)
            return null;

        // A key source is application code, and a bound is a convenience rather
        // than a contract: one that throws must not take the whole status report
        // down with it.
        try
        {
            return await keySource.TryGetApproximateCountAsync(cancellationToken).ConfigureAwait(true);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }
    }

    /// <summary>The number of entries the index's backing tree holds.</summary>
    private async Task<long> EntryCountAsync(string treeName, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(treeName))
            return 0;

        return await _grainFactory.GetGrain<ILattice>(treeName).CountAsync(cancellationToken)
            .ConfigureAwait(true);
    }

    private static GrainIndexDriftStatus ToDriftStatus(GrainIndexDriftReport report) =>
        report.HasDrift ? new GrainIndexDriftStatus(report.ChangedFields) : GrainIndexDriftStatus.None;

    private IGrainIndexBackfillGrain Backfill(string indexName) =>
        _grainFactory.GetGrain<IGrainIndexBackfillGrain>(indexName);

    private IGrainIndexDefinition Require(string indexName) =>
        _definitions.TryGetValue(indexName, out var definition)
            ? definition
            : throw new GrainIndexNotDeclaredException(indexName, _names);
}
