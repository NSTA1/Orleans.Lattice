using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.GrainIndex.Backfill;

/// <summary>
/// Starts or resumes each declared index's background backfill at silo start.
/// </summary>
/// <remarks>
/// <para>
/// It runs after the registry reconciler, which is what decides whether an index
/// owes a backfill at all: a first declaration and an accepted rebuild both
/// raise that flag, and this service is what acts on it. Asking the backfill
/// grain to start is idempotent, so every silo in a cluster may do it.
/// </para>
/// <para>
/// It deliberately does not block start-up and never lets an exception escape.
/// A backfill that could not be started is a slower index, not a broken silo,
/// and the crawl is durable: the next silo to start picks it up unchanged.
/// </para>
/// <para>
/// An index whose <see cref="GrainIndexOptions.BackfillEnabled"/> is off, or
/// which has no <see cref="IGrainKeySource"/> registered, is skipped. Neither is
/// an error: the activation path still indexes every grain that is used, and
/// only the crawl over dormant grains is unavailable.
/// </para>
/// </remarks>
internal sealed class GrainIndexBackfillHostedService : IHostedService
{
    private readonly IOptions<GrainIndexDeclarationOptions> _declarations;
    private readonly IOptionsMonitor<GrainIndexOptions> _indexOptions;
    private readonly IGrainKeySourceResolver _keySources;
    private readonly IGrainFactory _grainFactory;
    private readonly ILogger<GrainIndexBackfillHostedService> _logger;

    /// <summary>Initialises the service.</summary>
    /// <param name="declarations">The declared index set. Must not be <c>null</c>.</param>
    /// <param name="indexOptions">The per-index options, resolved by index name. Must not be <c>null</c>.</param>
    /// <param name="keySources">Resolves each index's key source. Must not be <c>null</c>.</param>
    /// <param name="grainFactory">Addresses the per-index backfill grains. Must not be <c>null</c>.</param>
    /// <param name="logger">Reports indexes that could not be started. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public GrainIndexBackfillHostedService(
        IOptions<GrainIndexDeclarationOptions> declarations,
        IOptionsMonitor<GrainIndexOptions> indexOptions,
        IGrainKeySourceResolver keySources,
        IGrainFactory grainFactory,
        ILogger<GrainIndexBackfillHostedService> logger)
    {
        ArgumentNullException.ThrowIfNull(declarations);
        ArgumentNullException.ThrowIfNull(indexOptions);
        ArgumentNullException.ThrowIfNull(keySources);
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(logger);

        _declarations = declarations;
        _indexOptions = indexOptions;
        _keySources = keySources;
        _grainFactory = grainFactory;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var definitions = _declarations.Value.Definitions;
        for (var i = 0; i < definitions.Count; i++)
        {
            var indexName = definitions[i].Name;
            if (!_indexOptions.Get(indexName).BackfillEnabled)
                continue;

            if (_keySources.Resolve(indexName) is null)
            {
                _logger.LogInformation(
                    "Grain index '{IndexName}' has no {KeySource} registered, so no background backfill is "
                    + "started for it; grains are indexed as they activate.",
                    indexName,
                    nameof(IGrainKeySource));
                continue;
            }

            try
            {
                var status = await _grainFactory
                    .GetGrain<IGrainIndexBackfillGrain>(indexName)
                    .EnsureStartedAsync()
                    .ConfigureAwait(false);

                _logger.LogInformation(
                    "The background backfill for grain index '{IndexName}' is {State}.",
                    indexName,
                    status.State);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "The background backfill for grain index '{IndexName}' could not be started; it will be "
                    + "picked up by the next host that starts.",
                    indexName);
            }
        }
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
