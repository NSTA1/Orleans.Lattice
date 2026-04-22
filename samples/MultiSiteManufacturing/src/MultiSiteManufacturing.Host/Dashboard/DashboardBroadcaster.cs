using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;

namespace MultiSiteManufacturing.Host.Dashboard;

/// <summary>
/// In-process pub/sub hub that feeds the Blazor dashboard (plan §7.3).
/// Subscribes to <see cref="FederationRouter.FactRouted"/> and
/// <see cref="FederationRouter.ChaosConfigChanged"/>, derives
/// <see cref="PartSummaryUpdate"/> / <see cref="ChaosOverview"/> records,
/// and broadcasts them to every active subscriber via per-subscriber
/// <see cref="Channel{T}"/> instances.
/// </summary>
/// <remarks>
/// Components call <see cref="SubscribePartUpdates"/> /
/// <see cref="SubscribeChaosChanges"/> in <c>OnInitializedAsync</c> and
/// iterate until disposal. Back-pressure is handled by unbounded
/// channels — the sample is single-silo and the update volume is
/// modest (one fact per operator action plus seed traffic).
/// </remarks>
public sealed class DashboardBroadcaster : IHostedService, IAsyncDisposable
{
    private readonly FederationRouter _router;
    private readonly ILogger<DashboardBroadcaster> _logger;
    private readonly ConcurrentDictionary<Guid, Channel<PartSummaryUpdate>> _partSubs = new();
    private readonly ConcurrentDictionary<Guid, Channel<ChaosOverview>> _chaosSubs = new();

    /// <summary>Creates the broadcaster (DI ctor).</summary>
    public DashboardBroadcaster(FederationRouter router, ILogger<DashboardBroadcaster> logger)
    {
        _router = router;
        _logger = logger;
    }

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        _router.FactRouted += OnFactRouted;
        _router.ChaosConfigChanged += OnChaosConfigChanged;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken)
    {
        _router.FactRouted -= OnFactRouted;
        _router.ChaosConfigChanged -= OnChaosConfigChanged;
        return Task.CompletedTask;
    }

    /// <summary>
    /// Builds a fresh snapshot for every part in the lattice backend.
    /// Components call this in <c>OnInitializedAsync</c> before starting
    /// their live subscription.
    /// </summary>
    public async Task<IReadOnlyList<PartSummaryUpdate>> GetInitialPartsAsync(CancellationToken cancellationToken = default)
    {
        var lattice = _router.GetBackend("lattice");
        var serials = await ListPartsWithRetryAsync(lattice, cancellationToken);
        var results = new List<PartSummaryUpdate>(serials.Count);
        foreach (var serial in serials)
        {
            results.Add(await BuildSummaryWithRetryAsync(serial, cancellationToken));
        }
        return results;
    }

    private static async Task<IReadOnlyList<PartSerialNumber>> ListPartsWithRetryAsync(
        IFactBackend lattice,
        CancellationToken cancellationToken)
    {
        // TODO(F-034): delete this wrapper once ILattice ships resilient
        // scan iterators that transparently recover from
        // EnumerationAbortedException (tracked in roadmap.md). Today the
        // lattice backend streams via Tree.KeysAsync; the remote
        // enumerator can be reclaimed mid-scan on cold-start or
        // fan-out, and every consumer has to re-implement this retry.
        const int maxAttempts = 8;
        for (var attempt = 0; attempt < maxAttempts; attempt++)
        {
            try
            {
                return await lattice.ListPartsAsync(cancellationToken);
            }
            catch (Orleans.Runtime.EnumerationAbortedException) when (attempt < maxAttempts - 1)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(100 * (attempt + 1)), cancellationToken);
            }
        }

        throw new InvalidOperationException("ListPartsWithRetryAsync exhausted retries without returning.");
    }

    /// <summary>Reads the current chaos overview (used by the banner on initial render).</summary>
    public async Task<ChaosOverview> GetChaosOverviewAsync(CancellationToken cancellationToken = default)
    {
        var sites = await _router.ListSitesAsync();
        var backends = await _router.ListBackendChaosAsync();
        return BuildOverview(sites, backends);
    }

    /// <summary>
    /// Live feed of part-summary updates. Yields one message per
    /// <see cref="FederationRouter.FactRouted"/> event, skipping parts
    /// the caller never asked about (the UI filters client-side).
    /// </summary>
    public async IAsyncEnumerable<PartSummaryUpdate> SubscribePartUpdates(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var id = Guid.NewGuid();
        var channel = Channel.CreateUnbounded<PartSummaryUpdate>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
        });
        _partSubs[id] = channel;
        try
        {
            await foreach (var update in channel.Reader.ReadAllAsync(cancellationToken))
            {
                yield return update;
            }
        }
        finally
        {
            _partSubs.TryRemove(id, out _);
            channel.Writer.TryComplete();
        }
    }

    /// <summary>Live feed of chaos-overview updates.</summary>
    public async IAsyncEnumerable<ChaosOverview> SubscribeChaosChanges(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var id = Guid.NewGuid();
        var channel = Channel.CreateUnbounded<ChaosOverview>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
        });
        _chaosSubs[id] = channel;
        try
        {
            await foreach (var update in channel.Reader.ReadAllAsync(cancellationToken))
            {
                yield return update;
            }
        }
        finally
        {
            _chaosSubs.TryRemove(id, out _);
            channel.Writer.TryComplete();
        }
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        foreach (var sub in _partSubs.Values)
        {
            sub.Writer.TryComplete();
        }
        foreach (var sub in _chaosSubs.Values)
        {
            sub.Writer.TryComplete();
        }
        _partSubs.Clear();
        _chaosSubs.Clear();
        return ValueTask.CompletedTask;
    }

    private void OnFactRouted(object? sender, Fact fact) => _ = PublishPartAsync(fact);

    private void OnChaosConfigChanged(object? sender, EventArgs e) => _ = PublishChaosAsync();

    private async Task PublishPartAsync(Fact fact)
    {
        try
        {
            var update = await BuildSummaryWithRetryAsync(fact.Serial, CancellationToken.None);
            foreach (var sub in _partSubs.Values)
            {
                sub.Writer.TryWrite(update);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to build dashboard update for fact {FactId}", fact.FactId);
        }
    }

    /// <summary>
    /// Wraps <see cref="BuildSummaryAsync"/> with a short retry loop for
    /// <see cref="Orleans.Runtime.EnumerationAbortedException"/>. The
    /// lattice backend streams entries from a B+ tree grain; if that
    /// grain deactivates mid-enumeration (cold-start, scale-down,
    /// idle-expiry) the iterator aborts. Re-opening the enumerator
    /// recovers deterministically.
    /// </summary>
    /// <remarks>
    /// TODO(F-034): delete this wrapper once <c>ILattice</c> ships
    /// resilient scan iterators that transparently recover from
    /// <see cref="Orleans.Runtime.EnumerationAbortedException"/>
    /// (tracked in <c>roadmap.md</c>). This is generic plumbing every
    /// Lattice consumer would otherwise have to reinvent.
    /// </remarks>
    private async Task<PartSummaryUpdate> BuildSummaryWithRetryAsync(
        PartSerialNumber serial,
        CancellationToken cancellationToken)
    {
        const int maxAttempts = 8;
        for (var attempt = 0; attempt < maxAttempts; attempt++)
        {
            try
            {
                return await BuildSummaryAsync(serial, cancellationToken);
            }
            catch (Orleans.Runtime.EnumerationAbortedException) when (attempt < maxAttempts - 1)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(100 * (attempt + 1)), cancellationToken);
            }
        }

        // Unreachable — last attempt either returns or throws.
        throw new InvalidOperationException("BuildSummaryWithRetryAsync exhausted retries without returning.");
    }

    private async Task PublishChaosAsync()
    {
        try
        {
            var overview = await GetChaosOverviewAsync();
            foreach (var sub in _chaosSubs.Values)
            {
                sub.Writer.TryWrite(overview);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to build chaos overview update");
        }
    }

    private async Task<PartSummaryUpdate> BuildSummaryAsync(
        PartSerialNumber serial,
        CancellationToken cancellationToken)
    {
        var baseline = _router.GetBackend("baseline");
        var lattice = _router.GetBackend("lattice");

        // One lattice-tree enumeration per summary: fetch facts once and
        // fold them locally for the lattice state. Opening a second
        // concurrent enumerator (via lattice.GetStateAsync, which itself
        // calls GetFactsAsync) multiplies the pressure on the tree grain
        // and triggers EnumerationAbortedException under cold-start or
        // high fan-out.
        var baselineStateTask = baseline.GetStateAsync(serial, cancellationToken);
        var factsTask = lattice.GetFactsAsync(serial, cancellationToken);
        await Task.WhenAll(baselineStateTask, factsTask);

        var facts = factsTask.Result;
        var latticeState = ComplianceFold.Fold(facts);
        ProcessStage? latestStage = null;
        for (var i = facts.Count - 1; i >= 0; i--)
        {
            if (facts[i] is ProcessStepCompleted step)
            {
                latestStage = step.Stage;
                break;
            }
        }

        return new PartSummaryUpdate
        {
            Serial = serial,
            Family = InferFamily(serial),
            LatestStage = latestStage,
            BaselineState = baselineStateTask.Result,
            LatticeState = latticeState,
            FactCount = facts.Count,
        };
    }

    private static ChaosOverview BuildOverview(
        IReadOnlyList<SiteState> sites,
        IReadOnlyList<BackendChaosState> backends)
    {
        var paused = 0;
        var delayed = 0;
        var reordering = 0;
        foreach (var site in sites)
        {
            if (site.Config.IsPaused) paused++;
            if (site.Config.DelayMs > 0) delayed++;
            if (site.Config.ReorderEnabled) reordering++;
        }

        var flaky = new List<string>();
        foreach (var backend in backends)
        {
            if (backend.Config != BackendChaosConfig.Nominal)
            {
                flaky.Add(backend.Name);
            }
        }

        return new ChaosOverview
        {
            PausedSites = paused,
            DelayedSites = delayed,
            ReorderingSites = reordering,
            FlakyBackends = flaky,
        };
    }

    private static string InferFamily(PartSerialNumber serial)
    {
        var value = serial.Value;
        var lastDash = value.LastIndexOf('-');
        if (lastDash <= 0)
        {
            return value;
        }
        var yearDash = value.LastIndexOf('-', lastDash - 1);
        return yearDash > 0 ? value[..yearDash] : value;
    }
}
