using System.Text.Json;

namespace Orleans.Lattice.Explorer.EndToEnd.Hosts.Silo;

/// <summary>
/// The line feed's in-cluster half: it owns the application-side tag index
/// <see cref="IndexName"/> over <see cref="OrdersTree"/>, opened through
/// <see cref="ILatticeTagIndexFactory"/> the way a service owns its tag indexes - inside the
/// cluster, because the factory is a silo service. Each tick tags every order with its
/// current status, re-tagging only the orders whose status moved.
/// </summary>
/// <remarks>
/// It is one activation cluster-wide, driven by <see cref="LineFeedOrderIndexer"/> on every
/// silo, so it fails over with the silo that hosts it. It writes as the cluster itself
/// (system origin), as a service's own in-cluster component does.
/// </remarks>
internal interface ILineFeedOrderIndexGrain : IGrainWithStringKey
{
    /// <summary>Tags every order whose status changed since the last tick.</summary>
    /// <returns>How many orders were (re)tagged.</returns>
    Task<int> TickAsync();
}

/// <inheritdoc cref="ILineFeedOrderIndexGrain" />
internal sealed class LineFeedOrderIndexGrain(ILatticeTagIndexFactory tagIndexes) : Grain, ILineFeedOrderIndexGrain
{
    /// <summary>The orders tree the index covers.</summary>
    public const string OrdersTree = "factory-orders";

    /// <summary>The index's logical name; its membership tree is <c>tag-order-status</c>.</summary>
    public const string IndexName = "order-status";

    private readonly Dictionary<string, string> _tagged = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public async Task<int> TickAsync()
    {
        using var origin = LatticeSystemOrigin.Enter();
        var orders = GrainFactory.GetGrain<ILattice>(OrdersTree);
        if (!await orders.TreeExistsAsync())
        {
            return 0;
        }

        var index = tagIndexes.Create(orders, IndexName);
        var changed = 0;
        await foreach (var (key, value) in orders.ScanEntriesAsync("order/", "order0"))
        {
            if (StatusOf(value) is not { } status
                || (_tagged.TryGetValue(key, out var previous) && previous == status))
            {
                continue;
            }

            await index.Key(key).SetAsync([status]);
            _tagged[key] = status;
            changed++;
        }

        return changed;
    }

    private static string? StatusOf(byte[] value)
    {
        try
        {
            using var document = JsonDocument.Parse(value);
            return document.RootElement.ValueKind == JsonValueKind.Object
                && document.RootElement.TryGetProperty("status", out var status)
                && status.ValueKind == JsonValueKind.String
                    ? status.GetString()
                    : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }
}

/// <summary>
/// Drives <see cref="ILineFeedOrderIndexGrain"/> on every silo when the line feed's indexer
/// is enabled (<c>LineFeed:Indexer=true</c>); the single activation does the work.
/// </summary>
internal sealed class LineFeedOrderIndexer(IGrainFactory grains, IConfiguration configuration, ILogger<LineFeedOrderIndexer> logger)
    : BackgroundService
{
    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var interval = TimeSpan.FromSeconds(configuration.GetValue("LineFeed:IndexerIntervalSeconds", 3.0));
        using var timer = new PeriodicTimer(interval);
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            try
            {
                await grains.GetGrain<ILineFeedOrderIndexGrain>(LineFeedOrderIndexGrain.IndexName).TickAsync();
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "The line feed's order-status indexer tick failed; it retries on the next tick.");
            }
        }
    }
}
