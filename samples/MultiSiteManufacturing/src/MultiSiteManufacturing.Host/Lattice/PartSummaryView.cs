using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;
using MultiSiteManufacturing.Host.Dashboard;
using MultiSiteManufacturing.Host.Domain;
using Orleans.Lattice;

namespace MultiSiteManufacturing.Host.Lattice;

/// <summary>
/// Materialised per-part dashboard summary, kept in its own Orleans.Lattice
/// B+ tree (<see cref="TreeId"/>) with exactly one row per part keyed by the
/// part serial. Each row is the pre-folded
/// <see cref="PartSummaryUpdate"/> (latest stage, baseline / lattice
/// compliance state, fact count) so the dashboard's initial-state query is a
/// single contiguous scan of ~one row per part instead of a per-part prefix
/// scan that re-folds the whole <c>mfg-facts</c> tree on every render.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why a materialised view.</b> The compliance summary is an
/// order-dependent fold (<see cref="ComplianceFold"/>) over <em>all</em> of a
/// part's facts, so it can't be expressed with the library's built-in scalar
/// aggregation or injective projection view shapes. Instead the sample
/// maintains the summary itself: whenever a part's facts change, the
/// dashboard broadcaster re-folds just that one part and upserts its row here.
/// Reads (the dashboard snapshot, the divergence seed) then hit this compact
/// tree once rather than folding every part on every render - the
/// idle-CPU scan-storm that motivated this view.
/// </para>
/// <para>
/// The view is cluster-local and derived: each region maintains its own copy
/// from its own routed + replicated facts, so it never needs cross-cluster
/// replication. A stale or missing row is self-healing - the next fact for
/// the part (or the snapshot bootstrap on an empty tree) rewrites it.
/// </para>
/// </remarks>
public sealed class PartSummaryView(
    IGrainFactory grainFactory,
    ILogger<PartSummaryView> logger,
    string treeId = PartSummaryView.TreeId)
{
    /// <summary>Default Lattice tree id holding the per-part dashboard summary rows.</summary>
    public const string TreeId = "mfg-part-summary";

    private static readonly JsonSerializerOptions Options = new()
    {
        WriteIndented = false,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        Converters =
        {
            new JsonStringEnumConverter(JsonNamingPolicy.CamelCase),
        },
    };

    private ILattice Tree => grainFactory.GetGrain<ILattice>(treeId);

    /// <summary>
    /// Upserts the materialised summary row for one part. Called from the
    /// broadcaster's coalesced rebuild loop after it re-folds a dirty part,
    /// so the row reflects the part's latest folded state. Keyed by the part
    /// serial, so repeated upserts for the same part overwrite in place -
    /// the tree holds exactly one row per part.
    /// </summary>
    public async Task UpsertAsync(PartSummaryUpdate summary, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(summary);

        var payload = Encode(summary);
        await Tree.SetAsync(summary.Serial.Value, payload, cancellationToken);

        logger.LogDebug(
            "Materialised summary row for {Serial} ({Bytes} bytes)",
            summary.Serial.Value,
            payload.Length);
    }

    /// <summary>
    /// Upserts a batch of materialised summary rows in a single
    /// <see cref="ILattice.SetManyAsync(List{KeyValuePair{string, byte[]}}, CancellationToken)"/>
    /// call. Called from the broadcaster's coalesced rebuild loop, which folds
    /// a whole window of dirty parts and flushes them together: one batched
    /// write lets the WAL layer pack the rows into far fewer Azure Table
    /// transactions than one <see cref="UpsertAsync"/> (single
    /// <c>SetAsync</c>) per part - the dominant durable-write cost when a bulk
    /// seed marks thousands of parts dirty at once. Keyed by part serial, so
    /// repeated rows for the same part overwrite in place. A <c>null</c> or
    /// empty batch is a no-op.
    /// </summary>
    public async Task UpsertManyAsync(IReadOnlyList<PartSummaryUpdate> summaries, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(summaries);
        if (summaries.Count == 0)
        {
            return;
        }

        var entries = new List<KeyValuePair<string, byte[]>>(summaries.Count);
        foreach (var summary in summaries)
        {
            ArgumentNullException.ThrowIfNull(summary);
            entries.Add(new KeyValuePair<string, byte[]>(summary.Serial.Value, Encode(summary)));
        }

        await Tree.SetManyAsync(entries, cancellationToken);

        logger.LogDebug("Materialised {Count} summary rows in one batch", entries.Count);
    }

    /// <summary>
    /// Reads every materialised summary row in a single contiguous tree scan
    /// and rehydrates the <see cref="PartSummaryUpdate"/> list. Returns an
    /// empty list when the view has not been populated yet (cold start before
    /// any facts have been folded), in which case the caller bootstraps it.
    /// </summary>
    public async Task<IReadOnlyList<PartSummaryUpdate>> ReadAllAsync(CancellationToken cancellationToken = default)
    {
        var rows = new List<PartSummaryUpdate>();
        await foreach (var kvp in Tree.ScanEntriesAsync(cancellationToken: cancellationToken))
        {
            rows.Add(Decode(new PartSerialNumber(kvp.Key), kvp.Value));
        }
        return rows;
    }

    private static byte[] Encode(PartSummaryUpdate summary) =>
        JsonSerializer.SerializeToUtf8Bytes(
            new Row(
                summary.Family,
                summary.LatestStage,
                summary.BaselineState,
                summary.LatticeState,
                summary.FactCount),
            Options);

    private static PartSummaryUpdate Decode(PartSerialNumber serial, byte[] payload)
    {
        var row = JsonSerializer.Deserialize<Row>(payload, Options)
            ?? throw new InvalidOperationException($"Decoded null summary row for {serial.Value}.");
        return new PartSummaryUpdate
        {
            Serial = serial,
            Family = row.Family,
            LatestStage = row.LatestStage,
            BaselineState = row.BaselineState,
            LatticeState = row.LatticeState,
            FactCount = row.FactCount,
        };
    }

    /// <summary>
    /// Compact on-disk shape of a summary row. The serial is the tree key, so
    /// it is not repeated in the value.
    /// </summary>
    private sealed record Row(
        string Family,
        ProcessStage? LatestStage,
        ComplianceState BaselineState,
        ComplianceState LatticeState,
        int FactCount);
}
