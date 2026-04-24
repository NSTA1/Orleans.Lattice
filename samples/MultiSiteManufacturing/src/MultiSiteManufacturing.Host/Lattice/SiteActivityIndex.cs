using System.Globalization;
using System.Text;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;

namespace MultiSiteManufacturing.Host.Lattice;

/// <summary>
/// Maintains a lightweight secondary index on top of
/// <see cref="ILattice"/>: every <see cref="Fact"/> routed through
/// <see cref="FederationRouter"/> emits one entry keyed
/// <c>{site}/{wallTicks:D20}/{counter:D10}/{serial}</c> whose value
/// is a short UTF-8 activity label (e.g. "Step: Machining",
/// "Inspection: CMM Pass", "MRB: UseAsIs"). The index tree is a pure
/// B+ tree demo — its sole purpose is to answer "which parts are at
/// site X right now?" via a native descending range scan over the
/// site prefix.
/// </summary>
/// <remarks>
/// <para>
/// Every fact inherits <see cref="Fact.Site"/>, so every fact type is
/// a candidate — including inspections at sites like
/// <c>StuttgartCmmLab</c> that never emit a
/// <see cref="ProcessStepCompleted"/>. That is what lets the "Stuttgart
/// CMM Lab" panel actually list its CMM inspections instead of
/// appearing permanently empty.
/// </para>
/// <para>
/// Keys embed the fact's zero-padded HLC so a <c>reverse: true</c>
/// lexicographic scan natively yields HLC-descending
/// (most-recent-first) order — no in-memory sort needed. The serial
/// is appended last so two facts recorded at the same instant don't
/// collide. <see cref="ListAtSiteAsync"/> dedups by serial, keeping
/// only the most recent activity per part.
/// </para>
/// <para>
/// Every silo runs its own copy of the hosted service, so both silo A
/// and silo B append to the shared index tree (the lattice tree
/// itself is cluster-wide). Write failures are logged and swallowed —
/// a transient storage hiccup on the index must not break the main
/// fact pipeline.
/// </para>
/// </remarks>
public sealed class SiteActivityIndex(
    IGrainFactory grainFactory,
    FederationRouter router,
    ILogger<SiteActivityIndex> logger) : IHostedService
{
    /// <summary>Lattice tree id that holds the site-activity index.</summary>
    public const string TreeId = "mfg-site-activity-index";

    private ILattice Tree => grainFactory.GetGrain<ILattice>(TreeId);

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        router.FactRouted += OnFactRouted;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken)
    {
        router.FactRouted -= OnFactRouted;
        return Task.CompletedTask;
    }

    /// <summary>
    /// Returns every part that has recent activity at
    /// <paramref name="site"/>, ordered <b>most-recent first</b> by
    /// the fact's hybrid logical clock, with one row per part (the
    /// latest activity for that part at that site). Implemented as a
    /// native descending <see cref="ILattice"/> range scan over the
    /// <c>{site}/</c> prefix (the core M12d demo) — the zero-padded
    /// HLC embedded in each key means a reverse lexicographic scan is
    /// already HLC-descending; no post-sort is required.
    /// </summary>
    public async Task<IReadOnlyList<SiteActivityIndexEntry>> ListAtSiteAsync(
        ProcessSite site, CancellationToken cancellationToken = default)
    {
        var (start, end) = SitePrefixRange(site);
        var entries = new List<SiteActivityIndexEntry>();
        var seen = new HashSet<PartSerialNumber>();
        await foreach (var kvp in Tree.ScanEntriesAsync(start, end, reverse: true, cancellationToken: cancellationToken))
        {
            if (TryParseEntry(kvp.Key, kvp.Value, out var entry) && seen.Add(entry.Serial))
            {
                entries.Add(entry);
            }
        }
        return entries;
    }

    /// <summary>
    /// Writes the index entry for <paramref name="fact"/>. Called
    /// internally by <see cref="OnFactRouted"/>; exposed publicly so
    /// tests can append without setting up the full federation router.
    /// </summary>
    public Task AppendAsync(Fact fact, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fact);
        var key = KeyFor(fact);
        var value = Encoding.UTF8.GetBytes(DescribeActivity(fact));
        return Tree.SetAsync(key, value, cancellationToken);
    }

    private void OnFactRouted(object? sender, Fact fact)
    {
        // Fire-and-forget write. The index is best-effort; we don't
        // want to block the router's fan-out on storage latency here.
        _ = SafeAppendAsync(fact);
    }

    private async Task SafeAppendAsync(Fact fact)
    {
        try
        {
            await AppendAsync(fact);
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "SiteActivityIndex failed to append for {Serial}/{Site}",
                fact.Serial, fact.Site);
        }
    }

    /// <summary>
    /// Short, human-readable label describing what happened at a
    /// site — these render directly into the parts-by-site grid.
    /// Exposed so the dashboard broadcaster can build a
    /// <see cref="SiteActivityIndexEntry"/> for a live fact without
    /// round-tripping through the lattice (the range scan writes the
    /// same label under <see cref="AppendAsync"/>).
    /// </summary>
    public static string DescribeActivity(Fact fact) => fact switch
    {
        ProcessStepCompleted s => $"Step: {s.Stage}",
        InspectionRecorded i => $"Inspection: {i.Inspection} {i.Outcome}",
        NonConformanceRaised n => $"NCR {n.NcNumber} ({n.Severity})",
        MrbDisposition m => $"MRB: {m.Disposition} ({m.NcNumber})",
        ReworkCompleted => "Rework complete",
        FinalAcceptance => "FAI accepted",
        _ => fact.GetType().Name,
    };

    private static string KeyFor(Fact fact) =>
        string.Create(CultureInfo.InvariantCulture,
            $"{fact.Site}/{fact.Hlc.WallClockTicks:D20}/{fact.Hlc.Counter:D10}/{fact.Serial.Value}");

    private static (string Start, string EndExclusive) SitePrefixRange(ProcessSite site)
    {
        var prefix = site + "/";
        var end = site + "0";
        return (prefix, end);
    }

    private static bool TryParseEntry(string key, byte[] value, out SiteActivityIndexEntry entry)
    {
        // {site}/{wallTicks:D20}/{counter:D10}/{serial}
        var parts = key.Split('/', 4);
        if (parts.Length != 4
            || !Enum.TryParse<ProcessSite>(parts[0], out var site)
            || !long.TryParse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var wallTicks)
            || !int.TryParse(parts[2], NumberStyles.Integer, CultureInfo.InvariantCulture, out var counter))
        {
            entry = default;
            return false;
        }
        var hlc = new HybridLogicalClock { WallClockTicks = wallTicks, Counter = counter };
        var activity = value is { Length: > 0 } ? Encoding.UTF8.GetString(value) : string.Empty;
        entry = new SiteActivityIndexEntry(site, new PartSerialNumber(parts[3]), hlc, activity);
        return true;
    }
}

/// <summary>
/// One row from <see cref="SiteActivityIndex.ListAtSiteAsync"/> — the
/// site, the part involved, the HLC at which the activity occurred
/// (used for most-recent-first ordering and relative-time display),
/// and a short label describing what happened.
/// </summary>
public readonly record struct SiteActivityIndexEntry(
    ProcessSite Site,
    PartSerialNumber Serial,
    HybridLogicalClock Hlc,
    string Activity);

