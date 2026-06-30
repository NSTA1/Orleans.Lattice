using System.Globalization;
using System.Text;
using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using Orleans.Lattice;

namespace MultiSiteManufacturing.Host.Lattice;

/// <summary>
/// Maintains a "which parts are at site X?" view on top of the
/// <see cref="ILatticeTagIndex">tag index</see>. Every <see cref="Fact"/>
/// routed through <see cref="FederationRouter"/> writes one row to a
/// <i>part-major</i> subject tree keyed <c>{serial}/{site}</c> whose
/// value carries the fact's HLC and a short activity label (e.g.
/// "Step: Machining", "Inspection: CMM Pass", "MRB: UseAsIs"); the same
/// write tags that key with its <see cref="ProcessSite"/>.
/// <see cref="ListAtSiteAsync"/> then answers the per-site query through
/// the tag index instead of a hand-rolled range scan.
/// </summary>
/// <remarks>
/// <para>
/// The subject key is part-major on purpose: <c>{serial}/{site}</c> means
/// the site is <b>not</b> a usable key prefix, so a range scan can no
/// longer answer "parts at site X". The tag index is the genuine access
/// path - <see cref="ILatticeTagIndex.WithAnyTags"/> walks the posting
/// list for the site tag and yields exactly the matching keys.
/// </para>
/// <para>
/// Because each <c>(serial, site)</c> pair maps to a single key, a newer
/// fact overwrites the older row for that part at that site, so the
/// per-site result already has one row per part - no in-memory dedup is
/// required. An HLC guard on <see cref="AppendAsync"/> keeps the
/// most-recent activity even when facts arrive out of order. The value
/// carries the HLC (it is no longer embedded in the key), so
/// <see cref="ListAtSiteAsync"/> sorts the small per-site result set
/// most-recent-first in memory.
/// </para>
/// <para>
/// Every fact inherits <see cref="Fact.Site"/>, so every fact type is a
/// candidate - including inspections at sites like <c>StuttgartCmmLab</c>
/// that never emit a <see cref="ProcessStepCompleted"/>. That is what
/// lets the "Stuttgart CMM Lab" panel actually list its CMM inspections
/// instead of appearing permanently empty.
/// </para>
/// <para>
/// Every silo runs its own copy of the hosted service, so both silo A
/// and silo B append to the shared trees (the lattice trees themselves
/// are cluster-wide). Routed facts are enqueued onto a bounded
/// single-consumer ingest queue rather than written fire-and-forget, so
/// a burst applies back-pressure (the newest fact is shed and counted
/// once the queue is full) instead of stampeding the lattice write path.
/// Write failures are logged and swallowed - a transient storage hiccup
/// on the index must not break the main fact pipeline.
/// </para>
/// </remarks>
public sealed class SiteActivityIndex(
    IGrainFactory grainFactory,
    ILatticeTagIndexFactory tagIndexFactory,
    FederationRouter router,
    ILogger<SiteActivityIndex> logger) : IHostedService
{
    /// <summary>Lattice tree id that holds the part-major activity rows tagged by the index.</summary>
    public const string TreeId = "mfg-site-activity";

    /// <summary>Logical tag-index name; the membership tree is resolved as <c>tag-{IndexName}</c>.</summary>
    public const string IndexName = "mfg-site";

    /// <summary>Lattice tree id that holds the tag-index membership rows (<c>tag-{IndexName}</c>).</summary>
    public const string IndexTreeId = "tag-" + IndexName;

    private ILattice Tree => grainFactory.GetGrain<ILattice>(TreeId);

    private ILatticeTagIndex Index => tagIndexFactory.Create(Tree, IndexName);

    // Bounded ingest queue. Facts route in on the synchronous router
    // event, but the index write must not run unbounded fire-and-forget:
    // a write burst would otherwise spawn one append task per fact and
    // stampede the lattice write path (the very failure mode that wedged
    // the drain path). A bounded single-consumer channel applies
    // back-pressure - the producer enqueues without blocking, the single
    // drain loop serializes the writes, and once the queue is full the
    // index sheds the new fact (it is best-effort) and counts the drop.
    private const int IngestQueueCapacity = 1024;

    private readonly Channel<Fact> _ingest = Channel.CreateBounded<Fact>(
        new BoundedChannelOptions(IngestQueueCapacity)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait,
        });

    private CancellationTokenSource? _drainCts;
    private Task? _drainLoop;
    private long _droppedAppends;

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        _drainCts = new CancellationTokenSource();
        _drainLoop = Task.Run(() => DrainAsync(_drainCts.Token), CancellationToken.None);
        router.FactRouted += OnFactRouted;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        router.FactRouted -= OnFactRouted;
        _ingest.Writer.TryComplete();
        if (_drainCts is not null)
        {
            await _drainCts.CancelAsync().ConfigureAwait(false);
        }
        if (_drainLoop is not null)
        {
            try
            {
                await _drainLoop.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown.
            }
        }
        _drainCts?.Dispose();
    }

    /// <summary>
    /// Returns every part that has recent activity at
    /// <paramref name="site"/>, ordered <b>most-recent first</b> by the
    /// fact's hybrid logical clock, with one row per part (the latest
    /// activity for that part at that site). Implemented as a tag-index
    /// union query over the site tag, reading each matched key's value
    /// from the subject tree and sorting the result HLC-descending.
    /// </summary>
    public async Task<IReadOnlyList<SiteActivityIndexEntry>> ListAtSiteAsync(
        ProcessSite site, CancellationToken cancellationToken = default)
    {
        var tag = site.ToString();
        var tree = Tree;
        var entries = new List<SiteActivityIndexEntry>();
        await foreach (var key in Index.WithAnyTags(tag).WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            var value = await tree.GetAsync(key, cancellationToken).ConfigureAwait(false);
            if (value is null)
            {
                // Orphaned membership row (key removed). Skip; a reconcile
                // pass would prune it.
                continue;
            }
            if (TryParseEntry(key, value, out var entry))
            {
                entries.Add(entry);
            }
        }
        entries.Sort(static (a, b) => b.Hlc.CompareTo(a.Hlc));
        return entries;
    }

    /// <summary>
    /// Writes the index entry for <paramref name="fact"/>. Called
    /// internally by <see cref="OnFactRouted"/>; exposed publicly so
    /// tests can append without setting up the full federation router.
    /// A newer write wins: if a row already exists for this part at this
    /// site with an equal-or-newer HLC, the stale append is skipped.
    /// </summary>
    public async Task AppendAsync(Fact fact, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fact);
        var site = fact.Site.ToString();
        var key = KeyFor(fact.Serial, site);

        var existing = await Tree.GetAsync(key, cancellationToken).ConfigureAwait(false);
        if (existing is { Length: > 0 }
            && TryParseValue(existing, out var existingHlc, out _)
            && fact.Hlc.CompareTo(existingHlc) <= 0)
        {
            // A concurrent or newer fact already recorded this part's
            // activity at this site; keep the most recent.
            return;
        }

        var value = EncodeValue(fact);
        await Index.SetValueWithTags(key, value, site).Eventual().CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    private void OnFactRouted(object? sender, Fact fact)
    {
        // Non-blocking enqueue onto the bounded ingest queue. The single
        // drain loop serializes the actual lattice writes, so a fact burst
        // queues (and sheds the newest past the cap) instead of spawning an
        // unbounded fan-out of concurrent append tasks.
        if (!_ingest.Writer.TryWrite(fact))
        {
            var dropped = Interlocked.Increment(ref _droppedAppends);
            if (dropped % 256 == 1)
            {
                logger.LogWarning(
                    "SiteActivityIndex ingest queue saturated; shed {Dropped} best-effort index appends so far",
                    dropped);
            }
        }
    }

    private async Task DrainAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var fact in _ingest.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                await SafeAppendAsync(fact).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown.
        }
    }

    private async Task SafeAppendAsync(Fact fact)
    {
        try
        {
            await AppendAsync(fact).ConfigureAwait(false);
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
    /// site - these render directly into the parts-by-site grid.
    /// Exposed so the dashboard broadcaster can build a
    /// <see cref="SiteActivityIndexEntry"/> for a live fact without
    /// round-tripping through the lattice (the tag-index write stores
    /// the same label under <see cref="AppendAsync"/>).
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

    // Part-major key: site is deliberately the suffix so it is not a
    // usable range-scan prefix - the tag index is the only access path.
    private static string KeyFor(PartSerialNumber serial, string site) =>
        string.Concat(serial.Value, "/", site);

    // Value layout: {wallTicks}/{counter}/{activity}. The HLC moves into
    // the value because the part-major key no longer embeds it.
    private static byte[] EncodeValue(Fact fact) =>
        Encoding.UTF8.GetBytes(string.Create(
            CultureInfo.InvariantCulture,
            $"{fact.Hlc.WallClockTicks}/{fact.Hlc.Counter}/{DescribeActivity(fact)}"));

    private static bool TryParseEntry(string key, byte[] value, out SiteActivityIndexEntry entry)
    {
        entry = default;
        // {serial}/{site}
        var slash = key.IndexOf('/');
        if (slash <= 0 || slash >= key.Length - 1)
        {
            return false;
        }
        if (!Enum.TryParse<ProcessSite>(key.AsSpan(slash + 1), out var site))
        {
            return false;
        }
        if (!TryParseValue(value, out var hlc, out var activity))
        {
            return false;
        }
        entry = new SiteActivityIndexEntry(site, new PartSerialNumber(key[..slash]), hlc, activity);
        return true;
    }

    private static bool TryParseValue(byte[] value, out HybridLogicalClock hlc, out string activity)
    {
        hlc = default;
        activity = string.Empty;
        if (value is not { Length: > 0 })
        {
            return false;
        }
        var text = Encoding.UTF8.GetString(value);
        // {wallTicks}/{counter}/{activity}
        var firstSlash = text.IndexOf('/');
        if (firstSlash <= 0)
        {
            return false;
        }
        var secondSlash = text.IndexOf('/', firstSlash + 1);
        if (secondSlash < 0)
        {
            return false;
        }
        if (!long.TryParse(text.AsSpan(0, firstSlash), NumberStyles.Integer, CultureInfo.InvariantCulture, out var wallTicks)
            || !int.TryParse(text.AsSpan(firstSlash + 1, secondSlash - firstSlash - 1), NumberStyles.Integer, CultureInfo.InvariantCulture, out var counter))
        {
            return false;
        }
        hlc = new HybridLogicalClock { WallClockTicks = wallTicks, Counter = counter };
        activity = text[(secondSlash + 1)..];
        return true;
    }
}
