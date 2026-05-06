using System.Runtime.CompilerServices;
using System.Threading.Channels;
using MultiSiteManufacturing.Host.Lattice;

namespace MultiSiteManufacturing.Host.Dashboard;

/// <summary>
/// Per-circuit subscribe APIs for <see cref="DashboardBroadcaster"/>.
/// Each call allocates a private unbounded <see cref="Channel{T}"/>
/// and yields a long-lived <see cref="IAsyncEnumerable{T}"/> until
/// the caller's cancellation token fires; on cancellation the channel
/// is removed from the broadcaster's subscriber dictionary and
/// completed. The fan-out path (<c>.FanOut.cs</c>) writes a single
/// derived event to every channel in the matching dictionary.
/// </summary>
public sealed partial class DashboardBroadcaster
{
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

    /// <summary>
    /// Live feed of divergence events. Yields a new <see cref="DivergenceEvent"/>
    /// whenever a part's baseline/lattice agreement changes — enters
    /// divergence, stays divergent with a new state pair, or resolves
    /// (<see cref="DivergenceEvent.Resolved"/> is <c>true</c>).
    /// </summary>
    public async IAsyncEnumerable<DivergenceEvent> SubscribeDivergence(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var id = Guid.NewGuid();
        var channel = Channel.CreateUnbounded<DivergenceEvent>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
        });
        _divSubs[id] = channel;
        try
        {
            await foreach (var update in channel.Reader.ReadAllAsync(cancellationToken))
            {
                yield return update;
            }
        }
        finally
        {
            _divSubs.TryRemove(id, out _);
            channel.Writer.TryComplete();
        }
    }

    /// <summary>
    /// Live feed of site-activity entries — one message per fact
    /// routed locally (<see cref="FederationRouter.FactRouted"/>) or
    /// replicated from a peer cluster
    /// (<see cref="FederationRouter.FactReplicated"/>). The dashboard's
    /// "Inventory By Activity" sub-tab consumes this and merges entries
    /// that match its currently-selected <see cref="ProcessSite"/> into
    /// the displayed grid so a user watching a site sub-tab sees new
    /// activity appear immediately without re-running the range scan.
    /// </summary>
    /// <remarks>
    /// The broadcaster does not filter by site — every subscriber
    /// receives every entry and filters client-side. Volumes are
    /// modest (one entry per fact) and the per-subscriber channel is
    /// unbounded, matching the back-pressure model of the existing
    /// <see cref="SubscribePartUpdates"/> and
    /// <see cref="SubscribeChaosChanges"/> feeds.
    /// </remarks>
    public async IAsyncEnumerable<SiteActivityIndexEntry> SubscribeSiteActivity(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var id = Guid.NewGuid();
        var channel = Channel.CreateUnbounded<SiteActivityIndexEntry>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
        });
        _activitySubs[id] = channel;
        try
        {
            await foreach (var entry in channel.Reader.ReadAllAsync(cancellationToken))
            {
                yield return entry;
            }
        }
        finally
        {
            _activitySubs.TryRemove(id, out _);
            channel.Writer.TryComplete();
        }
    }
}
