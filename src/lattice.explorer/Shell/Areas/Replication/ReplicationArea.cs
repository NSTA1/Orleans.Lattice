using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Explorer.Shell.Navigation;

namespace Orleans.Lattice.Explorer.Shell.Areas.Replication;

/// <summary>
/// The Replication area: the estate's replication links drawn as an order
/// diagram, the enrolled trees, and each tree's per-peer links. Bound to
/// <see cref="ILatticeReplicationStatus"/> and <see cref="ILatticeReplicationControl"/>
/// only, through <see cref="ReplicationDataSource"/>.
/// </summary>
/// <remarks>
/// Visibility fails closed: the area is shown only when the caller can read the
/// peer status report, or the enrolment report names at least one tree the caller
/// may manage. A denial, an unserved facade, a missing registration or any other
/// fault leaves it hidden.
/// </remarks>
internal sealed class ReplicationArea : IExplorerArea
{
    /// <summary>The id of the command that re-reads the estate.</summary>
    public const string RefreshCommandId = "replication.refresh";

    /// <summary>The id of the command that shows the enrolled trees.</summary>
    public const string TreesCommandId = "replication.trees";

    private readonly ReplicationDataSource _data;

    /// <summary>Creates the area over the circuit's data source and completions.</summary>
    /// <param name="data">The circuit's replication data source.</param>
    /// <param name="completions">The area's address completions.</param>
    public ReplicationArea(ReplicationDataSource data, ReplicationCompletionSource completions)
    {
        ArgumentNullException.ThrowIfNull(data);
        ArgumentNullException.ThrowIfNull(completions);
        _data = data;
        Completions = completions;
        Commands =
        [
            new ExplorerCommand(RefreshCommandId, "Refresh replication status")
            {
                Detail = "Read every peer link again",
                Target = ReplicationAddresses.Estate,
                InvokeAsync = _ =>
                {
                    _data.Invalidate();
                    return ValueTask.CompletedTask;
                },
            },
            new ExplorerCommand(TreesCommandId, "Show enrolled trees")
            {
                Detail = "Replicated trees, their merge mode and enrolment",
                Target = ReplicationAddresses.Trees,
            },
        ];
    }

    /// <inheritdoc />
    public string Key => ReplicationAddresses.AreaKey;

    /// <inheritdoc />
    public string DisplayName => "Replication";

    /// <inheritdoc />
    public int DirectoryOrder => 60;

    /// <inheritdoc />
    public IAddressCompletionSource? Completions { get; }

    /// <inheritdoc />
    public IReadOnlyList<ExplorerCommand> Commands { get; }

    /// <inheritdoc />
    public async ValueTask<AreaAvailability> GetAvailabilityAsync(CancellationToken cancellationToken)
    {
        if (!_data.HasStatus && !_data.HasControl)
        {
            return AreaAvailability.Hidden;
        }

        var status = await _data.GetEstateAsync(refresh: false, cancellationToken).ConfigureAwait(false);
        if (status.Succeeded)
        {
            return AreaAvailability.Visible;
        }

        var config = await _data.GetConfigAsync(refresh: false, cancellationToken).ConfigureAwait(false);
        return config.Value is { Trees.Count: > 0 } ? AreaAvailability.Visible : AreaAvailability.Hidden;
    }

    /// <inheritdoc />
    public async ValueTask<string?> GetHomeStatusAsync(CancellationToken cancellationToken)
    {
        var status = await _data.GetEstateAsync(refresh: false, cancellationToken).ConfigureAwait(false);
        if (status.Value is { } estate)
        {
            if (estate.Links.Count == 0)
            {
                return "No replication links yet.";
            }

            var peers = ReplicationFormat.Count(estate.PeerRegions.Count, "peer region", "peer regions");
            var links = ReplicationFormat.Count(estate.Links.Count, "link", "links");
            var stalled = estate.Count(ReplicationLinkHealth.Stalled);
            var lagging = estate.Count(ReplicationLinkHealth.Lagging);
            if (stalled == 0 && lagging == 0)
            {
                return $"{peers}, {links}, none stalled or lagging.";
            }

            var parts = new List<string>(2);
            if (stalled > 0)
            {
                parts.Add(ReplicationFormat.Count(stalled) + " stalled");
            }

            if (lagging > 0)
            {
                parts.Add(ReplicationFormat.Count(lagging) + " lagging");
            }

            return $"{peers}, {links}: {string.Join(", ", parts)}.";
        }

        var config = await _data.GetConfigAsync(refresh: false, cancellationToken).ConfigureAwait(false);
        return config.Value is { } report
            ? ReplicationFormat.Count(report.Trees.Count(tree => tree.Enabled), "tree", "trees") + " enrolled."
            : null;
    }

    /// <inheritdoc />
    public async ValueTask<string?> GetDirectoryBadgeAsync(CancellationToken cancellationToken)
    {
        var status = await _data.GetEstateAsync(refresh: false, cancellationToken).ConfigureAwait(false);
        if (status.Value is not { } estate)
        {
            return null;
        }

        var stalled = estate.Count(ReplicationLinkHealth.Stalled);
        if (stalled > 0)
        {
            return ReplicationFormat.Count(stalled) + " stalled";
        }

        var lagging = estate.Count(ReplicationLinkHealth.Lagging);
        return lagging > 0 ? ReplicationFormat.Count(lagging) + " lag" : null;
    }
}
