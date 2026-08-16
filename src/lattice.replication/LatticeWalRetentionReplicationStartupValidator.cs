using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Startup guard that fails fast when a <see cref="LatticeReplicationOptions.ReplicatedTrees">
/// replicated</see> tree has an effective <see cref="LatticeOptions.WalRetention"/> ceiling
/// configured while the anti-entropy detection backstop
/// (<see cref="LatticeReplicationOptions.DigestProbeEnabled"/>) is disabled - the
/// combination that lets the sender's WAL garbage collector silently trim entries a lagging
/// cross-cluster shipper has not shipped yet, permanently diverging the receiver with no
/// wired detector. Mirrors <see cref="LatticeViewReplicationStartupValidator"/>: it runs as
/// an <see cref="IHostedService"/> and throws <see cref="InvalidOperationException"/> from
/// <see cref="StartAsync"/> so the silo refuses to start.
/// <para>
/// Unlike a local materialiser - whose fall-off-the-log read surfaces the trimmed prefix to
/// the auto-bootstrap trigger - the cross-cluster shipper advances past a trimmed prefix
/// without emitting a fall-off event, and the receiver-side fall-off detector can only
/// compare against its own local WAL, so it never sees what it never received. The digest
/// probe compares content digests out-of-band and therefore detects a garbage-collected
/// divergence; requiring it (or an explicit, audited acknowledgement via
/// <see cref="LatticeReplicationOptions.AllowWalRetentionWithoutAntiEntropy"/>) converts a
/// silent-divergence footgun into a loud startup error.
/// </para>
/// <para>
/// The effective retention is read from the per-tree core
/// <see cref="LatticeOptions.WalRetention"/> resolved by tree name, which already reflects any
/// value mirrored from <see cref="LatticeReplicationOptions.WalRetention"/>, so the guard
/// catches retention configured on either surface. Trees that are not replicated are out of
/// scope: they have no cross-cluster shipper and therefore no gap.
/// </para>
/// </summary>
internal sealed class LatticeWalRetentionReplicationStartupValidator(
    IOptionsMonitor<LatticeOptions> latticeOptions,
    IOptionsMonitor<LatticeReplicationOptions> replicationOptions) : IHostedService
{
    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        var replication = replicationOptions.CurrentValue;
        var trees = replication.ReplicatedTrees;
        if (trees is null || trees.Count == 0)
        {
            return Task.CompletedTask;
        }

        // The digest probe is the out-of-band backstop that makes a
        // garbage-collected cross-cluster divergence observable; when it is on,
        // WAL retention on a replicated tree is safe. The explicit override lets
        // an operator knowingly accept the risk (for example a strictly
        // unidirectional deployment where the trimming cluster is never a
        // receiver).
        if (replication.DigestProbeEnabled || replication.AllowWalRetentionWithoutAntiEntropy)
        {
            return Task.CompletedTask;
        }

        foreach (var treeName in trees.Keys)
        {
            if (latticeOptions.Get(treeName).WalRetention is not null)
            {
                throw new InvalidOperationException(
                    $"Tree '{treeName}' is declared in {nameof(LatticeReplicationOptions)}."
                    + $"{nameof(LatticeReplicationOptions.ReplicatedTrees)} and has an effective "
                    + $"{nameof(LatticeOptions)}.{nameof(LatticeOptions.WalRetention)} ceiling set, but the "
                    + $"anti-entropy detection backstop {nameof(LatticeReplicationOptions)}."
                    + $"{nameof(LatticeReplicationOptions.DigestProbeEnabled)} is disabled. WAL retention lets "
                    + "the sender's garbage collector trim entries a lagging cross-cluster shipper has not "
                    + "shipped yet; unlike a local consumer, the shipper does not surface that trim as a "
                    + "fall-off-the-log event, so the receiver would silently and permanently diverge for the "
                    + "trimmed range with no metric and no repair. Resolve this by one of: enable "
                    + $"{nameof(LatticeReplicationOptions.DigestProbeEnabled)} (and, for automatic repair, the "
                    + $"remaining anti-entropy stages - {nameof(LatticeReplicationOptions.MerkleWalkEnabled)}, "
                    + $"{nameof(LatticeReplicationOptions.LeafReReplayEnabled)}, "
                    + $"{nameof(LatticeReplicationOptions.BootstrapFallbackEnabled)}, "
                    + $"{nameof(LatticeReplicationOptions.AutoRemediateOnDigestMismatch)}) so the divergence is "
                    + $"detected and healed; remove {nameof(LatticeOptions.WalRetention)} from this tree so a "
                    + "lagging shipper pins the WAL until it catches up; or, if the silent-divergence risk is "
                    + $"knowingly acceptable, set {nameof(LatticeReplicationOptions)}."
                    + $"{nameof(LatticeReplicationOptions.AllowWalRetentionWithoutAntiEntropy)} to true to "
                    + "acknowledge it explicitly.");
            }
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
