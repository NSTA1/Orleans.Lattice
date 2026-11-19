using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// The registered <see cref="IReplicationReseedResponder"/>: gates on local
/// enrollment, then runs the same receiver logic the gRPC transport runs.
/// </summary>
internal sealed class ReplicationReseedResponderService(
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeReplicationOptions> options,
    ILatticeReplicationContext? replicationContext = null,
    ILogger<ReplicationReseedResponderService>? logger = null) : IReplicationReseedResponder
{
    private static readonly Task<long?> NoEpoch = Task.FromResult<long?>(null);

    private readonly ILogger _logger = logger ?? NullLogger<ReplicationReseedResponderService>.Instance;

    /// <inheritdoc />
    public Task<long?> RespondAsync(
        string treeName,
        string authenticatedOriginClusterId,
        long reseedAfterEpoch,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeName);
        ArgumentException.ThrowIfNullOrEmpty(authenticatedOriginClusterId);
        cancellationToken.ThrowIfCancellationRequested();

        // The tree name is wire input and a re-seed registers, fences and drains
        // the named tree, so only a tree enrolled here is answered.
        if (!IsEnrolled(treeName))
        {
            return NoEpoch;
        }

        return ReplicationReseedResponder.RespondAsync(
            grainFactory,
            treeName,
            authenticatedOriginClusterId,
            reseedAfterEpoch,
            options.Get(treeName).AutoBootstrapOnFallOffLog,
            _logger);
    }

    /// <inheritdoc />
    public Task<bool> RefusesStragglerAsync(
        string treeName,
        string authenticatedOriginClusterId,
        IReadOnlyList<WalRecord> entries,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeName);
        ArgumentException.ThrowIfNullOrEmpty(authenticatedOriginClusterId);
        ArgumentNullException.ThrowIfNull(entries);
        cancellationToken.ThrowIfCancellationRequested();

        // A tree not enrolled here activates no coordinator; the applier's own
        // enrollment gate drops its entries.
        if (!IsEnrolled(treeName))
        {
            return Task.FromResult(false);
        }

        return ReplicationReseedResponder.RefusesStragglerAsync(
            grainFactory, treeName, authenticatedOriginClusterId, entries, _logger);
    }

    private bool IsEnrolled(string treeName) => replicationContext?.ResolveMergeMode(treeName) is not null;
}
