using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Apps.Tests;

internal sealed class RecordingReplicationAuthority : ILatticeReplicationConfigAuthority
{
    public Dictionary<string, LatticeReplicationTreeStatus> Trees { get; } = new(StringComparer.Ordinal);
    public List<string> Enables { get; } = new();
    public List<string> Disables { get; } = new();
    public Exception? EnableFailure { get; set; }
    public Func<string, Exception?>? FailEnable { get; set; }
    public Exception? DisableFailure { get; set; }

    public Task<LatticeReplicationEnableResult> EnableReplicationAsync(string treeId, LatticeMergeMode mode,
        string? bootstrapSourceClusterId = null, CancellationToken cancellationToken = default)
    {
        Assert.That(LatticeSystemOrigin.IsActive, Is.True);
        Assert.That(bootstrapSourceClusterId, Is.Null, "adopted-tree bootstrap is an explicit operator action");
        if (EnableFailure is { } failure)
            throw failure;
        if (FailEnable?.Invoke(treeId) is { } treeFailure)
            throw treeFailure;
        var alreadyEnabled = Trees.TryGetValue(treeId, out var current) && current.Enabled;
        if (alreadyEnabled && (current.Mode != mode || current.Ambiguous))
            throw new LatticeReplicationModeChangeRejectedException("mode changed");
        Enables.Add(treeId);
        Trees[treeId] = new(treeId, true, mode, false);
        return Task.FromResult(new LatticeReplicationEnableResult(treeId, mode, alreadyEnabled, false));
    }

    public Task<LatticeReplicationDisableResult> DisableReplicationAsync(string treeId, CancellationToken cancellationToken = default)
    {
        Assert.That(LatticeSystemOrigin.IsActive, Is.True);
        if (DisableFailure is { } failure)
            throw failure;
        var alreadyDisabled = !Trees.TryGetValue(treeId, out var current) || !current.Enabled;
        Disables.Add(treeId);
        if (!alreadyDisabled)
            Trees[treeId] = current with { Enabled = false };
        return Task.FromResult(new LatticeReplicationDisableResult(treeId, alreadyDisabled));
    }

    public Task<LatticeReplicationTreeStatus?> GetTreeStatusAsync(string treeId, CancellationToken cancellationToken = default) =>
        Task.FromResult(Trees.TryGetValue(treeId, out var current) ? (LatticeReplicationTreeStatus?)current : null);

    public Task<IReadOnlyDictionary<string, LatticeReplicationTreeStatus>> GetAllTreeStatusesAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult<IReadOnlyDictionary<string, LatticeReplicationTreeStatus>>(Trees);
}
