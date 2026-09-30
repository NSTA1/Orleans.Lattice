using Orleans.Lattice.Api.Replication;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Replication;

/// <summary>
/// A scripted <see cref="ILatticeReplicationControl"/>: it reports <see cref="Trees"/>,
/// records every enable and disable, applies them to <see cref="Trees"/>, and throws
/// <see cref="ReadFailure"/> or <see cref="ChangeFailure"/> when set.
/// </summary>
internal sealed class FakeReplicationControl : ILatticeReplicationControl
{
    public List<ReplicationTreeConfigEntry> Trees { get; } = [];

    public Exception? ReadFailure { get; set; }

    public Exception? ChangeFailure { get; set; }

    public TaskCompletionSource? ChangeGate { get; set; }

    public int Reads { get; private set; }

    public List<(string TreeId, LatticeMergeMode Mode, string? Bootstrap)> Enables { get; } = [];

    public List<string> Disables { get; } = [];

    public async Task<ReplicationEnableResult> EnableReplicationAsync(
        string treeId,
        LatticeMergeMode mode,
        string? bootstrapSourceClusterId = null,
        CancellationToken cancellationToken = default)
    {
        Enables.Add((treeId, mode, bootstrapSourceClusterId));
        if (ChangeGate is { } gate)
        {
            await gate.Task.WaitAsync(cancellationToken);
        }

        if (ChangeFailure is not null)
        {
            throw ChangeFailure;
        }

        var existing = Trees.FindIndex(tree => tree.TreeId == treeId);
        var already = existing >= 0 && Trees[existing].Enabled;
        var entry = new ReplicationTreeConfigEntry(treeId, enabled: true, mode, ambiguous: false) { Source = ReplicationEnrollmentSource.Runtime };
        if (existing >= 0)
        {
            Trees[existing] = entry with { Source = Trees[existing].Source };
        }
        else
        {
            Trees.Add(entry);
        }

        return new ReplicationEnableResult(treeId, mode, already, bootstrapRequested: bootstrapSourceClusterId is not null);
    }

    public async Task<ReplicationDisableResult> DisableReplicationAsync(string treeId, CancellationToken cancellationToken = default)
    {
        Disables.Add(treeId);
        if (ChangeGate is { } gate)
        {
            await gate.Task.WaitAsync(cancellationToken);
        }

        if (ChangeFailure is not null)
        {
            throw ChangeFailure;
        }

        var existing = Trees.FindIndex(tree => tree.TreeId == treeId);
        var already = existing < 0 || !Trees[existing].Enabled;
        if (existing >= 0)
        {
            Trees[existing] = Trees[existing] with { Enabled = false };
        }

        return new ReplicationDisableResult(treeId, already);
    }

    public Task<ReplicationConfigReport> GetReplicationConfigAsync(CancellationToken cancellationToken = default)
    {
        Reads++;
        return ReadFailure is not null
            ? Task.FromException<ReplicationConfigReport>(ReadFailure)
            : Task.FromResult(new ReplicationConfigReport([.. Trees]));
    }
}
