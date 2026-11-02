using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// A switch that holds <see cref="ILattice.DeleteTreeAsync"/> on one tree until the test
/// releases it, as a loaded cluster delays retiring a replicated tree (issue #5482). The
/// world is shared by every fixture, so a test arms it for one step and disposes the hold.
/// </summary>
internal sealed class TreeDeleteHolds
{
    private readonly Lock _gate = new();
    private readonly Dictionary<string, Hold> _holds = new(StringComparer.Ordinal);

    /// <summary>Holds every delete of <paramref name="treeId"/> until the returned hold is released or disposed.</summary>
    /// <param name="treeId">The tree.</param>
    /// <returns>The hold.</returns>
    public Hold Arm(string treeId)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        var hold = new Hold(this, treeId);
        lock (_gate)
        {
            _holds[treeId] = hold;
        }

        return hold;
    }

    /// <summary>Waits while <paramref name="treeId"/> is held.</summary>
    /// <param name="treeId">The tree a delete was called on.</param>
    public Task WaitAsync(string treeId)
    {
        Hold? hold;
        lock (_gate)
        {
            _holds.TryGetValue(treeId, out hold);
        }

        if (hold is null)
        {
            return Task.CompletedTask;
        }

        hold.Entered.TrySetResult();
        return hold.Released.Task;
    }

    private void Remove(Hold hold)
    {
        lock (_gate)
        {
            if (_holds.TryGetValue(hold.TreeId, out var current) && ReferenceEquals(current, hold))
            {
                _holds.Remove(hold.TreeId);
            }
        }
    }

    /// <summary>One armed hold; disposing it releases the tree.</summary>
    internal sealed class Hold(TreeDeleteHolds owner, string treeId) : IDisposable
    {
        /// <summary>The held tree.</summary>
        public string TreeId { get; } = treeId;

        /// <summary>Completes when a delete reaches the hold.</summary>
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Completes when the hold is released.</summary>
        public TaskCompletionSource Released { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Lets every held delete continue.</summary>
        public void Release()
        {
            owner.Remove(this);
            Released.TrySetResult();
        }

        /// <inheritdoc />
        public void Dispose() => Release();
    }
}
