namespace Orleans.Lattice.GrainIndex.Tests.Registry;

/// <summary>
/// An <see cref="ILatticeMergeModeResolver"/> that answers from a fixed map, so
/// the startup replication guard can be exercised without the replication
/// package. Returning <c>null</c> for an unlisted tree is exactly what the core
/// default resolver does.
/// </summary>
internal sealed class FakeMergeModeResolver : ILatticeMergeModeResolver
{
    private readonly Dictionary<string, LatticeMergeMode> _modes = new(StringComparer.Ordinal);

    /// <summary>The tree ids <see cref="Resolve"/> has been asked about, in call order.</summary>
    internal List<string> Queried { get; } = [];

    /// <summary>Declares <paramref name="treeId"/> as replicated with <paramref name="mode"/>.</summary>
    internal FakeMergeModeResolver Replicating(string treeId, LatticeMergeMode mode)
    {
        _modes[treeId] = mode;
        return this;
    }

    /// <inheritdoc />
    public LatticeMergeMode? Resolve(string treeId)
    {
        Queried.Add(treeId);
        return _modes.TryGetValue(treeId, out var mode) ? mode : null;
    }
}
