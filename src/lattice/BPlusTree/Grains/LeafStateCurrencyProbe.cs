using Orleans.Lattice.BPlusTree.State;
using Orleans.Storage;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The claim's half of #5841's rule A: whether a leaf activation's state row is still the
/// one it loaded or last wrote, read fresh from storage. A split donor persists its narrowing
/// before it fences the leaf's epoch row, so a duplicate that claims after that fence reads
/// here a row it did not write and is superseded before it stamps a single append.
/// </summary>
/// <remarks>
/// The read and the comparison run under the leaf's persist-write gate (A1): a persist the
/// activation has in flight completes first, and its new ETag is the one compared, so the
/// activation's own write never reads as another's. Any other difference counts as stale,
/// which is conservative: a false mismatch only deactivates the activation. A missing row and
/// an empty ETag compare equal to each other, as an activation that never persisted holds.
/// The fresh read relies on the storage provider's strong read-after-write across clients
/// (A2).
/// </remarks>
internal sealed class LeafStateCurrencyProbe
{
    private readonly IGrainStorage _storage;
    private readonly GrainId _grainId;
    private readonly Func<string?> _loadedEtag;
    private readonly SemaphoreSlim _persistGate;

    /// <summary>The leaf's persistent-state name.</summary>
    internal const string LeafStateName = "leaf";

    /// <summary>Creates the probe for one leaf activation.</summary>
    /// <param name="storage">The grain storage provider holding the leaf's state.</param>
    /// <param name="grainId">The leaf's grain id.</param>
    /// <param name="loadedEtag">The activation's in-memory ETag, read under <paramref name="persistGate"/>.</param>
    /// <param name="persistGate">The gate that serialises the activation's state writes.</param>
    public LeafStateCurrencyProbe(IGrainStorage storage, GrainId grainId, Func<string?> loadedEtag, SemaphoreSlim persistGate)
    {
        ArgumentNullException.ThrowIfNull(storage);
        ArgumentNullException.ThrowIfNull(loadedEtag);
        ArgumentNullException.ThrowIfNull(persistGate);
        _storage = storage;
        _grainId = grainId;
        _loadedEtag = loadedEtag;
        _persistGate = persistGate;
    }

    /// <summary>Whether the durable state row still carries the activation's ETag.</summary>
    public async Task<bool> IsCurrentAsync()
    {
        await _persistGate.WaitAsync().ConfigureAwait(false);
        try
        {
            var fresh = new GrainState<LeafNodeState>(new LeafNodeState());
            await _storage.ReadStateAsync(LeafStateName, _grainId, fresh).ConfigureAwait(false);
            return EtagsEqual(fresh.RecordExists ? fresh.ETag : null, _loadedEtag());
        }
        finally
        {
            _persistGate.Release();
        }
    }

    /// <summary>Whether two ETags name the same row version, treating null and empty alike.</summary>
    internal static bool EtagsEqual(string? durable, string? loaded) =>
        string.IsNullOrEmpty(durable) ? string.IsNullOrEmpty(loaded) : string.Equals(durable, loaded, StringComparison.Ordinal);
}
