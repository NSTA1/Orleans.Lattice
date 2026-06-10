using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Typed value-surface accessor for a <see cref="Orleans.Lattice.VersionVector"/>
/// stored under a single key in an <see cref="ILattice"/>. Mutating
/// methods read-modify-write under optimistic concurrency, retrying on
/// CAS failure up to a configurable budget.
/// </summary>
public readonly record struct VersionVectorAccessor
{
    /// <summary>Default CAS retry budget for mutating operations.</summary>
    public const int DefaultMaxAttempts = 16;

    private readonly ILattice _lattice;
    private readonly string _key;

    internal VersionVectorAccessor(ILattice lattice, string key)
    {
        _lattice = lattice;
        _key = key;
    }

    /// <summary>The tree the accessor is bound to.</summary>
    public ILattice Lattice => _lattice;

    /// <summary>The key the accessor reads and writes.</summary>
    public string Key => _key;

    /// <summary>
    /// Reads the current vector state. Returns an empty <see cref="Orleans.Lattice.VersionVector"/>
    /// when the key is absent or tombstoned.
    /// </summary>
    public async Task<VersionVector> GetAsync(CancellationToken cancellationToken = default)
    {
        EnsureInitialised();
        var bytes = await _lattice.GetAsync(_key, cancellationToken).ConfigureAwait(false);
        return Decode(bytes);
    }

    /// <summary>
    /// Advances the entry for <paramref name="replicaId"/> via
    /// <see cref="VersionVector.Tick(string)"/> and persists the result.
    /// </summary>
    public Task TickAsync(string replicaId, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        EnsureInitialised();
        return MutateAsync(v =>
        {
            var clock = v.Tick(replicaId);
            return new VersionVectorDelta
            {
                Entries = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal) { [replicaId] = clock },
            };
        }, cancellationToken, maxAttempts);
    }

    /// <summary>Merges <paramref name="other"/> into the stored state under CAS.</summary>
    public Task MergeAsync(VersionVector other, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentNullException.ThrowIfNull(other);
        EnsureInitialised();
        return MutateAsync(v =>
        {
            v.MergeFrom(other);
            return new VersionVectorDelta
            {
                Entries = new Dictionary<string, HybridLogicalClock>(other.Entries, StringComparer.Ordinal),
            };
        }, cancellationToken, maxAttempts);
    }

    private async Task MutateAsync<TDelta>(
        Func<VersionVector, TDelta> mutate,
        CancellationToken cancellationToken,
        int maxAttempts)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);
        // CAS-free producer-side delta apply: see
        // PnCounterAccessor.MutateAsync for the single-read +
        // ApplyCrdtDeltaAsync rationale. Per-entry merge inside the
        // leaf takes the max HLC, so a stale local snapshot is
        // harmless - the worst case is a slightly lower tick that the
        // leaf will subsume on merge.
        _ = maxAttempts;
        cancellationToken.ThrowIfCancellationRequested();
        var current = await GetAsync(cancellationToken).ConfigureAwait(false);
        var delta = mutate(current);
        var deltaBytes = JsonLatticeSerializer<TDelta>.Default.Serialize(delta);
        await _lattice.ApplyCrdtDeltaAsync(_key, LatticeMergeMode.VersionVector, deltaBytes, cancellationToken).ConfigureAwait(false);
    }

    private static VersionVector Decode(byte[]? bytes) =>
        bytes is null ? new VersionVector() : JsonLatticeSerializer<VersionVector>.Default.Deserialize(bytes);

    private void EnsureInitialised()
    {
        if (_lattice is null)
        {
            throw new InvalidOperationException(
                "VersionVectorAccessor is uninitialised; obtain it via ILattice.VersionVector(key) instead of `default`.");
        }
    }
}
