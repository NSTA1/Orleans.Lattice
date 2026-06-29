using System.Runtime.CompilerServices;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// In-memory implementation of <see cref="ICommitLogReader"/> for unit-testing
/// the generic WAL-tailing loop without a real per-shard WAL. Holds a dense,
/// offset-addressed list of mutations per <c>(treeId, shardIndex)</c> and
/// models a trimmed prefix via <see cref="TrimBefore"/> so fall-off-log
/// detection can be exercised.
/// </summary>
internal sealed class FakeCommitLogReader : ICommitLogReader
{
    private readonly Dictionary<(string Tree, int Shard), List<LatticeMutation>> _entries = new();
    private readonly Dictionary<(string Tree, int Shard), long> _trimBefore = new();

    /// <summary>Records the arguments of every <see cref="ReadAsync"/> call for assertions.</summary>
    public List<(string Tree, int Shard, long From)> Reads { get; } = new();

    /// <summary>Counts <see cref="GetTailOffsetAsync"/> calls, the storage-backed
    /// fall-off probe a caught-up drain must skip.</summary>
    public int TailProbes { get; private set; }

    /// <summary>Counts <see cref="GetHeadOffsetAsync"/> calls, the cheap in-memory
    /// next-sequence read the fall-off guard consults first.</summary>
    public int HeadProbes { get; private set; }

    /// <summary>Appends a mutation to a partition, returning its assigned offset.</summary>
    public long Append(string tree, int shard, LatticeMutation mutation)
    {
        var key = (tree, shard);
        if (!_entries.TryGetValue(key, out var list))
        {
            list = new List<LatticeMutation>();
            _entries[key] = list;
        }

        list.Add(mutation);
        return list.Count - 1;
    }

    /// <summary>
    /// Marks every offset strictly below <paramref name="offset"/> as trimmed
    /// (no longer readable), so <see cref="GetTailOffsetAsync"/> returns
    /// <paramref name="offset"/> and a consumer checkpointed below it falls off
    /// the log.
    /// </summary>
    public void TrimBefore(string tree, int shard, long offset) =>
        _trimBefore[(tree, shard)] = offset;

    /// <inheritdoc />
    public async IAsyncEnumerable<(long Offset, LatticeMutation Mutation)> ReadAsync(
        string treeId,
        int shardIndex,
        long fromOffsetExclusive,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        Reads.Add((treeId, shardIndex, fromOffsetExclusive));
        await Task.Yield();
        var key = (treeId, shardIndex);
        var trim = _trimBefore.GetValueOrDefault(key, 0);
        if (_entries.TryGetValue(key, out var list))
        {
            for (var offset = Math.Max(fromOffsetExclusive + 1, trim); offset < list.Count; offset++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return (offset, list[(int)offset]);
            }
        }
    }

    /// <inheritdoc />
    public Task<long> GetHeadOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken = default)
    {
        HeadProbes++;
        var count = _entries.TryGetValue((treeId, shardIndex), out var list) ? list.Count : 0;
        return Task.FromResult((long)count);
    }

    /// <inheritdoc />
    public Task<long> GetTailOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken = default)
    {
        TailProbes++;
        var key = (treeId, shardIndex);
        var count = _entries.TryGetValue(key, out var list) ? list.Count : 0;
        var trim = _trimBefore.GetValueOrDefault(key, 0);
        // Oldest still-readable offset: the trim point, clamped to head when the
        // partition is empty or fully trimmed.
        return Task.FromResult(Math.Min(trim, count));
    }
}
