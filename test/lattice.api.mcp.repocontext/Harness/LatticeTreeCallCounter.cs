using System.Collections.Concurrent;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// Tallies the <see cref="ILattice"/> grain calls a test drives, by method name
/// and optionally scoped to a single tree, so a fixture can assert the <b>shape</b>
/// of a write path rather than its wall-clock cost.
/// <para>
/// The motivating assertion is that a batched write really is batched: a per-key
/// loop and a single batched call produce identical stored state, so only a call
/// count can tell them apart, and a timing assertion would be flaky. Register it
/// with <see cref="LatticeTreeCallCountingFilter"/> through
/// <see cref="RepoContextMcpHarnessOptions.ConfigureSilo"/>.
/// </para>
/// </summary>
public sealed class LatticeTreeCallCounter
{
    private readonly ConcurrentDictionary<string, int> _counts = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, int> _keyCounts = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, int> _treeKeyCounts = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, int> _treeCounts = new(StringComparer.Ordinal);

    /// <summary>
    /// The tree id to restrict counting to (an <see cref="ILattice"/> grain's
    /// string key), or <see langword="null"/> to count calls to every tree.
    /// </summary>
    public string? TreeId { get; init; }

    /// <summary>Returns how many times <paramref name="method"/> has been called.</summary>
    /// <param name="method">The <see cref="ILattice"/> method name, for example <c>ApplyCrdtDeltaManyAsync</c>.</param>
    public int Count(string method) => _counts.TryGetValue(method, out var count) ? count : 0;

    /// <summary>
    /// Returns how many <b>keys</b> <paramref name="method"/> has been called with in
    /// total, counting a batched call as the size of its batch.
    /// <para>
    /// This is the figure that distinguishes an O(sources) read path from an O(pages)
    /// one, which a call count alone cannot: batching turns 20,000 point reads into 79
    /// multi-gets, so the call count falls by the batch size on a path whose real cost
    /// is unchanged. Issue #2486 measures the coverage-detection path in keys for
    /// exactly that reason.
    /// </para>
    /// </summary>
    /// <param name="method">The <see cref="ILattice"/> method name.</param>
    public int KeyCount(string method) => _keyCounts.TryGetValue(method, out var count) ? count : 0;

    /// <summary>
    /// Returns the total keys read or written against <paramref name="treeId"/> across
    /// every method, for a counter that is not restricted to a single tree.
    /// </summary>
    /// <param name="treeId">The tree id (an <see cref="ILattice"/> grain's string key).</param>
    public int KeyCountForTree(string treeId)
        => _treeKeyCounts.TryGetValue(treeId, out var count) ? count : 0;

    /// <summary>Returns the total calls made against <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The tree id (an <see cref="ILattice"/> grain's string key).</param>
    public int CountForTree(string treeId)
        => _treeCounts.TryGetValue(treeId, out var count) ? count : 0;

    /// <summary>
    /// The <see cref="ILattice"/> methods that actually read stored data. Anything
    /// outside this set is transport or saga machinery - <c>GetRoutingAsync</c>
    /// resolves a shard and reads no entry, and an atomic write is implemented over
    /// an inner <c>SetManyAsync</c>, so counting either against a data-key total
    /// inflates it by an amount that varies with the tree's internals rather than
    /// with the caller's cost model.
    /// <para>
    /// Issue #2486's central claim is about <b>keys read during gap detection</b>, so
    /// it is measured over exactly these three methods and nothing else. Measuring it
    /// over every recorded call would have reported 6 keys for a one-source write
    /// whose real cost is 3, and the same contamination would sit silently inside the
    /// headline detection figures.
    /// </para>
    /// </summary>
    private static readonly HashSet<string> ReadMethods = new(StringComparer.Ordinal)
    {
        "GetAsync",
        "GetManyAsync",
        "GetManyWithGateAccountingAsync",
    };

    /// <summary>
    /// Returns the number of <b>stored data keys read</b> against
    /// <paramref name="treeId"/>, counting a batched read as the size of its batch and
    /// excluding routing and saga machinery.
    /// <para>
    /// This is the figure issue #2486's cost claim is stated in. It is deliberately
    /// narrower than <see cref="KeyCountForTree(string)"/>: a raw per-tree total also
    /// picks up shard-routing lookups and the inner write a saga performs, neither of
    /// which is a read and neither of which scales with the corpus.
    /// </para>
    /// </summary>
    /// <param name="treeId">The tree id (an <see cref="ILattice"/> grain's string key).</param>
    public int ReadKeyCountForTree(string treeId)
        => _treeMethodKeyCounts
            .Where(pair => pair.Key.TreeId.Equals(treeId, StringComparison.Ordinal)
                && ReadMethods.Contains(pair.Key.Method))
            .Sum(pair => pair.Value.Keys);

    /// <summary>
    /// Returns a stable, printable per-method breakdown of the calls and keys
    /// recorded against <paramref name="treeId"/>, ordered by method name.
    /// <para>
    /// A bare total answers "how much did this cost" but not "what did it spend it
    /// on", and the difference matters when a total comes in above a predicted
    /// figure: the useful next question is always which method the surplus is in.
    /// Printing this into the test output keeps the answer in the run log rather
    /// than requiring the fixture be re-instrumented to ask.
    /// </para>
    /// </summary>
    /// <param name="treeId">The tree id (an <see cref="ILattice"/> grain's string key).</param>
    public string DescribeTree(string treeId)
    {
        var parts = _treeMethodKeyCounts
            .Where(pair => pair.Key.TreeId.Equals(treeId, StringComparison.Ordinal))
            .OrderBy(pair => pair.Key.Method, StringComparer.Ordinal)
            .Select(pair => $"{pair.Key.Method} x{pair.Value.Calls} ({pair.Value.Keys} key(s))");
        return string.Join(", ", parts);
    }

    /// <summary>
    /// Returns how many keys <paramref name="method"/> was called with against
    /// <paramref name="treeId"/>.
    /// </summary>
    /// <param name="treeId">The tree id (an <see cref="ILattice"/> grain's string key).</param>
    /// <param name="method">The <see cref="ILattice"/> method name.</param>
    public int KeyCountForTree(string treeId, string method)
        => _treeMethodKeyCounts.TryGetValue(new TreeMethod(treeId, method), out var entry) ? entry.Keys : 0;

    private readonly record struct TreeMethod(string TreeId, string Method);

    private readonly ConcurrentDictionary<TreeMethod, (int Calls, int Keys)> _treeMethodKeyCounts = new();

    /// <summary>
    /// Clears every tally. Call it after harness bring-up so the host's own warm-up
    /// writes (grant seeding, and so on) are not counted against the test's action.
    /// </summary>
    public void Reset()
    {
        _counts.Clear();
        _keyCounts.Clear();
        _treeKeyCounts.Clear();
        _treeCounts.Clear();
        _treeMethodKeyCounts.Clear();
    }

    internal void Record(string method) => Record(method, treeId: string.Empty, keys: 1);

    internal void Record(string method, string treeId, int keys)
    {
        _counts.AddOrUpdate(method, 1, static (_, count) => count + 1);
        _keyCounts.AddOrUpdate(method, keys, (_, count) => count + keys);
        _treeCounts.AddOrUpdate(treeId, 1, static (_, count) => count + 1);
        _treeKeyCounts.AddOrUpdate(treeId, keys, (_, count) => count + keys);
        _treeMethodKeyCounts.AddOrUpdate(
            new TreeMethod(treeId, method),
            (1, keys),
            (_, entry) => (entry.Calls + 1, entry.Keys + keys));
    }
}
