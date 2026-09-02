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

    /// <summary>
    /// The tree id to restrict counting to (an <see cref="ILattice"/> grain's
    /// string key), or <see langword="null"/> to count calls to every tree.
    /// </summary>
    public string? TreeId { get; init; }

    /// <summary>Returns how many times <paramref name="method"/> has been called.</summary>
    /// <param name="method">The <see cref="ILattice"/> method name, for example <c>ApplyCrdtDeltaManyAsync</c>.</param>
    public int Count(string method) => _counts.TryGetValue(method, out var count) ? count : 0;

    /// <summary>
    /// Clears every tally. Call it after harness bring-up so the host's own warm-up
    /// writes (grant seeding, and so on) are not counted against the test's action.
    /// </summary>
    public void Reset() => _counts.Clear();

    internal void Record(string method)
        => _counts.AddOrUpdate(method, 1, static (_, count) => count + 1);
}
