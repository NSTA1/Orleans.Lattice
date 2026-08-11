namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Merges an incoming snapshot value into the value already stored under a key at
/// import time, so a re-import converges through the store's CRDT semantics
/// rather than overwriting blindly. Called once per imported record.
/// <para>
/// <paramref name="existing"/> is <see langword="null"/> when the key is absent
/// from the target store (a first-time import), in which case the strategy should
/// return <paramref name="incoming"/> unchanged. When both sides are present the
/// strategy returns the join of the two CRDT states, which - because a CRDT join
/// is idempotent - makes a second import of the same snapshot a no-op.
/// </para>
/// </summary>
/// <param name="key">The store key being imported.</param>
/// <param name="existing">The value already stored under the key, or <see langword="null"/> when absent.</param>
/// <param name="incoming">The value carried by the snapshot record.</param>
/// <returns>The merged value bytes to store under the key.</returns>
internal delegate byte[] RepoContextSnapshotMerge(string key, byte[]? existing, byte[] incoming);
