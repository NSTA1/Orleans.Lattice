namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Internal in-process seam exposing a leaf grain's projection as a
/// deterministic apply-replay surface. Implemented by
/// <see cref="BPlusLeafGrain"/>; consumed by the in-grain replay path the
/// WAL-as-sole-commit-point promotion will introduce so a reactivating
/// leaf can rebuild its in-memory projection by replaying the
/// authoritative write-ahead log from a persisted checkpoint.
/// <para>
/// The seam ships dormant - the leaf grain continues to durably write
/// through its existing storage provider on every commit, and no caller
/// drives <see cref="Apply"/> in the foreground today. The interface is
/// pinned ahead of the activation-path flip so the replay surface and
/// the checkpoint storage shape are stable before the commit point
/// changes.
/// </para>
/// <para>
/// <see cref="Apply"/> is intentionally synchronous and operates on a
/// by-ref <see cref="LatticeMutation"/>: it must produce identical
/// in-memory state for identical input across every silo so a parallel
/// replay on a freshly-activated leaf converges on the same projection
/// the authoritative writer reached. Side effects beyond mutating the
/// in-memory <c>Entries</c> dictionary (and the leaf's clock /
/// version-vector tracking required for downstream cache-delta
/// detection) are out of scope for the replay step - a checkpoint
/// advance via <see cref="SetCheckpointOffsetAsync"/> is the only
/// durable write the seam performs.
/// </para>
/// </summary>
internal interface ILeafProjection
{
    /// <summary>
    /// Replays a single durably-committed mutation against the leaf's
    /// in-memory projection using last-writer-wins semantics. Idempotent
    /// and commutative - replaying the same mutation twice, or replaying
    /// two mutations in either order, converges to the same projection
    /// as the original commit order. Does not advance the persisted
    /// checkpoint or write to durable storage.
    /// </summary>
    /// <param name="mutation">
    /// The mutation to apply. <see cref="MutationKind.Set"/> merges a
    /// live <c>LwwValue</c> for <see cref="LatticeMutation.Key"/>;
    /// <see cref="MutationKind.Delete"/> merges a tombstone for the same
    /// key; <see cref="MutationKind.DeleteRange"/> tombstones every
    /// existing entry whose key falls in
    /// <c>[<see cref="LatticeMutation.Key"/>, <see cref="LatticeMutation.EndExclusiveKey"/>)</c>.
    /// All metadata (timestamp, expiry, origin cluster, vector clock) is
    /// preserved verbatim from the mutation so the projection mirrors
    /// the authoritative committed entry byte-for-byte.
    /// </param>
    void Apply(in LatticeMutation mutation);

    /// <summary>
    /// Returns the highest write-ahead-log offset whose mutation has
    /// been durably applied to this leaf's projection. A reactivating
    /// leaf resumes replay from <c>offset + 1</c>.
    /// </summary>
    Task<long> GetCheckpointOffsetAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Persists <paramref name="offset"/> as the new projection
    /// checkpoint and durably commits any in-memory state advances since
    /// the previous checkpoint. <paramref name="offset"/> must be
    /// monotonically non-decreasing - the implementation rejects an
    /// attempt to roll the checkpoint backwards with
    /// <see cref="ArgumentOutOfRangeException"/>.
    /// </summary>
    Task SetCheckpointOffsetAsync(long offset, CancellationToken cancellationToken = default);
}
