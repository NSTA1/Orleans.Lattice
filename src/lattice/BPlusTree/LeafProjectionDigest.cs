namespace Orleans.Lattice;

/// <summary>
/// A deterministic XxHash128 fingerprint of a leaf's materialised projection,
/// used by operators and chaos tests to detect cross-silo divergence the
/// moment the per-shard write-ahead log (WAL) becomes the rebuild source
/// of truth. Two silos that have applied the same prefix of the
/// same WAL must produce byte-identical digests for every leaf.
/// <para>
/// The leaf-level digest is computed in two phases. First, every entry's
/// <c>(key, hlc.WallClockTicks, hlc.Counter, isTombstone, expiresAtTicks,
/// originClusterId, vector-clock-fingerprint, value)</c> tuple is hashed
/// to a 16-byte XxHash128 contribution; these contributions are XOR-folded
/// into a running hash on the leaf state, updated incrementally on every
/// mutation. Second, the public digest is the XxHash128 of
/// <c>(running_xor || entryCount || checkpointOffset)</c>. The XOR fold is
/// commutative and self-inverse, and XxHash128 outputs are uniformly
/// distributed, so the digest is independent of insertion order and the
/// leaf does not need to walk every entry on each call. XxHash128 is a
/// non-cryptographic hash chosen for ~10x lower CPU cost than SHA-256 on
/// the per-mutation hot path; the digest is a drift-detection fingerprint,
/// not an authentication tag. Determinism across silos is preserved
/// because the underlying <c>SortedDictionary&lt;string, LwwValue&lt;byte[]&gt;&gt;</c>
/// uses <see cref="StringComparer.Ordinal"/>, which is locale-independent
/// and stable across silos.
/// </para>
/// <para>
/// Shard-level digests are computed by chaining every leaf digest in the
/// shard's leaf chain through XxHash128 — the shard digest's <see cref="Hash"/>
/// covers the concatenation <c>XxHash128(leaf_1.Hash || leaf_2.Hash || ...)</c>,
/// <see cref="EntryCount"/> sums the per-leaf entry counts, and
/// <see cref="CheckpointOffset"/> is the sum of the per-leaf checkpoint
/// offsets so divergence at any leaf surfaces in the shard total.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafProjectionDigest)]
[Immutable]
public readonly record struct LeafProjectionDigest
{
    /// <summary>The XxHash128 hash bytes (16 bytes) of the leaf or shard projection.</summary>
    [Id(0)] public byte[] Hash { get; init; }

    /// <summary>
    /// Number of entries (live and tombstoned) folded into <see cref="Hash"/>.
    /// Reported alongside the hash so a digest mismatch can be triaged
    /// quickly: a different entry count points to applied-prefix divergence
    /// while an identical entry count with a different hash points to
    /// metadata or value drift.
    /// </summary>
    [Id(1)] public long EntryCount { get; init; }

    /// <summary>
    /// The persisted <c>ProjectionCheckpointOffset</c> at the time the
    /// digest was computed (or, for a shard digest, the sum across every
    /// leaf in the shard). Folded into <see cref="Hash"/> so two silos at
    /// different replay positions report distinct digests even if their
    /// post-state happens to coincide.
    /// </summary>
    [Id(2)] public long CheckpointOffset { get; init; }
}
