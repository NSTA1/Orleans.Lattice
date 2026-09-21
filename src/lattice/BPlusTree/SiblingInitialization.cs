namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The batched metadata a leaf-split donor stamps onto a freshly created
/// sibling in a single <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.InitializeSiblingAsync"/>
/// round-trip. Collapses the five separate gated setter RPCs the donor
/// used to issue (tree id, shard index, ownership key range, and the
/// next/prev sibling pointers) into one cross-grain call backed by a
/// single gate acquire and a single state persist on the sibling.
/// <para>
/// Every field carries the same idempotent semantics as the setter it
/// replaces: <see cref="TreeId"/> and <see cref="ShardIndex"/> are
/// write-once (a re-call against an already-seeded sibling is a no-op),
/// and the key-range low bound is the seeded sentinel. A
/// <see langword="null"/> <see cref="ShardIndex"/> means the donor's
/// own shard index was unset and the sibling should leave its slot
/// unseeded.
/// </para>
/// <para>
/// <see cref="MovedAwaySlots"/> and <see cref="MovedAwayVirtualShardCount"/>
/// replace no setter - they close issue 3121, where a sibling minted from a
/// sealed donor was born unsealed and served the migrated orphans the seal
/// exists to suppress. They are unioned rather than write-once, because a seal
/// is sticky and seeding must never drop one.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.SiblingInitialization)]
[Immutable]
internal readonly record struct SiblingInitialization
{
    /// <summary>The tree id the sibling is associated with (write-once).</summary>
    [Id(0)] public string TreeId { get; init; }

    /// <summary>The owning chain-shard index, or <see langword="null"/> when the donor's slot was unseeded.</summary>
    [Id(1)] public int? ShardIndex { get; init; }

    /// <summary>The inclusive low bound of the sibling's ownership range (the split key).</summary>
    [Id(2)] public string? LowKeyInclusive { get; init; }

    /// <summary>The exclusive high bound of the sibling's ownership range (the donor's pre-split high).</summary>
    [Id(3)] public string? HighKeyExclusive { get; init; }

    /// <summary>The sibling's right (next) sibling pointer.</summary>
    [Id(4)] public GrainId? NextSibling { get; init; }

    /// <summary>The sibling's left (prev) sibling pointer (the donor itself).</summary>
    [Id(5)] public GrainId? PrevSibling { get; init; }

    /// <summary>
    /// The donor's moved-away seal - the virtual slots that have migrated to
    /// another shard - or <see langword="null"/> when the donor holds no seal.
    /// <para>
    /// Carried because the seal is keyed by the key's HASH rather than by the
    /// leaf's declared range, so a sealed slot is a residue class scattered across
    /// the whole keyspace. Any non-trivial division of a sealed donor therefore
    /// hands the sibling part of a sealed residue class along with the rows, and a
    /// sibling born without the seal would serve the migrated orphans that the seal
    /// exists to suppress - permanently, because writes for those slots route to
    /// the destination shard and never reach it. Issue 3121.
    /// </para>
    /// <para>
    /// The receiver unions this into its own seal via
    /// <see cref="MovedAwaySealInheritance.TryInherit"/> rather than assigning it,
    /// so seeding stays monotonic and a re-call against a partially seeded sibling
    /// cannot drop a slot. The donor keeps its own seal: this is a copy, not a
    /// move.
    /// </para>
    /// <para>
    /// <b>The array arrives shared on the same-silo path</b> - this type is
    /// <see cref="ImmutableAttribute"/>, so Orleans hands the reference through
    /// without a deep copy. The receiver therefore copies it before retaining it in
    /// durable state, because a leaf that persisted the donor's own array would alias
    /// two leaves' persisted state to one instance. That copy is the receiver's
    /// responsibility, not this type's.
    /// </para>
    /// </summary>
    [Id(6)] public int[]? MovedAwaySlots { get; init; }

    /// <summary>
    /// The virtual shard count <see cref="MovedAwaySlots"/> was recorded under, or
    /// <see langword="null"/> when the donor holds no seal. A slot index only has
    /// meaning under its own count, so the two always travel together.
    /// </summary>
    [Id(7)] public int? MovedAwayVirtualShardCount { get; init; }

    /// <summary>
    /// The per-partition WAL head offsets the donor captured at split time, or
    /// <see langword="null"/> when the donor could not capture them (no tree id
    /// bound). Indexed by WAL partition, in the same offset space as
    /// <c>ILeafProjection.SetCheckpointOffsetAsync</c> - these are the very
    /// values the donor goes on to stamp through
    /// <c>SetCheckpointOffsetHintsAsync</c>.
    /// <para>
    /// <b>Why the birth pin needs them (issue #3094).</b> The sibling seeds a
    /// durable materialiser pin before the donor's <c>MergeEntriesAsync</c>
    /// makes its rows reachable in the WAL. That pin used to carry the "-1"
    /// no-offset sentinel, which leaves the pin outside the WAL GC's offset
    /// coverage set and so protected <i>only</i> by the Zero-HLC block-pin
    /// branch - and that branch disables the cursor trim for the entire tree.
    /// Because leaf keys hash across every partition, a single newborn leaf
    /// blocked all of them, and splits admit newborns continuously, so a
    /// growing tree could never reclaim. Carrying the captured heads lets the
    /// seed publish a real offset instead, which is both an accurate retention
    /// floor (the sibling's rows are appended at or above the head) and enough
    /// to bring the pin inside the offset coverage set.
    /// </para>
    /// <para>
    /// A head of <c>0</c> (or a negative) is not a usable checkpoint offset and
    /// is left as the sentinel, mirroring the <c>donorHead &gt; 0</c> guard the
    /// donor's own checkpoint advance applies to the same array.
    /// </para>
    /// </summary>
    [Id(8)] public long[]? WalHeadsAtBirth { get; init; }
}
