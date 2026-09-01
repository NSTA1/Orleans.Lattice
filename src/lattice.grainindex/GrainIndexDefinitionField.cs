namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// The individual fields of a grain-index declaration that the index registry
/// compares when it reconciles a live declaration against the record persisted
/// under it. Each member's documentation states whether a change to that field
/// is <b>drift-breaking</b> (it invalidates data already written under the old
/// declaration) or <b>drift-safe</b> (it does not), and
/// <see cref="GrainIndexDriftClassification"/> exposes the same classification
/// programmatically.
/// </summary>
public enum GrainIndexDefinitionField
{
    /// <summary>
    /// The logical index name.
    /// <para>
    /// <b>Drift-breaking.</b> The name is the index's identity and the key its
    /// registry record is filed under, so in practice it cannot drift: renaming
    /// an index declares a different index, which reconciles as a first run
    /// against an empty registry slot. The member exists so the classification
    /// is total over the declaration rather than silently omitting a field.
    /// </para>
    /// </summary>
    Name = 0,

    /// <summary>
    /// The lattice tree the index's entries are written to.
    /// <para>
    /// <b>Drift-breaking.</b> The entries written under the old declaration live
    /// in the old tree. Repointing the index leaves the reader querying a tree
    /// that holds none of them, so every query silently returns an incomplete
    /// result until the index is rebuilt.
    /// </para>
    /// </summary>
    TreeName = 1,

    /// <summary>
    /// The indexed grain interface type.
    /// <para>
    /// <b>Drift-breaking.</b> Every stored entry encodes the identity of a grain
    /// of the old type. Resolving those encoded keys as the new type addresses
    /// different grains, so query results point at the wrong activations.
    /// </para>
    /// </summary>
    GrainInterfaceType = 2,

    /// <summary>
    /// The grain-state type the index projects from.
    /// <para>
    /// <b>Drift-breaking.</b> The projected values already on the tree were read
    /// from the old state type. The same property name on a different state type
    /// is a different value, so comparisons against stored entries become
    /// meaningless.
    /// </para>
    /// </summary>
    StateType = 3,

    /// <summary>
    /// The codec that encodes an indexed grain's identity into the string an
    /// index entry stores, and decodes it back.
    /// <para>
    /// <b>Drift-breaking.</b> The stored keys were produced by the old codec.
    /// The new codec either cannot decode them at all or - worse - decodes them
    /// into a different grain, and its ordering scheme need not agree with the
    /// order the existing keys are already stored in.
    /// </para>
    /// </summary>
    KeyCodec = 4,

    /// <summary>
    /// The ordered set of projected properties, each with its declared CLR type.
    /// <para>
    /// <b>Drift-breaking.</b> The projected set is what an index entry encodes,
    /// so adding, removing, retyping, or reordering a property changes the shape
    /// of every entry the index would now write while leaving the old-shaped
    /// entries in place. A query that spans both shapes is quietly wrong.
    /// </para>
    /// </summary>
    Properties = 5,

    /// <summary>
    /// Whether the index's backing tree may be replicated across clusters.
    /// <para>
    /// <b>Drift-safe.</b> No part of an index entry's key or value encoding
    /// depends on the opt-in: it is a deployment policy that the startup
    /// replication guard audits against the resolved merge mode. Flipping it
    /// therefore updates the stored record and logs, rather than rejecting
    /// startup.
    /// </para>
    /// </summary>
    AllowReplication = 6,
}
