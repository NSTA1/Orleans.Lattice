using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice;

/// <summary>
/// Thrown by the public <see cref="ILattice"/> write surface when a write
/// would violate the <b>single-shape-per-replicated-tree</b> invariant.
/// <para>
/// A tree replicated by <c>Orleans.Lattice.Replication</c> declares exactly
/// one <see cref="LatticeMergeMode"/> in
/// <c>LatticeReplicationOptions.ReplicatedTrees</c>. That single mode is
/// stamped onto every shipped <see cref="WalRecord"/> and the receiver
/// re-stamps every decoded entry with it - there is no per-key mode on the
/// wire. A replicated tree is therefore single-shape by construction: every
/// value in it must be authored under the declared mode. A write that uses a
/// different shape (a CRDT accessor whose mode differs from the declared
/// mode, or a plain last-writer-wins write to a tree declared as a typed CRDT
/// mode) would ship bytes the receiver cannot decode under the declared
/// shape, faulting the apply and parking the entry on the peer's dead-letter
/// queue while local reads stayed silently correct.
/// </para>
/// <para>
/// This guard converts that silent, after-the-fact remote failure into an
/// immediate, local error at the offending call site. It fires only when the
/// tree is declared for replication (the
/// <see cref="ILatticeMergeModeResolver"/> returns a non-<c>null</c> mode);
/// trees that are not replicated - including every tree on a single-cluster
/// host - are unaffected. See
/// <see href="../../docs/lattice.replication/replication-modes.md">Replication
/// modes</see> for the full invariant.
/// </para>
/// <para>
/// Derives from <see cref="System.InvalidOperationException"/> so existing
/// catch handlers that match on <see cref="System.InvalidOperationException"/>
/// continue to absorb it; the typed slot lets callers that care about the
/// configuration mismatch distinguish it explicitly.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeReplicationModeMismatch)]
public sealed class LatticeReplicationModeMismatchException : InvalidOperationException
{
    /// <summary>
    /// Logical tree id whose declared replication mode the write violated.
    /// Empty on the parameterless constructor.
    /// </summary>
    [Id(0)]
    public string TreeId { get; }

    /// <summary>
    /// The <see cref="LatticeMergeMode"/> the tree is declared with in
    /// <c>LatticeReplicationOptions.ReplicatedTrees</c> - the only shape the
    /// tree may hold.
    /// </summary>
    [Id(1)]
    public LatticeMergeMode DeclaredMode { get; }

    /// <summary>
    /// The <see cref="LatticeMergeMode"/> the rejected write attempted.
    /// <see cref="LatticeMergeMode.LwwRegister"/> denotes a plain
    /// value/tombstone write (for example <c>SetAsync</c> / <c>DeleteAsync</c>).
    /// </summary>
    [Id(2)]
    public LatticeMergeMode AttemptedMode { get; }

    /// <summary>
    /// Initialises a new instance with no diagnostic message and empty
    /// context. Provided to satisfy the framework's exception construction
    /// contract; production throw sites use the context-carrying overload.
    /// </summary>
    public LatticeReplicationModeMismatchException()
    {
        TreeId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and
    /// empty context.
    /// </summary>
    /// <param name="message">Diagnostic context describing the rejected write.</param>
    public LatticeReplicationModeMismatchException(string message) : base(message)
    {
        TreeId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and
    /// wrapped inner exception, and empty context.
    /// </summary>
    /// <param name="message">Diagnostic context describing the rejected write.</param>
    /// <param name="innerException">The underlying cause.</param>
    public LatticeReplicationModeMismatchException(string message, Exception innerException)
        : base(message, innerException)
    {
        TreeId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance carrying the originating tree id, the
    /// tree's declared replication mode, and the mode the rejected write
    /// attempted. The primary production throw shape.
    /// </summary>
    /// <param name="message">Diagnostic context describing the rejected write.</param>
    /// <param name="treeId">Logical tree id whose declared mode the write violated.</param>
    /// <param name="declaredMode">The mode the tree is declared with in <c>ReplicatedTrees</c>.</param>
    /// <param name="attemptedMode">The mode the rejected write attempted.</param>
    public LatticeReplicationModeMismatchException(
        string message,
        string treeId,
        LatticeMergeMode declaredMode,
        LatticeMergeMode attemptedMode) : base(message)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        TreeId = treeId;
        DeclaredMode = declaredMode;
        AttemptedMode = attemptedMode;
    }
}
