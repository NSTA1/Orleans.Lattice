namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// An immutable set of the <see cref="LatticeApiMcpGroup"/> values a caller may
/// use, resolved once per session by an
/// <see cref="ILatticeApiMcpPermissionResolver"/> and consulted by the discovery
/// core to filter the advertised tool set. Backed by a small bitmask so
/// membership tests allocate nothing on the per-session filtering path.
/// </summary>
internal readonly struct LatticeApiMcpAccessSet : IEquatable<LatticeApiMcpAccessSet>
{
    private readonly int _mask;
    private readonly LatticeOperation _granted;

    private LatticeApiMcpAccessSet(int mask, LatticeOperation granted)
    {
        _mask = mask;
        _granted = granted;
    }

    /// <summary>An access set granting none of the facade groups (fail-closed default).</summary>
    public static LatticeApiMcpAccessSet None => new(0, LatticeOperation.None);

    /// <summary>Whether no group is granted.</summary>
    public bool IsEmpty => _mask == 0;

    /// <summary>
    /// The union of every operation the caller holds an <c>Allow</c> grant for.
    /// A group's coarse mask admits the group on <i>any</i> intersecting
    /// operation, so a data-plane group is usable on a bare read grant; carrying
    /// the granted operations lets the discovery core apply a per-tool minimum
    /// inside such a group and withhold its mutating tools from a read-only
    /// caller.
    /// </summary>
    public LatticeOperation GrantedOperations => _granted;

    /// <summary>
    /// Whether the resolver populated <see cref="GrantedOperations"/>. A set that
    /// carries no operation detail cannot support a per-tool minimum, so the
    /// discovery core applies group-level filtering alone for it, exactly as
    /// before this refinement existed.
    /// </summary>
    public bool CarriesOperationDetail => _granted != LatticeOperation.None;

    /// <summary>Returns a copy of this set with <paramref name="group"/> added.</summary>
    public LatticeApiMcpAccessSet With(LatticeApiMcpGroup group) => new(_mask | Bit(group), _granted);

    /// <summary>
    /// Returns a copy of this set with <paramref name="operations"/> unioned into
    /// <see cref="GrantedOperations"/>, leaving group membership untouched.
    /// </summary>
    public LatticeApiMcpAccessSet WithOperations(LatticeOperation operations)
        => new(_mask, _granted | operations);

    /// <summary>Whether <paramref name="group"/> is a member of the set.</summary>
    public bool Contains(LatticeApiMcpGroup group) => (_mask & Bit(group)) != 0;

    private static int Bit(LatticeApiMcpGroup group) => 1 << (int)group;

    /// <inheritdoc />
    public bool Equals(LatticeApiMcpAccessSet other)
        => _mask == other._mask && _granted == other._granted;

    /// <inheritdoc />
    public override bool Equals(object? obj) => obj is LatticeApiMcpAccessSet other && Equals(other);

    /// <inheritdoc />
    public override int GetHashCode() => HashCode.Combine(_mask, (int)_granted);
}
