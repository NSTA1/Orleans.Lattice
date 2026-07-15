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

    private LatticeApiMcpAccessSet(int mask) => _mask = mask;

    /// <summary>An access set granting none of the facade groups (fail-closed default).</summary>
    public static LatticeApiMcpAccessSet None => new(0);

    /// <summary>Whether no group is granted.</summary>
    public bool IsEmpty => _mask == 0;

    /// <summary>Returns a copy of this set with <paramref name="group"/> added.</summary>
    public LatticeApiMcpAccessSet With(LatticeApiMcpGroup group) => new(_mask | Bit(group));

    /// <summary>Whether <paramref name="group"/> is a member of the set.</summary>
    public bool Contains(LatticeApiMcpGroup group) => (_mask & Bit(group)) != 0;

    private static int Bit(LatticeApiMcpGroup group) => 1 << (int)group;

    /// <inheritdoc />
    public bool Equals(LatticeApiMcpAccessSet other) => _mask == other._mask;

    /// <inheritdoc />
    public override bool Equals(object? obj) => obj is LatticeApiMcpAccessSet other && Equals(other);

    /// <inheritdoc />
    public override int GetHashCode() => _mask;
}
