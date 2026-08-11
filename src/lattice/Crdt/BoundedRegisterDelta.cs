namespace Orleans.Lattice;

/// <summary>
/// Typed delta record for a monotonic bounded register mutation, shared by both
/// the <c>Max</c> and <c>Min</c> directions. Carries the candidate
/// <see cref="Value"/> and its total-order <see cref="OrderKey"/>; the direction
/// is not carried on the delta because the receiver's register already knows its
/// direction (<see cref="BoundedRegister.IsMin"/>), stamped when the empty state
/// was created for the tree's declared merge mode.
/// <para>
/// Apply semantics on the receiver: fold the candidate through
/// <see cref="BoundedRegister.MergeDelta(BoundedRegisterDelta)"/>, which advances
/// the register only when the candidate beats the current value under the
/// register's direction. The result is independent of arrival order, duplicate
/// delivery, and partial overlap with the local state, so the merge is
/// commutative, associative, and idempotent.
/// </para>
/// <para>
/// Emitters populate <see cref="Value"/> and <see cref="OrderKey"/> and set
/// <see cref="HasValue"/>; use <see cref="Empty"/> to author a no-op delta. The
/// <see langword="default"/> instance carries no candidate and is a valid no-op.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.BoundedRegisterDelta)]
[Immutable]
public readonly record struct BoundedRegisterDelta
{
    /// <summary>The candidate value bytes the producing write proposes, or <see langword="null"/> for a no-op delta.</summary>
    [Id(0)] public byte[]? Value { get; init; }

    /// <summary>The candidate's order-preserving total-order key, or <see langword="null"/> for a no-op delta.</summary>
    [Id(1)] public byte[]? OrderKey { get; init; }

    /// <summary>
    /// <see langword="true"/> when this delta carries a candidate value to fold.
    /// A delta with <see langword="false"/> is a no-op regardless of the other
    /// fields.
    /// </summary>
    [Id(2)] public bool HasValue { get; init; }

    /// <summary>A reusable no-op delta that carries no candidate.</summary>
    public static BoundedRegisterDelta Empty { get; } = new();
}
