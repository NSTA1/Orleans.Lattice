namespace Orleans.Lattice.Api.Data;

/// <summary>
/// The read-only result of a data-plane capability probe: whether the current
/// caller may write or delete in a tree, or at one key of it, evaluated through
/// the same fail-closed access gate the real writes use but with <b>no side
/// effects</b>. Every flag is default-deny: it is <see langword="true"/> only when
/// the gate would let the corresponding write through.
/// </summary>
/// <remarks>
/// <para>
/// The probe is a UX affordance, so a management surface can hide the write
/// controls a caller cannot use; it is <b>not</b> a security boundary. The data
/// plane still authorizes every real write fail-closed on attempt, so a client
/// that acts on a stale or wrong flag is still refused by the server.
/// </para>
/// <para>
/// Without a <see cref="Key"/> the flags answer "may the caller write (or delete)
/// at least one key of the tree", so a caller holding only a prefix or exact-key
/// grant still reports <see langword="true"/>. With a key they answer for that
/// one key exactly as the write path would.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(DataApiTypeAliases.LatticeDataCapabilities)]
[Immutable]
public sealed record LatticeDataCapabilities
{
    /// <summary>The tree id these capabilities were evaluated over, as the caller supplied it.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The key the capabilities were evaluated at, or <see langword="null"/> for the whole tree.</summary>
    [Id(1)] public string? Key { get; init; }

    /// <summary>
    /// <see langword="true"/> when the caller may write (<see cref="LatticeOperation.Write"/>)
    /// the probed key, or at least one key of the tree when no key was probed.
    /// </summary>
    [Id(2)] public bool CanWrite { get; init; }

    /// <summary>
    /// <see langword="true"/> when the caller may delete (<see cref="LatticeOperation.Delete"/>)
    /// the probed key, or at least one key of the tree when no key was probed.
    /// </summary>
    [Id(3)] public bool CanDelete { get; init; }
}
