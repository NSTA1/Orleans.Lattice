namespace Orleans.Lattice.Auth;

/// <summary>
/// Identifies the region of the keyspace an authorization rule governs: an entire
/// tree, a single key within a tree, or every key sharing a prefix within a tree.
/// Modelled as a small discriminated shape - a <see cref="Kind"/> discriminator,
/// the always-present <see cref="TreeId"/>, and an optional
/// <see cref="KeyOrPrefix"/> - and constructed through the
/// <see cref="Tree(string)"/>, <see cref="Key(string, string)"/> and
/// <see cref="Prefix(string, string)"/> factory methods. The
/// <see cref="TreeId"/> is always present so rules for a tree can be retrieved by
/// a single prefix scan of the policy store. Persisted as part of a
/// <see cref="LatticeAuthorizationRule"/>.
/// </summary>
[GenerateSerializer]
[Alias(AuthTypeAliases.LatticeScope)]
[Immutable]
public sealed record LatticeScope
{
    /// <summary>
    /// Initializes a new <see cref="LatticeScope"/>. Prefer the
    /// <see cref="Tree(string)"/> / <see cref="Key(string, string)"/> /
    /// <see cref="Prefix(string, string)"/> factory methods; this constructor
    /// exists for serialization and exhaustive construction.
    /// </summary>
    /// <param name="kind">The extent of the tree the scope covers.</param>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="keyOrPrefix">
    /// The exact key (for <see cref="LatticeScopeKind.Key"/>) or key prefix (for
    /// <see cref="LatticeScopeKind.Prefix"/>). Must be <c>null</c> for
    /// <see cref="LatticeScopeKind.Tree"/> and non-<c>null</c> otherwise.
    /// </param>
    /// <exception cref="ArgumentException">
    /// <paramref name="treeId"/> is <c>null</c> or empty, or
    /// <paramref name="keyOrPrefix"/> is inconsistent with <paramref name="kind"/>.
    /// </exception>
    public LatticeScope(LatticeScopeKind kind, string treeId, string? keyOrPrefix = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        switch (kind)
        {
            case LatticeScopeKind.Tree when keyOrPrefix is not null:
                throw new ArgumentException(
                    "A Tree scope must not carry a key or prefix.", nameof(keyOrPrefix));
            case LatticeScopeKind.Key when string.IsNullOrEmpty(keyOrPrefix):
                throw new ArgumentException(
                    "A Key scope requires a non-empty key.", nameof(keyOrPrefix));
            case LatticeScopeKind.Prefix when string.IsNullOrEmpty(keyOrPrefix):
                throw new ArgumentException(
                    "A Prefix scope requires a non-empty prefix.", nameof(keyOrPrefix));
        }

        Kind = kind;
        TreeId = treeId;
        KeyOrPrefix = keyOrPrefix;
    }

    /// <summary>The extent of the tree this scope covers.</summary>
    [Id(0)]
    public LatticeScopeKind Kind { get; init; }

    /// <summary>The governed tree id. Always present.</summary>
    [Id(1)]
    public string TreeId { get; init; }

    /// <summary>
    /// The exact key (when <see cref="Kind"/> is <see cref="LatticeScopeKind.Key"/>)
    /// or the key prefix (when it is <see cref="LatticeScopeKind.Prefix"/>);
    /// <c>null</c> for a whole-tree scope.
    /// </summary>
    [Id(2)]
    public string? KeyOrPrefix { get; init; }

    /// <summary>Creates a scope covering the entire tree <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <returns>A whole-tree scope.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public static LatticeScope Tree(string treeId) =>
        new(LatticeScopeKind.Tree, treeId);

    /// <summary>Creates a scope covering the single key <paramref name="key"/> within <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="key">The exact key. Must not be <c>null</c> or empty.</param>
    /// <returns>A single-key scope.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> or <paramref name="key"/> is <c>null</c> or empty.</exception>
    public static LatticeScope Key(string treeId, string key) =>
        new(LatticeScopeKind.Key, treeId, key);

    /// <summary>Creates a scope covering every key beginning with <paramref name="prefix"/> within <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="prefix">The key prefix. Must not be <c>null</c> or empty.</param>
    /// <returns>A prefix scope.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> or <paramref name="prefix"/> is <c>null</c> or empty.</exception>
    public static LatticeScope Prefix(string treeId, string prefix) =>
        new(LatticeScopeKind.Prefix, treeId, prefix);

    /// <summary>
    /// The sentinel tree id representing <b>every tree / no specific tree</b>: the
    /// all-trees wildcard used to author a <b>cluster-wide, scopeless</b> grant
    /// such as <see cref="LatticeOperation.Telemetry"/>. Because the authorization
    /// model always keys a rule by a tree id, a capability that is not attached to
    /// any single tree is represented as an ordinary <see cref="Tree(string)"/>
    /// scope over this well-known sentinel, and the matching access-gate request
    /// targets the same sentinel. It is deliberately a value that no real
    /// application tree is expected to use; even were it to collide with a real
    /// tree name the collision is harmless, because a scopeless capability bit
    /// (for example <see cref="LatticeOperation.Telemetry"/>) never overlaps a
    /// data-plane operation bit, so a data-plane rule on the tree can never grant
    /// the scopeless capability and a scopeless grant can never grant a data-plane
    /// operation.
    /// </summary>
    public const string ClusterWideTreeId = "*";

    /// <summary>
    /// Creates the <b>cluster-wide, scopeless</b> scope over
    /// <see cref="ClusterWideTreeId"/>. Use it to author a grant for a capability
    /// that is not attached to any single tree - notably
    /// <see cref="LatticeOperation.Telemetry"/>. The returned value is an ordinary
    /// whole-tree scope over the all-trees sentinel, so it compiles, persists, and
    /// evaluates through the standard policy pipeline with no special-casing.
    /// </summary>
    /// <returns>A whole-tree scope over the all-trees sentinel id.</returns>
    public static LatticeScope ClusterWide() =>
        new(LatticeScopeKind.Tree, ClusterWideTreeId);
}
