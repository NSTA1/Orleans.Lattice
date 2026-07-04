namespace Orleans.Lattice.Auth;

/// <summary>
/// Identifies the principal an authorization rule applies to: either a single
/// user or a group (matching every transitive member of that group). Modelled as
/// a small discriminated shape - a <see cref="Kind"/> discriminator plus the
/// target <see cref="Id"/> - and constructed through the
/// <see cref="User(string)"/> and <see cref="Group(string)"/> factory methods.
/// Persisted as part of a <see cref="LatticeAuthorizationRule"/>.
/// </summary>
[GenerateSerializer]
[Alias(AuthTypeAliases.LatticeSubjectSelector)]
[Immutable]
public sealed record LatticeSubjectSelector
{
    /// <summary>
    /// Initializes a new <see cref="LatticeSubjectSelector"/>. Prefer the
    /// <see cref="User(string)"/> / <see cref="Group(string)"/> factory methods;
    /// this constructor exists for serialization and exhaustive construction.
    /// </summary>
    /// <param name="kind">Whether the selector targets a user or a group.</param>
    /// <param name="id">The target user id or group id. Must not be <c>null</c> or empty.</param>
    /// <exception cref="ArgumentException"><paramref name="id"/> is <c>null</c> or empty.</exception>
    public LatticeSubjectSelector(LatticeSubjectSelectorKind kind, string id)
    {
        ArgumentException.ThrowIfNullOrEmpty(id);
        Kind = kind;
        Id = id;
    }

    /// <summary>Whether the selector targets a single user or a group.</summary>
    [Id(0)]
    public LatticeSubjectSelectorKind Kind { get; init; }

    /// <summary>The target user id (when <see cref="Kind"/> is <see cref="LatticeSubjectSelectorKind.User"/>) or group id (when it is <see cref="LatticeSubjectSelectorKind.Group"/>).</summary>
    [Id(1)]
    public string Id { get; init; }

    /// <summary>Creates a selector matching the single user with id <paramref name="userId"/>.</summary>
    /// <param name="userId">The user id. Must not be <c>null</c> or empty.</param>
    /// <returns>A user selector.</returns>
    /// <exception cref="ArgumentException"><paramref name="userId"/> is <c>null</c> or empty.</exception>
    public static LatticeSubjectSelector User(string userId) =>
        new(LatticeSubjectSelectorKind.User, userId);

    /// <summary>Creates a selector matching every transitive member of the group with id <paramref name="groupId"/>.</summary>
    /// <param name="groupId">The group id. Must not be <c>null</c> or empty.</param>
    /// <returns>A group selector.</returns>
    /// <exception cref="ArgumentException"><paramref name="groupId"/> is <c>null</c> or empty.</exception>
    public static LatticeSubjectSelector Group(string groupId) =>
        new(LatticeSubjectSelectorKind.Group, groupId);
}
