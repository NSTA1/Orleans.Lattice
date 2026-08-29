namespace Orleans.Lattice.Tenancy;

/// <summary>
/// A cross-tenant grant: an authorization the granting tenant issues so a
/// principal outside it - a single caller subject or an entire other tenant -
/// may perform a set of operations against a named scope of the granting
/// tenant's data.
/// </summary>
/// <remarks>
/// The grant's identity, <see cref="GrantId"/>, is derived deterministically
/// from the grantee kind, the grantee, and the scope - <b>not</b> the operation
/// set - so re-issuing a grant to the same principal on the same scope with a
/// different operation set updates the existing grant in place rather than
/// creating a second one. This is a definition record held in the registry;
/// evaluating a grant at access time is a separate concern.
/// </remarks>
[GenerateSerializer]
[Alias(TenantTypeAliases.CrossTenantGrant)]
[Immutable]
public readonly record struct CrossTenantGrant
{
    /// <summary>The separator between the identity-bearing fields of <see cref="GrantId"/>.</summary>
    private const char IdentitySeparator = '\u001f';

    /// <summary>
    /// The principal the grant is issued to: a caller subject id when
    /// <see cref="GranteeKind"/> is <see cref="TenantGranteeKind.Subject"/>, or a
    /// tenant id when it is <see cref="TenantGranteeKind.Tenant"/>.
    /// </summary>
    [Id(0)]
    public string Grantee { get; init; }

    /// <summary>Whether <see cref="Grantee"/> names a subject or a whole tenant.</summary>
    [Id(1)]
    public TenantGranteeKind GranteeKind { get; init; }

    /// <summary>
    /// The scope of the granting tenant's data the grant applies to - a tree name
    /// or tree-name prefix, interpreted by the enforcement layer.
    /// </summary>
    [Id(2)]
    public string Scope { get; init; }

    /// <summary>The operations the grant authorizes on <see cref="Scope"/>.</summary>
    [Id(3)]
    public TenantGrantOperations Operations { get; init; }

    /// <summary>
    /// The grant's lifecycle state. Only <see cref="TenantGrantState.Active"/>
    /// authorizes anything; an offered-but-unapproved, declined, or withdrawn
    /// grant resolves to a denial. Added additively, so a grant persisted before
    /// this field existed reads back as
    /// <see cref="TenantGrantState.Active"/> - see the remarks on
    /// <see cref="TenantGrantState"/> for why that is the safe default.
    /// </summary>
    [Id(4)]
    public TenantGrantState State { get; init; }

    /// <summary>
    /// Creates a cross-tenant grant that is already in force
    /// (<see cref="TenantGrantState.Active"/>). This is the pre-existing overload
    /// and its meaning is unchanged; use
    /// <see cref="Create(string, TenantGranteeKind, string, TenantGrantOperations, TenantGrantState)"/>
    /// to offer a grant into <see cref="TenantGrantState.Pending"/> for the
    /// grantee to approve.
    /// </summary>
    /// <param name="grantee">The subject id or tenant id the grant is issued to. Must not be <c>null</c>.</param>
    /// <param name="granteeKind">Whether <paramref name="grantee"/> is a subject or a tenant.</param>
    /// <param name="scope">The scope (tree name or prefix) the grant applies to. Must not be <c>null</c>.</param>
    /// <param name="operations">The operations the grant authorizes.</param>
    /// <returns>The constructed grant.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="grantee"/> or <paramref name="scope"/> is <c>null</c>.</exception>
    public static CrossTenantGrant Create(
        string grantee,
        TenantGranteeKind granteeKind,
        string scope,
        TenantGrantOperations operations) =>
        Create(grantee, granteeKind, scope, operations, TenantGrantState.Active);

    /// <summary>
    /// Creates a cross-tenant grant in an explicit lifecycle state.
    /// </summary>
    /// <param name="grantee">The subject id or tenant id the grant is issued to. Must not be <c>null</c>.</param>
    /// <param name="granteeKind">Whether <paramref name="grantee"/> is a subject or a tenant.</param>
    /// <param name="scope">The scope (tree name or prefix) the grant applies to. Must not be <c>null</c>.</param>
    /// <param name="operations">The operations the grant authorizes.</param>
    /// <param name="state">The lifecycle state to create the grant in.</param>
    /// <returns>The constructed grant.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="grantee"/> or <paramref name="scope"/> is <c>null</c>.</exception>
    public static CrossTenantGrant Create(
        string grantee,
        TenantGranteeKind granteeKind,
        string scope,
        TenantGrantOperations operations,
        TenantGrantState state)
    {
        ArgumentNullException.ThrowIfNull(grantee);
        ArgumentNullException.ThrowIfNull(scope);
        return new CrossTenantGrant
        {
            Grantee = grantee,
            GranteeKind = granteeKind,
            Scope = scope,
            Operations = operations,
            State = state,
        };
    }

    /// <summary>
    /// The deterministic identity of this grant: <c>{kind}:{grantee}\u001f{scope}</c>.
    /// Two grants with the same grantee kind, grantee, and scope share a
    /// <see cref="GrantId"/> (and so the same registry slot) regardless of their
    /// operation sets.
    /// </summary>
    public string GrantId =>
        $"{(int)GranteeKind}:{Grantee ?? string.Empty}{IdentitySeparator}{Scope ?? string.Empty}";
}
