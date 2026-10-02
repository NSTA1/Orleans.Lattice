namespace Orleans.Lattice;

/// <summary>
/// The identity of a <b>tenant group</b>: a membership group that a tenant's
/// administrators create and manage inside their own tenant. A tenant group id
/// has the reserved grammar <c>t/{tenant}/{name}</c>, where <c>{tenant}</c> is a
/// valid, non-<see cref="TenantId.Default"/> <see cref="TenantId"/> and
/// <c>{name}</c> is 1 to <see cref="MaxNameLength"/> characters of lower-case
/// ASCII letters, digits, <c>-</c>, <c>_</c> and <c>.</c>. This type is the single
/// owner of that grammar, so membership, authorization and tenancy agree on
/// which group ids belong to the tenant tier.
/// </summary>
/// <remarks>
/// <para>
/// Tenant groups share the cluster's <c>sys-membership-*</c> trees with cluster
/// groups and are told apart by this grammar alone. The <c>t/</c> grammar is
/// therefore reserved to the tenant tier: a cluster group may not be created
/// with an id in it, and an identity-provider-asserted group claim in it is
/// ignored, so no one outside the tenant tier can mint or assert a tenant group.
/// </para>
/// <para>
/// The reserved <see cref="TenantId.Default"/> tenant (the legacy-adoption
/// tenant) has no tenant groups, so <c>t/default/{name}</c> is not a tenant group
/// id: <see cref="TryParse"/> and <see cref="IsTenantGroupId"/> reject it, and
/// <see cref="Compose"/> refuses it.
/// </para>
/// <para>
/// The uninitialised value (<c>default(LatticeTenantGroupId)</c>) carries a
/// <c>null</c> <see cref="Value"/> and represents "no group". Construct a valid
/// instance through <see cref="Parse"/>, <see cref="TryParse"/> or
/// <see cref="Compose"/>. Equality is ordinal.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.LatticeTenantGroupId)]
[Immutable]
public readonly record struct LatticeTenantGroupId
{
    /// <summary>The maximum length, in characters, of the tenant-local <see cref="Name"/>.</summary>
    public const int MaxNameLength = 63;

    private LatticeTenantGroupId(TenantId tenant, string name, string value)
    {
        Tenant = tenant;
        Name = name;
        Value = value;
    }

    /// <summary>The tenant that owns the group.</summary>
    [Id(0)]
    public TenantId Tenant { get; private init; }

    /// <summary>
    /// The tenant-local group name (the <c>{name}</c> segment). <c>null</c> only
    /// for the uninitialised <c>default(LatticeTenantGroupId)</c>.
    /// </summary>
    [Id(1)]
    public string Name { get; private init; }

    /// <summary>
    /// The full group id, <c>t/{tenant}/{name}</c>: the id the group is stored and
    /// referenced by. <c>null</c> only for the uninitialised
    /// <c>default(LatticeTenantGroupId)</c>.
    /// </summary>
    [Id(2)]
    public string Value { get; private init; }

    /// <summary>
    /// Composes the tenant group id <c>t/{tenant}/{name}</c> for a group owned by
    /// <paramref name="tenant"/>.
    /// </summary>
    /// <param name="tenant">The owning tenant. Must be an initialised tenant other than <see cref="TenantId.Default"/>.</param>
    /// <param name="name">The tenant-local group name. Must match the name grammar documented on <see cref="LatticeTenantGroupId"/>.</param>
    /// <returns>The composed tenant group id.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="tenant"/> is the uninitialised "no tenant" value or the
    /// reserved <see cref="TenantId.Default"/> tenant, or <paramref name="name"/>
    /// does not match the name grammar.
    /// </exception>
    public static LatticeTenantGroupId Compose(TenantId tenant, string name)
    {
        ArgumentNullException.ThrowIfNull(name);

        if (tenant.Value is null || !TenantId.IsValid(tenant.Value.AsSpan()))
        {
            throw new ArgumentException(
                "Cannot compose a tenant group id from the uninitialised 'no tenant' value.",
                nameof(tenant));
        }

        if (tenant.IsDefault)
        {
            throw new ArgumentException(
                $"The reserved '{TenantId.DefaultId}' tenant has no tenant groups; its access is operator-administered.",
                nameof(tenant));
        }

        if (!IsValidName(name.AsSpan()))
        {
            throw new ArgumentException(
                $"'{name}' is not a valid tenant group name. A tenant group name must be 1 to {MaxNameLength} "
                + "characters of lower-case ASCII letters, digits, '-', '_' and '.'.",
                nameof(name));
        }

        return new LatticeTenantGroupId(
            tenant,
            name,
            string.Concat(LatticeTenantTrees.SegmentPrefix, tenant.Value, "/", name));
    }

    /// <summary>
    /// Parses <paramref name="value"/> into a <see cref="LatticeTenantGroupId"/>,
    /// throwing when it is not a tenant group id.
    /// </summary>
    /// <param name="value">The candidate group id. Must not be <c>null</c>.</param>
    /// <returns>The parsed tenant group id.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="value"/> is <c>null</c>.</exception>
    /// <exception cref="FormatException"><paramref name="value"/> is not a tenant group id.</exception>
    public static LatticeTenantGroupId Parse(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        if (!TryParse(value, out var groupId))
        {
            throw new FormatException(
                $"'{value}' is not a valid tenant group id. A tenant group id has the form "
                + $"'{LatticeTenantTrees.SegmentPrefix}{{tenant}}/{{name}}', where {{tenant}} is a valid tenant id "
                + $"other than '{TenantId.DefaultId}' and {{name}} is 1 to {MaxNameLength} characters of "
                + "lower-case ASCII letters, digits, '-', '_' and '.'.");
        }

        return groupId;
    }

    /// <summary>
    /// Attempts to parse <paramref name="value"/> into a
    /// <see cref="LatticeTenantGroupId"/> without throwing.
    /// </summary>
    /// <param name="value">The candidate group id, or <c>null</c>.</param>
    /// <param name="groupId">
    /// The parsed tenant group id when this returns <c>true</c>; otherwise
    /// <c>default</c>.
    /// </param>
    /// <returns><c>true</c> when <paramref name="value"/> is a tenant group id; otherwise <c>false</c>.</returns>
    public static bool TryParse(string? value, out LatticeTenantGroupId groupId)
    {
        if (value is not null && TrySplit(value.AsSpan(), out var tenant, out var name))
        {
            groupId = new LatticeTenantGroupId(
                TenantId.ForValidated(new string(tenant)),
                new string(name),
                value);
            return true;
        }

        groupId = default;
        return false;
    }

    /// <summary>
    /// Returns <c>true</c> when <paramref name="id"/> is a well-formed tenant group
    /// id, exactly when <see cref="TryParse"/> would succeed. A cheap prefix and
    /// shape test that allocates nothing, so it is safe on a per-claim or
    /// per-subject path.
    /// </summary>
    /// <param name="id">The candidate id, or <c>null</c>.</param>
    /// <returns><c>true</c> when <paramref name="id"/> is a tenant group id; otherwise <c>false</c>.</returns>
    public static bool IsTenantGroupId(string? id) =>
        id is not null && TrySplit(id.AsSpan(), out _, out _);

    /// <summary>Returns the full group id (empty for the uninitialised value).</summary>
    /// <returns>The <see cref="Value"/>, or the empty string for <c>default(LatticeTenantGroupId)</c>.</returns>
    public override string ToString() => Value ?? string.Empty;

    /// <summary>
    /// Validates a tenant-local group name against the name grammar without
    /// allocating.
    /// </summary>
    internal static bool IsValidName(ReadOnlySpan<char> name)
    {
        if (name.Length is < 1 or > MaxNameLength)
        {
            return false;
        }

        foreach (var c in name)
        {
            var allowed = c is (>= 'a' and <= 'z') or (>= '0' and <= '9') or '-' or '_' or '.';
            if (!allowed)
            {
                return false;
            }
        }

        return true;
    }

    private static bool TrySplit(ReadOnlySpan<char> id, out ReadOnlySpan<char> tenant, out ReadOnlySpan<char> name)
    {
        tenant = default;
        name = default;

        if (!id.StartsWith(LatticeTenantTrees.SegmentPrefix, StringComparison.Ordinal))
        {
            return false;
        }

        var rest = id[LatticeTenantTrees.SegmentPrefix.Length..];
        var slash = rest.IndexOf('/');
        if (slash <= 0)
        {
            return false;
        }

        var tenantSlice = rest[..slash];
        var nameSlice = rest[(slash + 1)..];
        if (!TenantId.IsValid(tenantSlice)
            || tenantSlice.Equals(TenantId.DefaultId, StringComparison.Ordinal)
            || !IsValidName(nameSlice))
        {
            return false;
        }

        tenant = tenantSlice;
        name = nameSlice;
        return true;
    }
}
