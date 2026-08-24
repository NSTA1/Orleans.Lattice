namespace Orleans.Lattice;

/// <summary>
/// The immutable identity of a tenant in an opt-in multi-tenant Lattice
/// cluster. A tenant id is a short DNS-label-like token that scopes a tenant's
/// trees into the reserved <c>t/{tenantId}/{name}</c> structural namespace
/// (see <see cref="LatticeTenantTrees"/>).
/// </summary>
/// <remarks>
/// <para>
/// The grammar is <c>^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$</c>: 1 to
/// <see cref="MaxLength"/> characters drawn from lower-case ASCII letters,
/// digits, and the hyphen, with no leading or trailing hyphen. This guarantees a
/// tenant id can never contain <c>/</c> (so it can never break the segmented
/// tree-id grammar), never begins with <c>_</c> (so it can never collide with
/// the <c>_lattice_</c> system namespace), and can never begin with <c>sys-</c>
/// in a way that shadows the <c>sys-</c> system-data namespace, because a tenant
/// id is only ever composed <em>after</em> the reserved <c>t/</c> segment.
/// </para>
/// <para>
/// The id <see cref="DefaultId"/> (<c>default</c>) is reserved for the
/// legacy-adoption tenant: the well-known <see cref="Default"/> tenant a cluster
/// with no tenancy add-on resolves to, so core behaves byte-for-byte as it did
/// before tenancy existed. Tenant ids are immutable once parsed; equality is
/// ordinal (valid ids are always lower-case, so ordinal comparison is exact).
/// </para>
/// <para>
/// The uninitialised value (<c>default(TenantId)</c>) carries a <c>null</c>
/// <see cref="Value"/> and represents "no tenant"; it is distinct from the
/// reserved <see cref="Default"/> tenant whose <see cref="Value"/> is
/// <c>default</c>. Construct a valid instance through <see cref="Parse"/> or
/// <see cref="TryParse"/>.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.TenantId)]
[Immutable]
public readonly record struct TenantId
{
    /// <summary>The maximum length, in characters, of a valid tenant id.</summary>
    public const int MaxLength = 63;

    /// <summary>
    /// The reserved tenant id (<c>default</c>) of the legacy-adoption tenant.
    /// A user-supplied tenant id equal to this value is still syntactically
    /// valid; it names the same tenant as the well-known <see cref="Default"/>.
    /// </summary>
    public const string DefaultId = "default";

    private TenantId(string value) => Value = value;

    /// <summary>
    /// The canonical tenant-id text. <c>null</c> only for the uninitialised
    /// <c>default(TenantId)</c> "no tenant" value; otherwise a token matching the
    /// grammar documented on <see cref="TenantId"/>.
    /// </summary>
    [Id(0)]
    public string Value { get; private init; }

    /// <summary>
    /// The well-known legacy-adoption tenant (<see cref="DefaultId"/>). Resolved
    /// by the core no-op tenant-context seam so a cluster without the tenancy
    /// add-on behaves exactly as it did before tenancy existed.
    /// </summary>
    public static TenantId Default { get; } = new(DefaultId);

    /// <summary>
    /// <c>true</c> when this is the reserved legacy-adoption tenant
    /// (<see cref="DefaultId"/>).
    /// </summary>
    public bool IsDefault => string.Equals(Value, DefaultId, StringComparison.Ordinal);

    /// <summary>
    /// Parses <paramref name="value"/> into a <see cref="TenantId"/>, throwing
    /// when it does not match the tenant-id grammar.
    /// </summary>
    /// <param name="value">The candidate tenant id. Must not be <c>null</c>.</param>
    /// <returns>The parsed tenant id.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="value"/> is <c>null</c>.</exception>
    /// <exception cref="FormatException"><paramref name="value"/> is not a valid tenant id.</exception>
    public static TenantId Parse(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        if (!TryParse(value, out var tenantId))
        {
            throw new FormatException(
                $"'{value}' is not a valid tenant id. A tenant id must be 1 to {MaxLength} " +
                "characters of lower-case ASCII letters, digits, and hyphens, with no leading " +
                "or trailing hyphen.");
        }

        return tenantId;
    }

    /// <summary>
    /// Attempts to parse <paramref name="value"/> into a <see cref="TenantId"/>
    /// without throwing.
    /// </summary>
    /// <param name="value">The candidate tenant id, or <c>null</c>.</param>
    /// <param name="tenantId">
    /// The parsed tenant id when this returns <c>true</c>; otherwise
    /// <c>default</c>.
    /// </param>
    /// <returns><c>true</c> when <paramref name="value"/> is a valid tenant id; otherwise <c>false</c>.</returns>
    public static bool TryParse(string? value, out TenantId tenantId)
    {
        if (value is not null && IsValid(value.AsSpan()))
        {
            tenantId = new TenantId(value);
            return true;
        }

        tenantId = default;
        return false;
    }

    /// <summary>Returns the canonical tenant-id text (empty for "no tenant").</summary>
    /// <returns>The tenant id, or the empty string for <c>default(TenantId)</c>.</returns>
    public override string ToString() => Value ?? string.Empty;

    /// <summary>
    /// Validates a candidate tenant id against the grammar without allocating.
    /// Shared with <see cref="LatticeTenantTrees"/> so the segmented-tree-id
    /// parser can validate the owning-tenant slice off a span.
    /// </summary>
    internal static bool IsValid(ReadOnlySpan<char> value)
    {
        var length = value.Length;
        if (length is < 1 or > MaxLength)
        {
            return false;
        }

        for (var i = 0; i < length; i++)
        {
            var c = value[i];
            var isLower = c is >= 'a' and <= 'z';
            var isDigit = c is >= '0' and <= '9';
            var isHyphen = c == '-';

            if (!isLower && !isDigit && !isHyphen)
            {
                return false;
            }

            if (isHyphen && (i == 0 || i == length - 1))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Constructs a <see cref="TenantId"/> from an already-validated value,
    /// skipping re-validation. Callers (in this assembly) must guarantee
    /// <paramref name="value"/> satisfies <see cref="IsValid"/>.
    /// </summary>
    internal static TenantId ForValidated(string value) => new(value);
}
