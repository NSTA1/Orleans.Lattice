namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// The label-matcher fragment the facade injects into a server-authored query
/// template: the derived <c>tenant</c> matcher that pins an evaluation to one
/// tenant, plus the caller's optional <c>tree</c> narrowing filter. It is the
/// only part of a rendered query that varies per request, and every value it
/// carries has already been validated and escaped by the facade.
/// </summary>
/// <remarks>
/// <para>
/// <b>Scoping is on the derived <c>tenant</c> label, never on a <c>tree</c>
/// regex.</b> Tenancy encodes ownership in the tree id, but the default tenant's
/// adopted ids are bare, so its matcher would have to be
/// <c>tree!~"^t/.*"</c> - which also matches the <c>_lattice_</c> and <c>sys-</c>
/// platform namespaces and would leak platform-internal series into a tenant's
/// view. The repository-wide derived <c>tenant</c> dimension exists precisely to
/// make the three-way classification (tenant-owned, default-adopted,
/// platform-owned) a single exact-match label, so a tenant scope is one
/// <c>tenant="..."</c> matcher and the platform sentinel can never satisfy it.
/// </para>
/// <para>
/// <b>The tree filter narrows, never widens.</b> It is rendered <em>alongside</em>
/// the tenant matcher, so naming another tenant's tree yields the intersection -
/// no series - rather than that tenant's data.
/// </para>
/// <para>
/// Each matcher is rendered with a trailing comma (<c>tenant="acme",</c>), so a
/// template writes <c>{$scope$}</c> for a scope-only selector and
/// <c>{$scope$outcome="committed"}</c> to combine one with a static matcher; both
/// stay well-formed when the selector is empty, because PromQL admits both an
/// empty matcher list and a trailing comma.
/// </para>
/// <para>
/// A <see langword="readonly"/> struct that holds only references the facade
/// already has, so attaching a scope to a render costs no allocation.
/// </para>
/// </remarks>
internal readonly struct TelemetryScopeSelector
{
    private const string TenantLabel = LatticeTenantLabel.TagTenant;
    private const string TreeLabel = "tree";

    /// <summary>The characters a matcher adds around its name and value: <c>="</c> and <c>",</c>.</summary>
    private const int MatcherOverhead = 4;

    private readonly string? _tenantId;
    private readonly string? _treeId;

    private TelemetryScopeSelector(string? tenantId, string? treeId)
    {
        _tenantId = tenantId;
        _treeId = treeId;
    }

    /// <summary>
    /// The empty selector, which renders to nothing. Produced only for a
    /// validated cross-tenant evaluation, where no tenant is pinned.
    /// </summary>
    public static TelemetryScopeSelector Unscoped => default;

    /// <summary>
    /// Creates a selector pinning the evaluation to <paramref name="tenantId"/>
    /// and, when supplied, narrowing it to <paramref name="escapedTreeId"/>.
    /// </summary>
    /// <param name="tenantId">
    /// The server-derived effective tenant, already validated against the tenant-id
    /// grammar (or the reserved platform sentinel), so it needs no escaping.
    /// </param>
    /// <param name="escapedTreeId">
    /// The caller's tree filter, already escaped by
    /// <see cref="PromQlLabelValue.Escape(string)"/>, or <see langword="null"/> for
    /// no tree narrowing.
    /// </param>
    /// <returns>The scope selector.</returns>
    public static TelemetryScopeSelector ForTenant(string tenantId, string? escapedTreeId) =>
        new(tenantId, escapedTreeId);

    /// <summary>
    /// Creates a selector that pins no tenant but still narrows to
    /// <paramref name="escapedTreeId"/>. Used for a validated cross-tenant
    /// evaluation that the caller narrowed to one tree.
    /// </summary>
    /// <param name="escapedTreeId">The escaped tree filter, or <see langword="null"/>.</param>
    /// <returns>The scope selector.</returns>
    public static TelemetryScopeSelector ForTree(string? escapedTreeId) =>
        new(tenantId: null, escapedTreeId);

    /// <summary>The exact number of characters <see cref="WriteTo"/> emits.</summary>
    public int Length
    {
        get
        {
            var length = 0;
            if (_tenantId is not null)
            {
                length += TenantLabel.Length + _tenantId.Length + MatcherOverhead;
            }

            if (_treeId is not null)
            {
                length += TreeLabel.Length + _treeId.Length + MatcherOverhead;
            }

            return length;
        }
    }

    /// <summary>
    /// Writes the matcher fragment into <paramref name="destination"/>, which must
    /// be at least <see cref="Length"/> characters long.
    /// </summary>
    /// <param name="destination">The buffer to write into.</param>
    /// <returns>The number of characters written.</returns>
    public int WriteTo(Span<char> destination)
    {
        var written = 0;
        if (_tenantId is not null)
        {
            written += WriteMatcher(destination, TenantLabel, _tenantId);
        }

        if (_treeId is not null)
        {
            written += WriteMatcher(destination[written..], TreeLabel, _treeId);
        }

        return written;
    }

    private static int WriteMatcher(Span<char> destination, string name, string value)
    {
        var written = 0;
        name.CopyTo(destination);
        written += name.Length;

        destination[written++] = '=';
        destination[written++] = '"';

        value.CopyTo(destination[written..]);
        written += value.Length;

        destination[written++] = '"';
        destination[written++] = ',';
        return written;
    }
}
