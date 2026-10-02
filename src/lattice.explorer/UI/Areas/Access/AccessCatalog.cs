using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Access;

/// <summary>
/// The circuit's small, memoised view of the access catalogue: the access model
/// the banner and forms read, and the first page of groups and rules that the
/// address line completes against. Every write the area makes invalidates it,
/// so a completion never offers something the operator just removed.
/// </summary>
/// <remarks>
/// Scoped per circuit. Completion reads at most one page of each catalogue
/// (<see cref="AuthPageRequest.MaxPageSize"/> entries), so a cluster with a vast
/// policy store keeps a bounded, cheap completion rather than a slow, exhaustive one.
/// </remarks>
/// <param name="admin">The auth facade.</param>
/// <param name="tenant">The circuit's asserted tenant, read when no <paramref name="caller"/> is given.</param>
/// <param name="caller">
/// The circuit's caller: everything memoised belongs to the sign-in, endpoint and
/// tenant it was read for. When <see langword="null"/>, a caller over
/// <paramref name="tenant"/> alone is used.
/// </param>
internal sealed class AccessCatalog(ILatticeAuthAdmin admin, ShellAssertedTenant? tenant = null, ShellCaller? caller = null)
{
    private static readonly AuthPageRequest CompletionPage = new() { PageSize = AuthPageRequest.MaxPageSize };

    private readonly ShellCaller _caller = caller ?? new ShellCaller(tenant: tenant);
    private ShellCallerKey _memoCaller;
    private AccessModelDescriptor? _model;
    private IReadOnlyList<AuthGroup>? _groups;
    private IReadOnlyList<LatticeAuthorizationRule>? _rules;
    private (string Tenant, IReadOnlyList<LatticeAuthorizationRule> Rules)? _scopedRules;

    /// <summary>
    /// The most catalogue pages one tenant-rooted page reads when the cluster does
    /// not narrow the listing itself, so filtering here stays bounded.
    /// </summary>
    public const int MaximumFillReads = 50;

    /// <summary>The auth facade the catalogue reads through.</summary>
    public ILatticeAuthAdmin Admin { get; } = admin ?? throw new ArgumentNullException(nameof(admin));

    /// <summary>
    /// The cluster's access model, or <see langword="null"/> when it could not be
    /// read; an unread model is unknown, never "not enforced".
    /// </summary>
    /// <param name="cancellationToken">Cancels the read.</param>
    public async Task<AccessModelDescriptor?> GetAccessModelAsync(CancellationToken cancellationToken)
    {
        var key = ForgetIfTheCallerChanged();
        if (_model is not null)
        {
            return _model;
        }

        AccessModelDescriptor? model;
        try
        {
            model = await Admin.GetAccessModelAsync(cancellationToken).ConfigureAwait(true);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return null;
        }

        if (ForgetIfTheCallerChanged() == key)
        {
            _model = model;
        }

        return model;
    }

    /// <summary>The first page of groups, for completion.</summary>
    /// <param name="cancellationToken">Cancels the read.</param>
    public async Task<IReadOnlyList<AuthGroup>> GetGroupsAsync(CancellationToken cancellationToken)
    {
        var key = ForgetIfTheCallerChanged();
        if (_groups is { } remembered)
        {
            return remembered;
        }

        var page = await Admin.ListGroupsAsync(CompletionPage, cancellationToken).ConfigureAwait(true);
        IReadOnlyList<AuthGroup> groups = page?.Entries ?? [];
        if (ForgetIfTheCallerChanged() == key)
        {
            _groups = groups;
        }

        return groups;
    }

    /// <summary>The first page of rules, for completion.</summary>
    /// <param name="cancellationToken">Cancels the read.</param>
    public Task<IReadOnlyList<LatticeAuthorizationRule>> GetRulesAsync(CancellationToken cancellationToken) =>
        GetRulesAsync(null, cancellationToken);

    /// <summary>
    /// The first page of rules a listing for <paramref name="scope"/> shows, for
    /// completion: every rule for a cluster-wide listing, or only the rules that
    /// govern the scope tenant's own trees.
    /// </summary>
    /// <param name="scope">The tenant a tenant-rooted address names, or <see langword="null"/> for the cluster-wide listing.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    public async Task<IReadOnlyList<LatticeAuthorizationRule>> GetRulesAsync(string? scope, CancellationToken cancellationToken)
    {
        var key = ForgetIfTheCallerChanged();
        if ((scope is null ? _rules : ScopedRulesFor(scope)) is { } remembered)
        {
            return remembered;
        }

        var page = await ListRulesAsync(scope, CompletionPage, cancellationToken).ConfigureAwait(true);
        IReadOnlyList<LatticeAuthorizationRule> rules = page?.Entries ?? [];
        if (ForgetIfTheCallerChanged() == key)
        {
            if (scope is null)
            {
                _rules = rules;
            }
            else
            {
                _scopedRules = (scope, rules);
            }
        }

        return rules;
    }

    /// <summary>
    /// Reads one page of the rules a listing for <paramref name="scope"/> shows.
    /// A cluster-wide listing is the whole catalogue. A tenant-rooted listing is
    /// only the rules governing that tenant's own trees: the cluster narrows it
    /// (<see cref="AuthPageRequest.ActiveTenantOnly"/>), and every rule is checked
    /// again here, so a cluster that predates the narrowing still shows nothing of
    /// another tenant's - its pages are then read on until this one is full. Each
    /// tenant-tier rule kept carries its owning tenant in
    /// <see cref="AuthRulePage.TenantRuleTenants"/>, index-aligned with the entries.
    /// </summary>
    /// <param name="scope">The tenant a tenant-rooted address names, or <see langword="null"/> for the cluster-wide listing.</param>
    /// <param name="request">The page asked for.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The page; it may hold more than the page size when the cluster did not narrow it.</returns>
    public async Task<AuthRulePage> ListRulesAsync(string? scope, AuthPageRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (scope is null)
        {
            return await Admin.ListRulesAsync(request, cancellationToken).ConfigureAwait(true);
        }

        if (!TenantId.TryParse(scope, out var owner))
        {
            // An address tenant that is no tenant id owns nothing.
            return new AuthRulePage { Tenant = scope };
        }

        var size = request.EffectivePageSize;
        var next = request with { ActiveTenantOnly = true };
        var entries = new List<LatticeAuthorizationRule>();

        // The owning tenant of each kept tenant-tier rule, kept index-aligned with
        // the entries; reported only when one of them is tenant-tier, as the cluster does.
        var tenants = new List<string?>();
        var anyTenantTier = false;
        for (var reads = 0; ; reads++)
        {
            var page = await Admin.ListRulesAsync(next, cancellationToken).ConfigureAwait(true);
            var pageTenants = page.TenantRuleTenants;
            for (var i = 0; i < page.Entries.Count; i++)
            {
                var rule = page.Entries[i];
                if (IsOwnedBy(rule, owner))
                {
                    entries.Add(rule);
                    var tenant = i < pageTenants.Count ? pageTenants[i] : null;
                    tenants.Add(tenant);
                    anyTenantTier |= tenant is not null;
                }
            }

            if (page.NextPageToken is null || entries.Count >= size || reads >= MaximumFillReads)
            {
                return new AuthRulePage
                {
                    Entries = entries,
                    NextPageToken = page.NextPageToken,
                    Tenant = scope,
                    TenantRuleTenants = anyTenantTier ? tenants : [],
                };
            }

            next = next with { PageToken = page.NextPageToken };
        }
    }

    /// <summary>
    /// How many cluster-wide (<c>Tree:*</c>) rules there are: they belong to no
    /// tenant, but apply to every tenant's trees. At most one page is read, so a
    /// vast bucket reports <see cref="AuthPageRequest.MaxPageSize"/> and more.
    /// </summary>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The count, and whether there are more than it.</returns>
    public async Task<(int Count, bool More)> CountClusterWideRulesAsync(CancellationToken cancellationToken = default)
    {
        var page = await Admin.ListRulesForTreeAsync(LatticeScope.ClusterWideTreeId, CompletionPage, cancellationToken).ConfigureAwait(true);
        return page is null ? (0, false) : (page.Entries.Count, page.NextPageToken is not null);
    }

    /// <summary>
    /// Whether <paramref name="rule"/> is one of <paramref name="tenant"/>'s rules:
    /// its governed tree is one of that tenant's own trees, as the cluster's tree
    /// ownership grammar decides. A cluster-wide <c>Tree:*</c> rule, and a rule on
    /// a platform tree, belong to no tenant.
    /// </summary>
    /// <param name="rule">The rule.</param>
    /// <param name="tenant">The tenant.</param>
    public static bool IsOwnedBy(LatticeAuthorizationRule rule, TenantId tenant)
    {
        ArgumentNullException.ThrowIfNull(rule);
        return IsOwnedBy(rule.Scope.TreeId, tenant);
    }

    /// <summary>
    /// Whether a rule governing <paramref name="treeId"/> is one of
    /// <paramref name="tenant"/>'s rules; see <see cref="IsOwnedBy(LatticeAuthorizationRule, TenantId)"/>.
    /// </summary>
    /// <param name="treeId">The governed tree id.</param>
    /// <param name="tenant">The tenant.</param>
    public static bool IsOwnedBy(string? treeId, TenantId tenant)
    {
        if (string.IsNullOrEmpty(treeId) || string.Equals(treeId, LatticeScope.ClusterWideTreeId, StringComparison.Ordinal))
        {
            return false;
        }

        var owner = LatticeTenantTrees.GetOwner(treeId);
        return owner.IsTenantOwned && owner.Tenant.Equals(tenant);
    }

    /// <summary>
    /// Whether a rule governing <paramref name="treeId"/> belongs on a listing for
    /// <paramref name="scope"/>: every rule on the cluster-wide listing, and only
    /// the scope tenant's own on a tenant-rooted one.
    /// </summary>
    /// <param name="scope">The tenant a tenant-rooted address names, or <see langword="null"/> for the cluster-wide listing.</param>
    /// <param name="treeId">The governed tree id.</param>
    public static bool Lists(string? scope, string? treeId) =>
        scope is null || (TenantId.TryParse(scope, out var tenant) && IsOwnedBy(treeId, tenant));

    /// <summary>Forgets the memoised groups and rules after a write.</summary>
    public void Invalidate()
    {
        _groups = null;
        _rules = null;
        _scopedRules = null;
    }

    private IReadOnlyList<LatticeAuthorizationRule>? ScopedRulesFor(string scope) =>
        _scopedRules is { } scoped && string.Equals(scoped.Tenant, scope, StringComparison.Ordinal) ? scoped.Rules : null;

    /// <summary>Forgets everything read for another caller, and returns the caller now.</summary>
    private ShellCallerKey ForgetIfTheCallerChanged()
    {
        var key = _caller.Current;
        if (_memoCaller != key)
        {
            _memoCaller = key;
            _model = null;
            _groups = null;
            _rules = null;
            _scopedRules = null;
        }

        return key;
    }
}
