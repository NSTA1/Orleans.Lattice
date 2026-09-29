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
/// <param name="tenant">The circuit's asserted tenant: everything memoised belongs to the tenant it was read under.</param>
internal sealed class AccessCatalog(ILatticeAuthAdmin admin, ShellAssertedTenant? tenant = null)
{
    private static readonly AuthPageRequest CompletionPage = new() { PageSize = AuthPageRequest.MaxPageSize };

    private readonly ShellAssertedTenant _tenant = tenant ?? ShellAssertedTenant.None;
    private string? _memoTenant;
    private AccessModelDescriptor? _model;
    private IReadOnlyList<AuthGroup>? _groups;
    private IReadOnlyList<LatticeAuthorizationRule>? _rules;

    /// <summary>The auth facade the catalogue reads through.</summary>
    public ILatticeAuthAdmin Admin { get; } = admin ?? throw new ArgumentNullException(nameof(admin));

    /// <summary>
    /// The cluster's access model, or <see langword="null"/> when it could not be
    /// read; an unread model is unknown, never "not enforced".
    /// </summary>
    /// <param name="cancellationToken">Cancels the read.</param>
    public async Task<AccessModelDescriptor?> GetAccessModelAsync(CancellationToken cancellationToken)
    {
        var tenant = ForgetIfTenantChanged();
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

        if (ShellAssertedTenant.Same(ForgetIfTenantChanged(), tenant))
        {
            _model = model;
        }

        return model;
    }

    /// <summary>The first page of groups, for completion.</summary>
    /// <param name="cancellationToken">Cancels the read.</param>
    public async Task<IReadOnlyList<AuthGroup>> GetGroupsAsync(CancellationToken cancellationToken)
    {
        var tenant = ForgetIfTenantChanged();
        if (_groups is { } remembered)
        {
            return remembered;
        }

        var page = await Admin.ListGroupsAsync(CompletionPage, cancellationToken).ConfigureAwait(true);
        IReadOnlyList<AuthGroup> groups = page?.Entries ?? [];
        if (ShellAssertedTenant.Same(ForgetIfTenantChanged(), tenant))
        {
            _groups = groups;
        }

        return groups;
    }

    /// <summary>The first page of rules, for completion.</summary>
    /// <param name="cancellationToken">Cancels the read.</param>
    public async Task<IReadOnlyList<LatticeAuthorizationRule>> GetRulesAsync(CancellationToken cancellationToken)
    {
        var tenant = ForgetIfTenantChanged();
        if (_rules is { } remembered)
        {
            return remembered;
        }

        var page = await Admin.ListRulesAsync(CompletionPage, cancellationToken).ConfigureAwait(true);
        IReadOnlyList<LatticeAuthorizationRule> rules = page?.Entries ?? [];
        if (ShellAssertedTenant.Same(ForgetIfTenantChanged(), tenant))
        {
            _rules = rules;
        }

        return rules;
    }

    /// <summary>Forgets the memoised groups and rules after a write.</summary>
    public void Invalidate()
    {
        _groups = null;
        _rules = null;
    }

    /// <summary>Forgets everything read under another tenant, and returns the tenant asserted now.</summary>
    private string? ForgetIfTenantChanged()
    {
        var tenant = _tenant.AssertedTenant;
        if (!ShellAssertedTenant.Same(_memoTenant, tenant))
        {
            _memoTenant = tenant;
            _model = null;
            _groups = null;
            _rules = null;
        }

        return tenant;
    }
}
