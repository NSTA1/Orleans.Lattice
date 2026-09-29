using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;

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
internal sealed class AccessCatalog(ILatticeAuthAdmin admin)
{
    private static readonly AuthPageRequest CompletionPage = new() { PageSize = AuthPageRequest.MaxPageSize };

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
        if (_model is not null)
        {
            return _model;
        }

        try
        {
            _model = await Admin.GetAccessModelAsync(cancellationToken).ConfigureAwait(true);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return null;
        }

        return _model;
    }

    /// <summary>The first page of groups, for completion.</summary>
    /// <param name="cancellationToken">Cancels the read.</param>
    public async Task<IReadOnlyList<AuthGroup>> GetGroupsAsync(CancellationToken cancellationToken)
    {
        if (_groups is null)
        {
            var page = await Admin.ListGroupsAsync(CompletionPage, cancellationToken).ConfigureAwait(true);
            _groups = page?.Entries ?? [];
        }

        return _groups;
    }

    /// <summary>The first page of rules, for completion.</summary>
    /// <param name="cancellationToken">Cancels the read.</param>
    public async Task<IReadOnlyList<LatticeAuthorizationRule>> GetRulesAsync(CancellationToken cancellationToken)
    {
        if (_rules is null)
        {
            var page = await Admin.ListRulesAsync(CompletionPage, cancellationToken).ConfigureAwait(true);
            _rules = page?.Entries ?? [];
        }

        return _rules;
    }

    /// <summary>Forgets the memoised groups and rules after a write.</summary>
    public void Invalidate()
    {
        _groups = null;
        _rules = null;
    }
}
