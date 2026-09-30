using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Suggestions;

namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>
/// One tenant's cross-tenant grants, bound to <c>ILatticeTenantGrantAdmin</c>:
/// the offers made to it (approve, or reject after a confirmation), the grants it
/// holds and has made (revoke, confirmed by typing the other tenant's id), and a
/// validated "Offer a grant" form - the visible control of the palette's offer
/// command.
/// </summary>
public partial class TenancyGrants
{
    private LtComboBox? _granteeBox;
    private static readonly IReadOnlyList<LtSelectOption> AccessOptions =
    [
        new(nameof(TenantGrantAccess.Read), "Read"),
        new(nameof(TenantGrantAccess.Write), "Write"),
        new(nameof(TenantGrantAccess.ReadWrite), "Read and write"),
    ];

    private TenantGrantReport? _report;
    private TenancyFailure? _failure;
    private string? _loadedFor;
    private bool _offerRequested;
    private bool _offering;
    private bool _busy;
    private string _grantee = string.Empty;
    private string _scope = string.Empty;
    private string _access = nameof(TenantGrantAccess.Read);
    private string? _granteeError;
    private string? _scopeError;
    private string? _formError;
    private TenantGrantDescriptor? _rejecting;
    private TenantGrantDescriptor? _revoking;

    /// <summary>The tenant whose grants to show.</summary>
    [Parameter, EditorRequired]
    public string TenantId { get; set; } = string.Empty;

    /// <summary>Whether to open the offer form once the grants are read, as the palette's offer command asks.</summary>
    [Parameter]
    public bool OpenOfferOnLoad { get; set; }

    [Inject]
    internal TenancyCatalog Catalog { get; set; } = default!;

    [Inject]
    internal ExplorerSuggestions Suggestions { get; set; } = default!;

    [Inject]
    internal LtToastService Toasts { get; set; } = default!;

    [Inject]
    internal IServiceProvider Services { get; set; } = default!;

    // Only a platform operator can list every tenant, so only an operator's picker refuses an unlisted one.
    private LtComboBoxMode GranteeMode => Catalog.LastStanding is { IsOperator: true } ? LtComboBoxMode.PickExisting : LtComboBoxMode.Suggest;

    [CascadingParameter(Name = LtBreakpointCascade.Name)]
    internal LtBreakpoint? Breakpoint { get; set; }

    private string HeadingId => "tenancy-grants-heading-" + TenantId;

    private bool IsDefault => string.Equals(TenantId, global::Orleans.Lattice.TenantId.DefaultId, StringComparison.Ordinal);

    private LtDialogPlacement DialogPlacement => Breakpoint == LtBreakpoint.Compact ? LtDialogPlacement.End : LtDialogPlacement.Center;

    private string RevokeName => _revoking is not { } grant ? string.Empty
        : string.Equals(grant.GranterTenantId, TenantId, StringComparison.Ordinal) ? grant.GranteeTenantId
        : grant.GranterTenantId;

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (string.Equals(_loadedFor, TenantId, StringComparison.Ordinal))
        {
            return;
        }

        _loadedFor = TenantId;
        _offerRequested = OpenOfferOnLoad;
        await LoadAsync().ConfigureAwait(true);
        if (_offerRequested && _report is not null && !IsDefault)
        {
            OpenOffer();
        }

        _offerRequested = false;
    }

    private async Task LoadAsync()
    {
        _failure = null;
        _report = null;
        try
        {
            var grants = Catalog.Grants ?? throw new NotSupportedException();
            var report = await grants.ListGrantsAsync(TenantId).ConfigureAwait(true);
            _report = report with
            {
                Received = [.. report.Received.OrderBy(StateOrder).ThenBy(grant => grant.GranterTenantId, StringComparer.Ordinal)],
                Issued = [.. report.Issued.OrderBy(StateOrder).ThenBy(grant => grant.GranteeTenantId, StringComparer.Ordinal)],
            };
        }
        catch (Exception exception) when (TenancyFailure.From(exception) is { } failure)
        {
            _failure = failure;
        }
    }

    // Offers awaiting a decision first, then live grants, then closed ones.
    private static int StateOrder(TenantGrantDescriptor grant) => grant.State switch
    {
        TenantGrantLifecycleState.Pending => 0,
        TenantGrantLifecycleState.Active => 1,
        _ => 2,
    };

    /// <summary>
    /// The scope to offer for a tree name or prefix typed in <paramref name="tenant"/>.
    /// The cluster's tenant gate matches a grant's scope against the full
    /// <c>t/{tenant}/...</c> tree id it reads, so a tenant-local name is qualified
    /// into the granting tenant's namespace; a bare name would share nothing.
    /// </summary>
    /// <param name="tenant">The granting tenant.</param>
    /// <param name="scope">The name or prefix as typed.</param>
    internal static string QualifyScope(string tenant, string scope) =>
        scope.StartsWith(ExplorerTenantTrees.SegmentPrefix, StringComparison.Ordinal)
            ? scope
            : $"{ExplorerTenantTrees.SegmentPrefix}{tenant}/{scope}";

    private void OpenOffer()
    {
        _grantee = string.Empty;
        _scope = string.Empty;
        _access = nameof(TenantGrantAccess.Read);
        _granteeError = null;
        _scopeError = null;
        _formError = null;
        _offering = true;
    }

    private async Task OfferAsync()
    {
        if (_busy)
        {
            return;
        }

        var grantee = _grantee.Trim();
        var scope = _scope.Trim();
        _granteeError = grantee.Length == 0 ? "Enter the id of the tenant to share with."
            : !global::Orleans.Lattice.TenantId.TryParse(grantee, out var parsed) ? "A tenant id is lower-case letters, digits and hyphens."
            : parsed.IsDefault ? "The reserved default tenant takes no part in grants."
            : string.Equals(grantee, TenantId, StringComparison.Ordinal) ? "A tenant cannot grant to itself."
            : null;
        _scopeError = scope.Length == 0 ? "Enter the tree name or prefix to share." : null;
        _formError = null;
        if (_granteeError is not null || _scopeError is not null)
        {
            return;
        }

        if (_granteeBox is not null && !await _granteeBox.ConfirmAsync().ConfigureAwait(true))
        {
            return;
        }

        var access = Enum.Parse<TenantGrantAccess>(_access);
        _busy = true;
        try
        {
            await Catalog.Grants!.OfferGrantAsync(TenantId, grantee, QualifyScope(TenantId, scope), access).ConfigureAwait(true);
        }
        catch (Exception exception) when (TenancyFailure.From(exception) is { } failure)
        {
            _formError = failure.Message;
            return;
        }
        finally
        {
            _busy = false;
        }

        _offering = false;
        Toasts.Show($"Offered {scope} to tenant {grantee}. It takes effect once tenant {grantee} approves.", LtToastTone.Success);
        await LoadAsync().ConfigureAwait(true);
    }

    private Task ApproveAsync(TenantGrantDescriptor grant) =>
        TransitionAsync(
            grant,
            (grants, g) => grants.ApproveGrantAsync(g.GranterTenantId, g.GranteeTenantId, g.Scope),
            $"Approved. Tenant {TenantId} can now {TenancyFormat.AccessLabel(grant.Operations).ToLowerInvariant()} {grant.Scope} in tenant {grant.GranterTenantId}.");

    private async Task RejectAsync()
    {
        if (_rejecting is not { } grant)
        {
            return;
        }

        _rejecting = null;
        await TransitionAsync(
            grant,
            (grants, g) => grants.RejectGrantAsync(g.GranterTenantId, g.GranteeTenantId, g.Scope),
            $"Rejected the offer of {grant.Scope} from tenant {grant.GranterTenantId}.").ConfigureAwait(true);
    }

    private async Task RevokeAsync()
    {
        if (_revoking is not { } grant)
        {
            return;
        }

        _revoking = null;
        await TransitionAsync(
            grant,
            (grants, g) => grants.RevokeGrantAsync(g.GranterTenantId, g.GranteeTenantId, g.Scope),
            $"Revoked. Tenant {grant.GranteeTenantId} no longer has access to {grant.Scope}.").ConfigureAwait(true);
    }

    private async Task TransitionAsync(
        TenantGrantDescriptor grant,
        Func<ILatticeTenantGrantAdmin, TenantGrantDescriptor, Task<TenantGrantChangeResult>> change,
        string done)
    {
        if (_busy)
        {
            return;
        }

        _busy = true;
        try
        {
            await change(Catalog.Grants!, grant).ConfigureAwait(true);
            Toasts.Show(done, LtToastTone.Success);

            // The trees shared with this tenant are part of the Data directory's
            // memo; a grant this circuit just approved, rejected or revoked changes
            // them, so the next Data read lists them afresh.
            (Services.GetService(typeof(DataDirectory)) as DataDirectory)?.Invalidate();
        }
        catch (Exception exception) when (TenancyFailure.From(exception) is { } failure)
        {
            Toasts.Show(failure.Message, LtToastTone.Danger);
        }
        finally
        {
            _busy = false;
        }

        await LoadAsync().ConfigureAwait(true);
    }
}
