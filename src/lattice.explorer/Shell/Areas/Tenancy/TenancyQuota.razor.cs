using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Shell.Design.Components;
using Orleans.Lattice.Explorer.Shell.Design.Tokens;

namespace Orleans.Lattice.Explorer.Shell.Areas.Tenancy;

/// <summary>
/// One tenant's use against its quota, dimension by dimension, and - when
/// <see cref="CanEdit"/> - the editor that replaces its quotas. An unbounded
/// dimension reads as having no ceiling and an unmeasured one as not measured;
/// neither is ever drawn as an empty or full bar.
/// </summary>
public partial class TenancyQuota
{
    private TenantQuotaUsageReport? _report;
    private IReadOnlyList<TenancyQuotaGauge> _gauges = [];
    private TenancyFailure? _failure;
    private string? _loadedFor;
    private bool _editing;
    private bool _saving;
    private TenancyQuotaDraft? _draft;
    private IReadOnlyDictionary<TenancyQuotaDimension, string> _errors = new Dictionary<TenancyQuotaDimension, string>();
    private string? _formError;

    /// <summary>The tenant whose quota to show.</summary>
    [Parameter, EditorRequired]
    public string TenantId { get; set; } = string.Empty;

    /// <summary>Whether the caller may edit the quotas: a platform operator. The cluster still authorizes the change.</summary>
    [Parameter]
    public bool CanEdit { get; set; }

    [Inject]
    internal TenancyCatalog Catalog { get; set; } = default!;

    [Inject]
    internal LtToastService Toasts { get; set; } = default!;

    [CascadingParameter(Name = LtBreakpointCascade.Name)]
    internal LtBreakpoint? Breakpoint { get; set; }

    private string HeadingId => "tenancy-quota-heading-" + TenantId;

    private LtDialogPlacement DialogPlacement => Breakpoint == LtBreakpoint.Compact ? LtDialogPlacement.End : LtDialogPlacement.Center;

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (string.Equals(_loadedFor, TenantId, StringComparison.Ordinal))
        {
            return;
        }

        _loadedFor = TenantId;
        await LoadAsync().ConfigureAwait(true);
    }

    private async Task LoadAsync()
    {
        _failure = null;
        _report = null;
        try
        {
            var quota = Catalog.Quota ?? throw new NotSupportedException();
            _report = await quota.GetQuotaUsageAsync(TenantId).ConfigureAwait(true);
            _gauges = TenancyQuotaGauge.All(_report);
        }
        catch (Exception exception) when (TenancyFailure.From(exception) is { } failure)
        {
            _failure = failure;
        }
    }

    private void OpenEditor()
    {
        if (_report is not { } report)
        {
            return;
        }

        _draft = TenancyQuotaDraft.From(report.Quotas);
        _errors = new Dictionary<TenancyQuotaDimension, string>();
        _formError = null;
        _editing = true;
    }

    private string? ErrorFor(TenancyQuotaDimension dimension) => _errors.TryGetValue(dimension, out var error) ? error : null;

    private static string? HintFor(TenancyQuotaDimension dimension) =>
        dimension is TenancyQuotaDimension.Bytes or TenancyQuotaDimension.MemoryBytes ? "Bytes, or a number with KiB, MiB, GiB or TiB." : null;

    private async Task SaveAsync()
    {
        if (_saving || _draft is not { } draft)
        {
            return;
        }

        _formError = null;
        if (!draft.TryBuild(out var quotas, out _errors))
        {
            return;
        }

        _saving = true;
        try
        {
            var admin = Catalog.Admin ?? throw new NotSupportedException();
            await admin.SetTenantQuotasAsync(TenantId, quotas).ConfigureAwait(true);
        }
        catch (Exception exception) when (TenancyFailure.From(exception) is { } failure)
        {
            _formError = failure.Message;
            return;
        }
        finally
        {
            _saving = false;
        }

        _editing = false;
        Catalog.Invalidate();
        Toasts.Show(quotas.IsUnbounded ? $"Tenant {TenantId} now has no quota ceilings." : $"Quotas of tenant {TenantId} saved.", LtToastTone.Success);
        await LoadAsync().ConfigureAwait(true);
    }
}
