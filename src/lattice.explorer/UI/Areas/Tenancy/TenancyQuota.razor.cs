using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>
/// One tenant's use against its quota, dimension by dimension, and - when
/// <see cref="CanEdit"/> - the editor that replaces its quotas. An unbounded
/// dimension reads as having no ceiling and an unmeasured one as not measured;
/// neither is ever drawn as an empty or full bar.
/// </summary>
public partial class TenancyQuota : IDisposable
{
    private readonly ComponentLifetime _lifetime = new();
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
        _editing = false;
        _saving = false;
        _draft = null;
        await LoadAsync().ConfigureAwait(true);
    }

    /// <summary>Cancels the quota reads when this section is left.</summary>
    public void Dispose()
    {
        _lifetime.Leave();
        GC.SuppressFinalize(this);
    }

    private async Task LoadAsync()
    {
        var token = _lifetime.Renew();
        _failure = null;
        _report = null;
        try
        {
            var quota = Catalog.Quota ?? throw new NotSupportedException();
            var report = await quota.GetQuotaUsageAsync(TenantId, token).ConfigureAwait(true);
            if (token.IsCancellationRequested)
            {
                return;
            }

            _report = report;
            _gauges = TenancyQuotaGauge.All(_report);
        }
        catch (Exception) when (token.IsCancellationRequested)
        {
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

    private static string AccessCapsText(TenantQuotasDescriptor quotas)
    {
        var parts = new string[TenancyAccessCaps.All.Count];
        for (var i = 0; i < parts.Length; i++)
        {
            var cap = TenancyAccessCaps.All[i];
            parts[i] = $"{TenancyAccessCaps.Label(cap)} {TenancyAccessCaps.EffectiveText(quotas, cap)}";
        }

        return string.Join(", ", parts);
    }

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
        var tenant = TenantId;
        var token = _lifetime.Token;
        try
        {
            var admin = Catalog.Admin ?? throw new NotSupportedException();
            await admin.SetTenantQuotasAsync(tenant, quotas).ConfigureAwait(true);
            Catalog.Invalidate();
        }
        catch (Exception) when (token.IsCancellationRequested)
        {
            return;
        }
        catch (Exception exception) when (TenancyFailure.From(exception) is { } failure)
        {
            _formError = failure.Message;
            return;
        }
        finally
        {
            if (!token.IsCancellationRequested)
            {
                _saving = false;
            }
        }

        if (token.IsCancellationRequested)
        {
            return;
        }

        _editing = false;
        Toasts.Show(quotas.IsUnbounded ? $"Tenant {tenant} now has no quota ceilings." : $"Quotas of tenant {tenant} saved.", LtToastTone.Success);
        await LoadAsync().ConfigureAwait(true);
    }
}
