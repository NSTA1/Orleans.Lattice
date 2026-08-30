using Orleans.Lattice.Explorer.Plugins.Tenancy;

namespace Orleans.Lattice.Explorer.Plugins.Tenants.Workspace;

/// <summary>
/// The quota surface: the selected tenant's usage against its ceilings, and the
/// editor that authors those ceilings.
/// <para>
/// Both halves keep the two distinctions the control API is careful to preserve.
/// A ceiling of <see langword="null"/> is <b>unbounded</b> and never a ceiling of
/// zero; a usage of <see langword="null"/> is <b>not measured</b> and never a
/// measured zero. The rows are projected through <see cref="TenantQuotaRow"/>,
/// which resolves both into an explicit reading state, and the editor round-trips
/// an unbounded ceiling through a blank field so saving cannot silently cap a
/// dimension at nothing.
/// </para>
/// </summary>
public sealed partial class TenantsWorkspace
{
    /// <summary>The message shown when the quota surface has no tenant selected.</summary>
    public const string QuotasNeedTenantMessage = "Select a tenant to read and author its quotas.";

    private static readonly IReadOnlyList<TenantQuotaRow> NoQuotaRows = Array.Empty<TenantQuotaRow>();

    // Sized to the fixed dimension set and rebuilt in place, so re-rendering the
    // surface allocates neither the array nor the strings inside it.
    private readonly TenantQuotaRow[] _quotaRows =
        new TenantQuotaRow[ExplorerTenantQuotaUsage.Dimensions.Count];

    private bool _quotasLoaded;

    /// <summary>
    /// The selected tenant's usage reading, or <see langword="null"/> when none
    /// has been read.
    /// </summary>
    public ExplorerTenantQuotaUsage? QuotaUsage { get; private set; }

    /// <summary>
    /// The per-dimension display rows for <see cref="QuotaUsage"/>, or an empty
    /// list before a reading has loaded.
    /// </summary>
    public IReadOnlyList<TenantQuotaRow> QuotaRows { get; private set; } = NoQuotaRows;

    /// <summary>The editable ceilings for the selected tenant.</summary>
    public TenantQuotaDraft QuotaDraft { get; } = new();

    /// <summary>
    /// The caption for the scope the reading was taken and is enforced under, or
    /// an empty string before a reading has loaded. A per-cluster reading is
    /// genuinely not a global total, so the figures are never presented without
    /// it.
    /// </summary>
    public string QuotaScopeCaption => QuotaUsage is { } usage
        ? TenantQuotaFormat.ScopeCaption(usage.EnforcementScope)
        : string.Empty;

    /// <summary>
    /// Whether the reading carried consumption figures at all.
    /// <see langword="false"/> for a registered tenant whose warm view has not
    /// compiled yet, whose ceilings below are still authoritative.
    /// </summary>
    public bool QuotaHasUsage => QuotaUsage?.HasUsage ?? false;

    /// <summary>
    /// The explanation rendered when a reading carried no consumption figures at
    /// all, or an empty string when it did.
    /// </summary>
    public string QuotaNoUsageCaption =>
        QuotaUsage is not null && !QuotaUsage.HasUsage ? TenantQuotaFormat.NoUsageCaption : string.Empty;

    /// <summary>
    /// The burst allowance in effect, as a percentage above each steady-state
    /// ceiling that admission tolerates, or <see langword="null"/> before a
    /// reading has loaded.
    /// </summary>
    public int? QuotaBurstPercent => QuotaUsage?.BurstPercent;

    /// <summary>
    /// Whether the authoritative ceilings on the tenant's detail differ from the
    /// ceilings the reading was taken against. The control API deliberately
    /// takes every figure in a reading from one coherent snapshot rather than
    /// pairing a just-changed ceiling with a not-yet-resampled usage, so a fresh
    /// quota edit shows on the detail first. That lag is captioned rather than
    /// hidden, because the alternative is a reading that invents a breach
    /// admission is not enforcing.
    /// </summary>
    public bool QuotaReadingIsBehind =>
        QuotaUsage is { } usage && SelectedDetail is { } detail && detail.Quotas != usage.Limits;

    /// <summary>The caption explaining a reading that lags a just-authored ceiling.</summary>
    public const string QuotaReadingBehindCaption =
        "The ceilings authored for this tenant have changed since this reading was taken. Every "
        + "figure here comes from one coherent snapshot, so the reading catches up on the next "
        + "sampling cycle rather than pairing new ceilings with old usage.";

    /// <summary>
    /// Re-reads the selected tenant's usage and reloads the ceiling editor from
    /// it.
    /// </summary>
    public async Task RefreshQuotasAsync()
    {
        if (!Allowed || Busy)
        {
            return;
        }

        ClearResult();
        BeginBusy();
        try
        {
            await LoadQuotasAsync(force: true).ConfigureAwait(false);
        }
        finally
        {
            EndBusy();
        }
    }

    /// <summary>
    /// Replaces the selected tenant's ceilings with the editor's contents. A
    /// blank field is authored as unbounded; <c>0</c> is authored as a real
    /// ceiling permitting nothing.
    /// </summary>
    public async Task SaveQuotasAsync()
    {
        if (!Allowed || Busy)
        {
            return;
        }

        if (SelectedTenantId is not { } tenantId)
        {
            Report(TenantOperationStatus.InvalidRequest, QuotasNeedTenantMessage);
            RaiseChanged();
            return;
        }

        if (!QuotaDraft.TryBuild(out var limits, out var error))
        {
            Report(TenantOperationStatus.InvalidRequest, error ?? TenantQuotaDraft.InvalidLimitMessage);
            RaiseChanged();
            return;
        }

        ClearResult();
        BeginBusy();
        try
        {
            var saved = await _domain.Tenants.SetQuotasAsync(tenantId, limits).ConfigureAwait(false);
            if (!saved.IsSuccess)
            {
                Report(saved);
                return;
            }

            QuotaDraft.Load(saved.Value);
            Report(
                TenantOperationStatus.Succeeded,
                saved.Value.IsUnbounded
                    ? "Quotas for " + tenantId + " are now unbounded on every dimension."
                    : "Quotas for " + tenantId + " were updated.");

            // The authoritative descriptor moves immediately; the usage reading
            // is deliberately allowed to lag, and the surface captions that.
            if (SelectedDetail is { } detail)
            {
                SelectedDetail = detail with { Quotas = saved.Value };
            }

            await LoadQuotasAsync(force: true).ConfigureAwait(false);
        }
        finally
        {
            EndBusy();
        }
    }

    private async Task LoadQuotasAsync(bool force)
    {
        if (!force && _quotasLoaded)
        {
            return;
        }

        if (SelectedTenantId is not { } tenantId)
        {
            ResetQuotas();
            return;
        }

        // The authored ceilings come from the tenant detail, which is the
        // authoritative descriptor; the reading below carries its own coherent
        // copy for the figures it reports.
        if (SelectedDetail is { } detail)
        {
            QuotaDraft.Load(detail.Quotas);
        }

        var read = await _domain.Tenants.GetQuotaUsageAsync(tenantId).ConfigureAwait(false);
        _quotasLoaded = true;

        if (!read.IsSuccess || read.Value is null)
        {
            QuotaUsage = null;
            QuotaRows = NoQuotaRows;
            Report(read);
            return;
        }

        QuotaUsage = read.Value;
        _headlineUsage[tenantId] = read.Value;
        ProjectQuotaRows(read.Value);
    }

    private void ProjectQuotaRows(ExplorerTenantQuotaUsage usage)
    {
        var dimensions = ExplorerTenantQuotaUsage.Dimensions;
        for (var i = 0; i < dimensions.Count; i++)
        {
            var kind = dimensions[i];
            _quotaRows[i] = TenantQuotaRow.From(kind, usage[kind]);
        }

        QuotaRows = _quotaRows;
    }

    private void ResetQuotas()
    {
        _quotasLoaded = false;
        QuotaUsage = null;
        QuotaRows = NoQuotaRows;
        QuotaDraft.Load(SelectedDetail?.Quotas ?? ExplorerTenantQuotaLimits.Unbounded);
    }
}
