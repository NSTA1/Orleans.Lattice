using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.UI.Navigation;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Backups;

/// <summary>
/// The Backups area's probes, per circuit: whether the area may be seen at all,
/// what the caller may do to one scope, whether health monitoring applies, and
/// whether this connection serves the catalogue extensions (the inventory and
/// cold restore). Catalogue rebuild and scrub are not among them: they run as
/// cluster operations (#4125), which the gRPC binding serves.
/// </summary>
/// <remarks>
/// <para>
/// Every probe fails closed. <see cref="ILatticeBackupControl.ProbeCapabilitiesAsync"/>
/// reads nothing and never throws a denial; its flags are advisory, and the
/// server still authorizes each real operation, so a page hides a control the
/// probe denies and still reports "not permitted" if the server refuses one it
/// allowed.
/// </para>
/// <para>
/// Only a definite answer is remembered for the circuit. A fault, a timeout or a
/// cancellation is not, so the next navigation asks again. What is remembered
/// belongs to the caller it was read for (the sign-in, the endpoint and the
/// asserted tenant): a sign-in, a sign-out, a new connection or a tenant switch
/// forgets it, and an answer that lands after such a change is not remembered.
/// </para>
/// </remarks>
internal sealed class BackupsAccess
{
    /// <summary>
    /// The tree the area-level probe names. It is never read or written: the
    /// probe asks only whether the caller could list backups at all.
    /// </summary>
    internal const string ProbeTreeId = "__backup_capability_probe__";

    /// <summary>The reason shown when the caller holds no backup grant.</summary>
    internal const string NoGrantReason = "You do not hold a backup grant on this cluster. Ask an administrator for one.";

    private static readonly BackupScopeSelector ProbeScope = BackupScopeSelector.WholeTree(ProbeTreeId);

    private readonly ILatticeBackupControl _control;
    private readonly ShellCaller _caller;
    private readonly ShellAssertedTenant _tenant;
    private readonly object _gate = new();
    private ShellCallerKey _memoCaller;
    private AreaAvailability? _availability;
    private bool? _healthMonitoring;
    private bool? _extensionsServed;

    /// <summary>Creates the probes over the circuit's backup facade.</summary>
    /// <param name="control">The backup facade.</param>
    /// <param name="tenant">The circuit's asserted tenant, read when no <paramref name="caller"/> is given.</param>
    /// <param name="caller">The circuit's caller, which keys what is remembered; when <see langword="null"/>, a caller over <paramref name="tenant"/> alone.</param>
    public BackupsAccess(
        [FromKeyedServices(ShellFacades.Key)] ILatticeBackupControl control,
        ShellAssertedTenant? tenant = null,
        ShellCaller? caller = null)
    {
        ArgumentNullException.ThrowIfNull(control);
        _control = control;
        _tenant = tenant ?? ShellAssertedTenant.None;
        _caller = caller ?? new ShellCaller(tenant: tenant);
    }

    /// <summary>
    /// Whether this connection serves the catalogue extensions. Known only once
    /// <see cref="GetInventoryAsync"/> has run; <see langword="null"/> until then.
    /// </summary>
    public bool? ExtensionsServed
    {
        get
        {
            lock (_gate)
            {
                ForgetIfTheCallerChanged();
                return _extensionsServed;
            }
        }
    }

    /// <summary>
    /// Whether the area may be seen: visible when the caller could list backups,
    /// unavailable with a reason when it could not, and hidden when the
    /// connection does not serve backup control or cannot be reached.
    /// </summary>
    /// <param name="cancellationToken">Cancelled when the directory stops waiting.</param>
    public async Task<AreaAvailability> GetAvailabilityAsync(CancellationToken cancellationToken)
    {
        ShellCallerKey caller;
        lock (_gate)
        {
            caller = ForgetIfTheCallerChanged();
            if (_availability is { } known)
            {
                return known;
            }
        }

        AreaAvailability answer;
        try
        {
            var capabilities = await _control.ProbeCapabilitiesAsync(ProbeScope, cancellationToken).ConfigureAwait(false);
            answer = capabilities is { CanList: true }
                ? AreaAvailability.Visible
                : AreaAvailability.Unavailable(NoGrantReason);
        }
        catch (Exception exception) when (BackupsFaults.IsDenied(exception))
        {
            answer = AreaAvailability.Unavailable(NoGrantReason);
        }
        catch (NotSupportedException)
        {
            answer = AreaAvailability.Hidden;
        }
        catch (Exception exception) when (!BackupsFaults.IsCancellation(exception, cancellationToken))
        {
            // Not configured, unreachable or faulted: hidden now, and asked again next time.
            return AreaAvailability.Hidden;
        }

        lock (_gate)
        {
            if (StillFor(caller))
            {
                _availability = answer;
            }
        }

        return answer;
    }

    /// <summary>What the caller may do to <paramref name="scope"/>; every flag is false when the probe fails.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancels the probe.</param>
    public async Task<BackupScopeCapabilities> ProbeAsync(BackupScopeSelector scope, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(scope);
        try
        {
            return await _control.ProbeCapabilitiesAsync(scope, cancellationToken).ConfigureAwait(false)
                ?? new BackupScopeCapabilities { Scope = scope };
        }
        catch (Exception exception) when (!BackupsFaults.IsCancellation(exception, cancellationToken))
        {
            return new BackupScopeCapabilities { Scope = scope };
        }
    }

    /// <summary>
    /// Whether backup-health monitoring applies to this deployment (a durable,
    /// external sink). False when the probe fails.
    /// </summary>
    /// <param name="cancellationToken">Cancels the probe.</param>
    public async Task<bool> IsHealthMonitoringAvailableAsync(CancellationToken cancellationToken)
    {
        ShellCallerKey caller;
        lock (_gate)
        {
            caller = ForgetIfTheCallerChanged();
            if (_healthMonitoring is { } known)
            {
                return known;
            }
        }

        bool available;
        try
        {
            available = await _control.IsHealthMonitoringAvailableAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (!BackupsFaults.IsCancellation(exception, cancellationToken))
        {
            return false;
        }

        lock (_gate)
        {
            if (StillFor(caller))
            {
                _healthMonitoring = available;
            }
        }

        return available;
    }

    /// <summary>
    /// The inventory, or <see langword="null"/> when it cannot be read. The
    /// inventory and cold restore are served by the same in-process surface and
    /// the inventory is not served by the gRPC binding, so an inventory that is not
    /// served tells the area to withdraw both.
    /// </summary>
    /// <param name="cancellationToken">Cancels the read.</param>
    public async Task<BackupInventoryReport?> GetInventoryAsync(CancellationToken cancellationToken)
    {
        if (ExtensionsServed == false)
        {
            return null;
        }

        try
        {
            var report = await _control.GetInventoryAsync(cancellationToken).ConfigureAwait(false);
            MarkExtensions(served: true);
            return report;
        }
        catch (NotSupportedException)
        {
            MarkExtensions(served: false);
            return null;
        }
        catch (Exception exception) when (!BackupsFaults.IsCancellation(exception, cancellationToken))
        {
            if (BackupsFaults.IsDenied(exception))
            {
                MarkExtensions(served: true);
            }

            return null;
        }
    }

    /// <summary>
    /// Reads one page of the backups the area lists: only the backups of the
    /// listing tenant's own trees when tenancy is on. The cluster narrows the page
    /// itself (<see cref="BackupCatalogRequest.ActiveTenantOnly"/>) - which matters
    /// above all for the reserved default tenant, which is otherwise handed every
    /// tenant's backups - and every row is checked again here, so a cluster that
    /// predates the narrowing still shows nothing of another tenant's.
    /// </summary>
    /// <param name="request">The page asked for.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    public async Task<BackupCatalogPage> ListBackupsAsync(BackupCatalogRequest request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        var listing = _tenant.ListingTenant;
        var page = await _control.ListBackupsAsync(Narrow(request, listing), cancellationToken).ConfigureAwait(false);
        return page.Entries.All(manifest => Lists(listing, manifest))
            ? page
            : page with { Entries = [.. page.Entries.Where(manifest => Lists(listing, manifest))] };
    }

    /// <summary>The tenant the area's listings are for, or <see langword="null"/> with tenancy off.</summary>
    public string? ListingTenant => _tenant.ListingTenant;

    /// <summary>
    /// <paramref name="request"/> narrowed to the caller's active tenant when there
    /// is a listing tenant (tenancy on); unchanged with tenancy off.
    /// </summary>
    /// <param name="request">The request.</param>
    /// <param name="listingTenant">The listing tenant, or <see langword="null"/>.</param>
    public static BackupCatalogRequest Narrow(BackupCatalogRequest request, string? listingTenant)
    {
        ArgumentNullException.ThrowIfNull(request);
        return listingTenant is null || request.ActiveTenantOnly ? request : request with { ActiveTenantOnly = true };
    }

    /// <summary>
    /// Whether a listing for <paramref name="listingTenant"/> shows
    /// <paramref name="manifest"/>: every backup with tenancy off, otherwise only a
    /// backup of one of that tenant's own trees.
    /// </summary>
    /// <param name="listingTenant">The listing tenant, or <see langword="null"/>.</param>
    /// <param name="manifest">The backup.</param>
    public static bool Lists(string? listingTenant, BackupManifest manifest)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        return listingTenant is null
            || (manifest.Scope?.TreeId is { Length: > 0 } tree && ShellAssertedTenant.Lists(listingTenant, tree));
    }

    /// <summary>Whether the catalogue extensions are served, asking once when it is not yet known.</summary>
    /// <param name="cancellationToken">Cancels the probe.</param>
    public async Task<bool> AreExtensionsServedAsync(CancellationToken cancellationToken)
    {
        if (ExtensionsServed is { } known)
        {
            return known;
        }

        await GetInventoryAsync(cancellationToken).ConfigureAwait(false);
        return ExtensionsServed ?? true;
    }

    /// <summary>Records that an extension call was refused as not served.</summary>
    public void MarkExtensionsNotServed() => MarkExtensions(served: false);

    private void MarkExtensions(bool served)
    {
        lock (_gate)
        {
            ForgetIfTheCallerChanged();
            _extensionsServed = served;
        }
    }

    /// <summary>Forgets every answer read for another caller, and returns the caller now. Call under the gate.</summary>
    private ShellCallerKey ForgetIfTheCallerChanged()
    {
        var caller = _caller.Current;
        if (_memoCaller != caller)
        {
            _memoCaller = caller;
            _availability = null;
            _healthMonitoring = null;
            _extensionsServed = null;
        }

        return caller;
    }

    /// <summary>Whether <paramref name="caller"/>, the caller an answer was read for, is still the caller. Call under the gate.</summary>
    private bool StillFor(ShellCallerKey caller) => ForgetIfTheCallerChanged() == caller;
}
