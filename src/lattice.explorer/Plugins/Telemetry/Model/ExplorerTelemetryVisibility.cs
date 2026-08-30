namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// The tenant visibility a telemetry panel may <em>request</em>, and the one the
/// facade reports it actually applied. It mirrors the facade's three-valued
/// contract exactly, because collapsing any two of them would lose the very
/// distinction a panel needs to label itself honestly.
/// </summary>
/// <remarks>
/// <para>
/// This is the Explorer's own vocabulary: the seam projects the facade's wire
/// enumeration onto it, so a panel never names a control-API type. It is wider
/// than the shell's own <c>ExplorerTenantVisibility</c>, which offers only the
/// active tenant and every tenant, because the telemetry facade additionally
/// lets a validated platform operator pin one <em>named</em> tenant instead of
/// fetching every tenant and discarding all but one.
/// </para>
/// <para>
/// <b>Requesting is not asserting.</b> Setting a wider value here asks the
/// facade for a wider view; it does not take one. The facade re-validates the
/// request server-side and reports what it pinned on
/// <see cref="ExplorerTelemetryScope"/>, so an unvalidated widening comes back
/// degraded rather than honoured.
/// </para>
/// </remarks>
public enum ExplorerTelemetryVisibility
{
    /// <summary>
    /// The fail-closed default: only the caller's own active tenant.
    /// </summary>
    ActiveTenant = 0,

    /// <summary>
    /// Every tenant's series at once. Honoured only for a caller the facade
    /// validates as a platform operator; otherwise it comes back degraded.
    /// </summary>
    AllTenants = 1,

    /// <summary>
    /// One named tenant, supplied on
    /// <see cref="ExplorerTelemetryRequest.RequestedTenantId"/>. Honoured only
    /// for a validated platform operator with a usable id; for anyone else the
    /// id is ignored in full and the query is served at
    /// <see cref="ActiveTenant"/> scope.
    /// </summary>
    SingleTenant = 2,
}
