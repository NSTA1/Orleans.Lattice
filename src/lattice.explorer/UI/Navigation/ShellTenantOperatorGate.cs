using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.UI.Navigation;

/// <summary>
/// The Explorer's platform-operator gate: a caller is a platform operator exactly
/// when the Access area proves visible for it, which is the successor of the old
/// Access plugin's gate.
/// </summary>
/// <remarks>
/// <para>
/// Core's tenant switcher and tenant view ask this gate before widening a
/// caller's scope. Without it, Core's fail-closed default denies every caller,
/// so no operator could switch tenant. The Access area's probe lists one
/// membership group, which the cluster serves only to a caller who may
/// administer access, and it memoises its verdict per identity.
/// </para>
/// <para>
/// The gate fails closed: no Access area, any answer other than Visible, a
/// fault, or a probe that outlasts
/// <see cref="ExplorerChromeOptions.AvailabilityTimeout"/> all read as "not an
/// operator". Only what the Explorer offers depends on it; the cluster authorizes
/// every cross-tenant call again.
/// </para>
/// <para>
/// The area is resolved lazily, on each question, because areas are scoped and
/// some of them read the tenant switcher that depends on this gate.
/// </para>
/// </remarks>
/// <param name="services">The circuit's service provider.</param>
/// <param name="options">The chrome's timing options.</param>
/// <param name="time">The clock the probe is time-boxed on.</param>
internal sealed class ShellTenantOperatorGate(IServiceProvider services, ExplorerChromeOptions options, TimeProvider time)
    : IExplorerTenantOperatorGate
{
    /// <summary>The key of the area whose visibility proves operator standing.</summary>
    internal const string AccessAreaKey = "access";

    /// <inheritdoc />
    public async ValueTask<bool> IsPlatformOperatorAsync(CancellationToken cancellationToken = default)
    {
        IExplorerArea? access = null;
        foreach (var area in services.GetServices<IExplorerArea>())
        {
            if (string.Equals(area.Key, AccessAreaKey, StringComparison.Ordinal))
            {
                access = area;
                break;
            }
        }

        if (access is null)
        {
            return false;
        }

        var (outcome, availability, _) = await TimeBoxed.RunAsync(
            access.GetAvailabilityAsync,
            options.AvailabilityTimeout,
            time,
            cancellationToken).ConfigureAwait(false);

        return outcome == TimeBoxed.Outcome.Completed && availability.Kind == AreaAvailabilityKind.Visible;
    }
}
