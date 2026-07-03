using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Auth;

/// <summary>
/// The optional durable audit trail: an <see cref="ILatticeAuthAuditSink"/> that
/// appends each decision event to the reserved, append-only
/// <c>sys-auth-audit</c> <c>ILattice</c> tree, keyed by a unique
/// timestamp-ordered id and (when configured) written with a time-to-live so old
/// rows are reaped automatically. Disabled by default: writes only when
/// <see cref="LatticeAuthOptions.EnableDurableAuditTrail"/> is set, so the trail
/// costs nothing until a host opts in.
/// </summary>
/// <remarks>
/// <para>
/// The audit tree is written under
/// <see cref="LatticeAccessGateContext.EnterSystemOrigin"/> so the write bypasses
/// the enforcement gate it feeds: this both avoids denying the infrastructure
/// write against a fail-closed policy and prevents the audit write from
/// recursively auditing itself.
/// </para>
/// <para>
/// Each event gets a unique key (a zero-padded UTC-ticks prefix for rough time
/// ordering, plus a GUID suffix for uniqueness), so under the audit tree's
/// observed-remove-set replication merge concurrently appended rows from
/// different sites all survive rather than overwriting one another.
/// </para>
/// </remarks>
internal sealed class DurableAuthAuditTrailSink(
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeAuthOptions> options) : ILatticeAuthAuditSink
{
    /// <summary>The field separator between the ticks prefix and the uniqueness suffix in an audit key.</summary>
    internal const char KeySeparator = '\u001f';

    /// <inheritdoc />
    public async ValueTask WriteAsync(LatticeAuthDecisionEvent decisionEvent, CancellationToken cancellationToken = default)
    {
        var current = options.CurrentValue;
        if (!current.EnableDurableAuditTrail)
        {
            // Opt-in: the durable trail is off, so this sink is a no-op.
            return;
        }

        var lattice = grainFactory.GetGrain<ILattice>(AuthConstants.AuditTree);
        var key = BuildKey(decisionEvent.TimestampUtc);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            if (current.AuditTrailTimeToLive is { } ttl)
            {
                await lattice.SetAsync(key, decisionEvent, ttl, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await lattice.SetAsync(key, decisionEvent, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Builds a unique, roughly time-ordered audit-row key: the event's UTC ticks
    /// zero-padded to 19 digits, a separator, then a compact GUID for uniqueness.
    /// </summary>
    /// <param name="timestampUtc">The decision timestamp.</param>
    /// <returns>The audit-row key.</returns>
    internal static string BuildKey(DateTimeOffset timestampUtc) =>
        $"{timestampUtc.UtcTicks:D19}{KeySeparator}{Guid.NewGuid():N}";
}
