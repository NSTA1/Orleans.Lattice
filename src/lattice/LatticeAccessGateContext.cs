using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Internal ambient marker that positively identifies the current turn as a
/// <em>system-origin</em> (infrastructure-authored) call - maintenance,
/// replication-apply, saga leg, or any other library-internal path that runs
/// outside a user identity - so the access-gate enforcement point can skip
/// authorization for it and never self-block.
/// </summary>
/// <remarks>
/// <para>
/// This mirrors the existing ambient-context idiom in the codebase (see
/// <see cref="Views.ViewWriteContext"/> and <see cref="LatticeMaintenanceContext"/>):
/// a <see cref="RequestContext"/>-key-backed <see cref="IsSystemOrigin"/>
/// predicate plus an <c>EnterSystemOrigin</c> <c>using</c> scope. The flag is
/// stamped on the
/// <see cref="LatticeEventConstants.AccessGateSystemOriginRequestContextKey"/>
/// entry and propagates across grain calls because <c>RequestContext</c> flows
/// automatically on outgoing calls, so a single scope at an infrastructure
/// path's entry marks every nested call it fans out.
/// </para>
/// <para>
/// A dedicated marker is used rather than an existing one because none of the
/// existing signals means exactly "system-origin, skip the gate":
/// <see cref="LatticeMaintenanceContext"/> is scoped to structural
/// maintenance <em>mutations</em> and additionally stamps
/// <see cref="MutationCategory.Maintenance"/> onto emitted mutations; an absent
/// <see cref="LatticeCredentialContext"/> credential is ambiguous between an
/// anonymous user and a system call. This marker is a single, unambiguous,
/// positive signal.
/// </para>
/// <para>
/// This issue only defines the seam. Wiring the marker onto the
/// maintenance / replication-apply / saga paths, and consulting
/// <see cref="IsSystemOrigin"/> at the enforcement point, is a later step; the
/// data-plane grain methods do not read it yet.
/// </para>
/// </remarks>
internal static class LatticeAccessGateContext
{
    /// <summary>
    /// Gets a value indicating whether a system-origin scope is active on the
    /// ambient <see cref="RequestContext"/>. The default outside any scope is
    /// <c>false</c> (a user-origin call).
    /// </summary>
    public static bool IsSystemOrigin =>
        RequestContext.Get(LatticeEventConstants.AccessGateSystemOriginRequestContextKey) is bool active && active;

    /// <summary>
    /// Marks the ambient context as a system-origin scope for the lifetime of
    /// the returned scope, restoring the prior value on
    /// <see cref="IDisposable.Dispose"/>. Safe to nest; disposal is idempotent.
    /// </summary>
    public static IDisposable EnterSystemOrigin()
    {
        var previous = RequestContext.Get(LatticeEventConstants.AccessGateSystemOriginRequestContextKey) as bool?;
        RequestContext.Set(LatticeEventConstants.AccessGateSystemOriginRequestContextKey, true);
        return new Scope(previous);
    }

    private sealed class Scope(bool? previous) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            if (previous is null)
            {
                RequestContext.Remove(LatticeEventConstants.AccessGateSystemOriginRequestContextKey);
            }
            else
            {
                RequestContext.Set(LatticeEventConstants.AccessGateSystemOriginRequestContextKey, previous.Value);
            }
        }
    }
}
