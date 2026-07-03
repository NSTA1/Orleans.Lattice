using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Ambient caller-credential scope used to propagate a <see cref="LatticeCredential"/>
/// from the client edge down to the silo on the Orleans
/// <see cref="RequestContext"/>, following the same marker idiom as
/// <see cref="LatticeOriginContext"/> and <see cref="LatticeIdempotencyContext"/>.
/// The credential is the channel the Membership layer later resolves into a
/// subject; nothing in the core library reads it, so an unset credential adds
/// no cost and changes no read/write semantics.
/// </summary>
/// <remarks>
/// <para>
/// The credential flows on every outgoing grain call via a
/// <see cref="RequestContext"/> entry keyed
/// <see cref="LatticeEventConstants.CredentialRequestContextKey"/>. A caller
/// stamps it ergonomically at the boundary of a logical operation with
/// <c>using var _ = LatticeCredentialContext.Use(token);</c> (or
/// <see cref="With(LatticeCredential?)"/> for the full payload); the marker
/// clears when the scope disposes. When no scope is entered,
/// <see cref="Current"/> is <c>null</c> and <see cref="IsActive"/> is
/// <c>false</c> - a single dictionary lookup with no allocation.
/// </para>
/// <para>
/// <b>System-origin interaction.</b> This marker carries a <em>user</em>
/// credential only. Library-internal system / maintenance / replication-origin
/// calls (the paths flagged by <see cref="LatticeMaintenanceContext"/>,
/// <see cref="LatticeOriginContext"/>, and the replication apply seams) are
/// authored by infrastructure, not by a user, and must therefore carry no
/// credential. Those paths never open a credential scope, so they naturally
/// carry none. When infrastructure code fans a system-origin sub-operation out
/// from within a turn that <em>did</em> carry a user credential, it wraps that
/// sub-operation in <see cref="Suppress"/> so the ambient credential is
/// stripped for the duration and cannot leak onto a system-authored call.
/// </para>
/// </remarks>
public static class LatticeCredentialContext
{
    /// <summary>
    /// <c>true</c> when a credential scope is currently set on the ambient
    /// <see cref="RequestContext"/>. Cheaper than reading <see cref="Current"/>
    /// because the result is a <c>bool</c> rather than a boxed
    /// <see cref="LatticeCredential"/> nullable; the Membership layer uses this
    /// to short-circuit resolution on the (default) cold path so callers who
    /// never stamp a credential pay no extra cost.
    /// </summary>
    public static bool IsActive =>
        RequestContext.Get(LatticeEventConstants.CredentialRequestContextKey) is LatticeCredential;

    /// <summary>
    /// Gets or sets the caller credential on the ambient
    /// <see cref="RequestContext"/>. Setting <c>null</c> removes the key rather
    /// than storing a null value, matching the "no credential" default.
    /// </summary>
    public static LatticeCredential? Current
    {
        get
        {
            var raw = RequestContext.Get(LatticeEventConstants.CredentialRequestContextKey);
            return raw is LatticeCredential credential ? credential : null;
        }
        set
        {
            if (value is null)
            {
                RequestContext.Remove(LatticeEventConstants.CredentialRequestContextKey);
            }
            else
            {
                RequestContext.Set(LatticeEventConstants.CredentialRequestContextKey, value.Value);
            }
        }
    }

    /// <summary>
    /// Sets <see cref="Current"/> to <paramref name="credential"/> for the
    /// lifetime of the returned scope, restoring the prior value on
    /// <see cref="IDisposable.Dispose"/>. Safe to nest; disposal is idempotent.
    /// </summary>
    /// <param name="credential">
    /// The credential to stamp onto calls authored inside the scope, or
    /// <c>null</c> to explicitly clear the ambient credential.
    /// </param>
    public static IDisposable With(LatticeCredential? credential)
    {
        var previous = Current;
        Current = credential;
        return new Scope(previous);
    }

    /// <summary>
    /// Convenience edge helper: stamps a <see cref="LatticeCredential"/> built
    /// from the supplied opaque <paramref name="token"/> (and optional hints)
    /// for the lifetime of the returned scope, restoring the prior value on
    /// <see cref="IDisposable.Dispose"/>. Safe to nest; disposal is idempotent.
    /// </summary>
    /// <param name="token">
    /// The opaque credential / token string an authenticator later resolves
    /// into a subject. Never inspected by the core library.
    /// </param>
    /// <param name="scheme">
    /// Optional scheme / issuer hint, or <c>null</c> when unspecified.
    /// </param>
    /// <param name="principalId">
    /// Optional pre-resolved principal identifier, or <c>null</c> when the silo
    /// should resolve the principal from <paramref name="token"/>.
    /// </param>
    /// <param name="metadata">
    /// Optional small metadata bag, or <c>null</c> when unspecified.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="token"/> is <c>null</c>.
    /// </exception>
    public static IDisposable Use(
        string token,
        string? scheme = null,
        string? principalId = null,
        IReadOnlyDictionary<string, string>? metadata = null)
    {
        ArgumentNullException.ThrowIfNull(token);
        return With(new LatticeCredential(token, scheme, principalId, metadata));
    }

    /// <summary>
    /// Clears the ambient credential for the lifetime of the returned scope,
    /// restoring the prior value on <see cref="IDisposable.Dispose"/>. Used by
    /// infrastructure code that fans a system-origin sub-operation out from
    /// within a turn that carried a user credential, so the system-authored
    /// call cannot inherit the user's credential. Safe to nest; disposal is
    /// idempotent. Equivalent to <c>With(null)</c>, named for intent at the
    /// call site.
    /// </summary>
    public static IDisposable Suppress() => With(null);

    private sealed class Scope(LatticeCredential? previous) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            Current = previous;
        }
    }
}
