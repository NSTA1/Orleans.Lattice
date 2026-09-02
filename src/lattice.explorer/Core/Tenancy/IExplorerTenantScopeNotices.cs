namespace Orleans.Lattice.Explorer.Core.Tenancy;

/// <summary>
/// The per-circuit place a tenant scope outcome is left so the shell can announce
/// it. Written by whatever produced the outcome - the identity resolver when it
/// abandons a remembered tenant, the tenant scope control when a switch is
/// applied or refused - and read by the control that owns the live region.
/// </summary>
/// <remarks>
/// A one-slot handover rather than an event stream: only the latest outcome is
/// worth announcing, and a control that re-reads after every action needs no
/// subscription. Keeping it out of
/// <see cref="IExplorerTenantIdentityResolver"/> means a head that supplies its
/// own resolver is unaffected, and a resolver that has nothing to say simply
/// never publishes.
/// </remarks>
public interface IExplorerTenantScopeNotices
{
    /// <summary>
    /// The outcome still to be announced, or <see langword="null"/> when there is
    /// nothing to say.
    /// </summary>
    ExplorerTenantScopeNotice? Current { get; }

    /// <summary>
    /// Records <paramref name="notice"/> as the outcome to announce, replacing
    /// any earlier one.
    /// </summary>
    /// <param name="notice">The outcome. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="notice"/> is <see langword="null"/>.</exception>
    void Publish(ExplorerTenantScopeNotice notice);

    /// <summary>Drops the current outcome, so it is not announced again.</summary>
    void Clear();
}
