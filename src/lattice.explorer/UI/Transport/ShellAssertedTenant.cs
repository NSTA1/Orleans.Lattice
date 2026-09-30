using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The tenant this circuit's Shell calls assert, and the key everything the
/// circuit remembers from the cluster is filed under: an answer read under one
/// tenant is never served under another.
/// </summary>
/// <remarks>
/// <para>
/// It reads Core's <see cref="ILatticeActiveTenantProvider"/> live, and asserts
/// nothing when the head registers no tenancy. It is itself the provider the
/// circuit's transport channel asks on every call.
/// </para>
/// <para>
/// <b>Pinning.</b> Work that outlives the page that started it - a staged backup
/// operation - must finish in the tenant it began in, even if the circuit moves
/// to another tenant meanwhile. <see cref="Pin"/> holds the asserted tenant for
/// the calling asynchronous flow only: the pin is carried by the execution
/// context, so it reaches exactly the calls that flow makes, and it is honoured
/// only by the instance that set it, so it can never cross into another circuit.
/// </para>
/// </remarks>
/// <param name="inner">Core's live tenant source, or <see langword="null"/> when tenancy is off.</param>
internal sealed class ShellAssertedTenant(ILatticeActiveTenantProvider? inner = null) : ILatticeActiveTenantProvider
{
    private static readonly AsyncLocal<PinnedTenant?> Pinned = new();

    /// <summary>The instance for a head without tenancy: it asserts nothing.</summary>
    public static ShellAssertedTenant None { get; } = new();

    /// <inheritdoc />
    public string? AssertedTenant =>
        Pinned.Value is { } pin && ReferenceEquals(pin.Owner, this) ? pin.Tenant : inner?.AssertedTenant;

    /// <summary>Whether <paramref name="left"/> and <paramref name="right"/> name the same tenant (or both none).</summary>
    /// <param name="left">A tenant, or <see langword="null"/>.</param>
    /// <param name="right">A tenant, or <see langword="null"/>.</param>
    public static bool Same(string? left, string? right) => string.Equals(left, right, StringComparison.Ordinal);

    /// <summary>
    /// Whether a tenant-scoped listing for <paramref name="listingTenant"/> shows
    /// <paramref name="treeId"/>: every tree when there is no listing tenant
    /// (tenancy off), otherwise only a tree that tenant owns. The cluster lists
    /// every tenant's trees, and its system trees, to the reserved default
    /// tenant, so a listing scoped to the default tenant must narrow itself.
    /// </summary>
    /// <param name="listingTenant">The tenant the listing is for, from <see cref="ListingTenantOf"/>, or <see langword="null"/>.</param>
    /// <param name="treeId">The logical tree id.</param>
    /// <returns><see langword="true"/> when the listing shows the tree.</returns>
    public static bool Lists(string? listingTenant, string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return string.IsNullOrEmpty(listingTenant)
            || ExplorerTenantTrees.IsOwnedBy(treeId, new ExplorerTenantId(listingTenant));
    }

    /// <summary>
    /// The tenant a tenant-scoped listing read through <paramref name="provider"/>
    /// is for: <see langword="null"/> with tenancy off (no provider is registered),
    /// otherwise the asserted tenant, or the reserved default tenant when none is
    /// asserted - a call that asserts nothing is a default-tenant call.
    /// </summary>
    /// <param name="provider">The circuit's tenant provider, or <see langword="null"/> when tenancy is off.</param>
    /// <returns>The listing tenant, or <see langword="null"/>.</returns>
    public static string? ListingTenantOf(ILatticeActiveTenantProvider? provider) =>
        provider is null ? null : provider.AssertedTenant ?? ExplorerTenantTrees.DefaultTenantId;

    /// <summary>The tenant a tenant-scoped listing read now is for; see <see cref="ListingTenantOf"/>.</summary>
    public string? ListingTenant => inner is null ? null : AssertedTenant ?? ExplorerTenantTrees.DefaultTenantId;

    /// <summary>
    /// Holds <paramref name="tenant"/> as the asserted tenant for the calling
    /// asynchronous flow until the returned scope is disposed.
    /// </summary>
    /// <param name="tenant">The tenant to hold, or <see langword="null"/> to hold none.</param>
    /// <returns>A scope that restores the previous pin.</returns>
    public IDisposable Pin(string? tenant)
    {
        var pin = new PinnedTenant(this, tenant, Pinned.Value);
        Pinned.Value = pin;
        return pin;
    }

    /// <summary>One pin, and the one it replaced.</summary>
    private sealed class PinnedTenant(ShellAssertedTenant owner, string? tenant, PinnedTenant? previous) : IDisposable
    {
        public ShellAssertedTenant Owner { get; } = owner;

        public string? Tenant { get; } = tenant;

        public void Dispose()
        {
            if (ReferenceEquals(Pinned.Value, this))
            {
                Pinned.Value = previous;
            }
        }
    }
}
