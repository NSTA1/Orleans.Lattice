namespace Orleans.Lattice.Explorer.Core.Tenancy;

/// <summary>
/// A client-side tenant identity used by the Explorer to scope its view of a
/// multi-tenant cluster's trees and data. It is the display-layer counterpart to
/// the cluster's server-side tenant id: the Explorer's Core project must not
/// reference Orleans.Lattice core, so it carries its own lightweight identity
/// rather than the cluster type. Purely in-process view vocabulary; never
/// persisted or sent on the wire, so it carries no Orleans serialization
/// attributes.
/// </summary>
public readonly record struct ExplorerTenantId
{
    /// <summary>
    /// Creates a tenant identity from a non-empty tenant id string. The string is
    /// taken as-is (the cluster guarantees its validity); the Explorer never
    /// re-validates a server-supplied id.
    /// </summary>
    /// <param name="value">The tenant id text. Must not be <see langword="null"/> or empty.</param>
    public ExplorerTenantId(string value)
    {
        ArgumentException.ThrowIfNullOrEmpty(value);
        Value = value;
    }

    /// <summary>The tenant id text. Never <see langword="null"/> for a constructed value.</summary>
    public string Value { get; }

    /// <summary>
    /// The default tenant that owns legacy, un-prefixed trees, mirrored from the
    /// cluster's <c>TenantId.DefaultId</c>. A tree with no <c>t/</c> ownership
    /// prefix belongs to this tenant.
    /// </summary>
    public static ExplorerTenantId Default { get; } = new(ExplorerTenantTrees.DefaultTenantId);

    /// <inheritdoc />
    public override string ToString() => Value;
}
