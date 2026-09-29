using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Connection;

/// <summary>A settable <see cref="ILatticeActiveTenantProvider"/> that counts how often it is asked.</summary>
internal sealed class FakeActiveTenantProvider : ILatticeActiveTenantProvider
{
    private string? _tenant;

    /// <summary>Creates the provider asserting <paramref name="tenant"/>.</summary>
    /// <param name="tenant">The tenant to assert, or <see langword="null"/> for none.</param>
    public FakeActiveTenantProvider(string? tenant = null) => _tenant = tenant;

    /// <summary>How many times <see cref="AssertedTenant"/> was read.</summary>
    public int Reads { get; private set; }

    /// <inheritdoc />
    public string? AssertedTenant
    {
        get
        {
            Reads++;
            return _tenant;
        }
    }

    /// <summary>Changes the tenant later reads report.</summary>
    /// <param name="tenant">The tenant, or <see langword="null"/> for none.</param>
    public void Set(string? tenant) => _tenant = tenant;
}
