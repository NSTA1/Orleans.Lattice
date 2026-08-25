using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

/// <summary>
/// A deterministic <see cref="IExplorerTenantOperatorGate"/> that returns a fixed
/// platform-operator verdict, so the view's fail-closed matrix can be exercised
/// without a live capability probe. No timing, ordering, or wall-clock dependence.
/// </summary>
internal sealed class StubOperatorGate(bool isOperator) : IExplorerTenantOperatorGate
{
    public ValueTask<bool> IsPlatformOperatorAsync(CancellationToken cancellationToken = default) =>
        new(isOperator);
}
