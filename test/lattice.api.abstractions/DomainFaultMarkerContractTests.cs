using System.Reflection;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.Abstractions.Tests;

/// <summary>Enrols API contract exceptions in the shared domain-refusal marker contract.</summary>
[TestFixture]
public sealed class DomainFaultMarkerContractTests : DomainFaultMarkerContractTestsBase
{
    /// <inheritdoc />
    protected override Assembly PackageAssembly => typeof(TenantAccessConfinementException).Assembly;

    /// <inheritdoc />
    protected override Type DomainFaultMarkerType => typeof(ILatticeDomainFault);

    /// <inheritdoc />
    protected override string PackageAnchorDescription => nameof(TenantAccessConfinementException);
}
