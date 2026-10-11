using System.Reflection;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>Enrols membership exceptions in the shared domain-refusal marker contract.</summary>
[TestFixture]
public sealed class DomainFaultMarkerContractTests : DomainFaultMarkerContractTestsBase
{
    /// <inheritdoc />
    protected override Assembly PackageAssembly => typeof(LatticeDirectoryValidationException).Assembly;

    /// <inheritdoc />
    protected override Type DomainFaultMarkerType => typeof(ILatticeDomainFault);

    /// <inheritdoc />
    protected override string PackageAnchorDescription => nameof(LatticeDirectoryValidationException);
}
