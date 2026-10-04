using System.Reflection;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Enrols the <c>Orleans.Lattice.Replication</c> assembly in the shared
/// <see cref="DomainFaultMarkerContractTestsBase"/> guard: every exception this
/// package declares whose base type is foreign (for example
/// <see cref="InvalidOperationException"/>) must implement
/// <see cref="ILatticeDomainFault"/>, so a broad <c>catch</c> above the
/// replication surface can decline a domain refusal with
/// <c>when (ex is not ILatticeDomainFault)</c>.
/// </summary>
[TestFixture]
public sealed class DomainFaultMarkerContractTests : DomainFaultMarkerContractTestsBase
{
    /// <inheritdoc />
    protected override Assembly PackageAssembly => typeof(LatticeReplicationModeChangeRejectedException).Assembly;

    /// <inheritdoc />
    protected override Type DomainFaultMarkerType => typeof(ILatticeDomainFault);

    /// <inheritdoc />
    protected override string PackageAnchorDescription => nameof(LatticeReplicationModeChangeRejectedException);
}
