using System.Reflection;
using Orleans.Lattice.Operations;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.Storage;

/// <summary>
/// Concrete guard for the <c>Orleans.Lattice</c> assembly: every grain
/// persistent-state type it declares must round-trip through the JSON
/// grain-storage serializer a silo uses for Azure Table storage (issue #5579).
/// Reuses <see cref="GrainStateStorageRoundTripContractTestsBase"/>.
/// </summary>
[TestFixture]
public sealed class GrainStateStorageRoundTripContractTests : GrainStateStorageRoundTripContractTestsBase
{
    private SiloJsonGrainStorage storage = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp() => this.storage = new SiloJsonGrainStorage();

    [OneTimeTearDown]
    public void OneTimeTearDown() => this.storage.Dispose();

    /// <inheritdoc />
    protected override Assembly PackageAssembly => typeof(LatticeOperationGrainState).Assembly;

    /// <inheritdoc />
    protected override T RoundTrip<T>(T state) => this.storage.RoundTrip(state);
}
