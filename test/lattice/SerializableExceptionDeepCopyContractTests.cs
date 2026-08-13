using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Testing;
using Orleans.Serialization;
using Orleans.Serialization.Cloning;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Concrete guard for the core <c>Orleans.Lattice</c> assembly: every
/// <c>[GenerateSerializer]</c> exception it declares must deep-copy across a
/// same-silo boundary. Reuses
/// <see cref="SerializableExceptionDeepCopyContractTestsBase"/> so a newly added
/// serializable exception deriving from a BCL exception subclass (which needs a
/// no-op <c>[RegisterCopier] IDeepCopier&lt;T&gt;</c>) is caught in CI.
/// </summary>
[TestFixture]
public sealed class SerializableExceptionDeepCopyContractTests : SerializableExceptionDeepCopyContractTestsBase
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp() =>
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    /// <inheritdoc />
    protected override Assembly PackageAssembly => typeof(LatticeWriteFencedException).Assembly;

    /// <inheritdoc />
    protected override IServiceProvider Services => _services;

    /// <inheritdoc />
    protected override Type DeepCopierType => typeof(DeepCopier<>);
}
