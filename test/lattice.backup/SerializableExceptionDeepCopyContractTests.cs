using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Testing;
using Orleans.Serialization;
using Orleans.Serialization.Cloning;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Concrete guard for the <c>Orleans.Lattice.Backup</c> assembly: every
/// <c>[GenerateSerializer]</c> exception it declares must deep-copy across a
/// same-silo boundary. Reuses
/// <see cref="SerializableExceptionDeepCopyContractTestsBase"/>. Enrolment is
/// required by <c>SerializableExceptionDeepCopyGateEnrolmentTests</c> because the
/// package declares <see cref="LatticeBackupCrossTreeFenceException"/>.
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
    protected override Assembly PackageAssembly => typeof(LatticeBackupCrossTreeFenceException).Assembly;

    /// <inheritdoc />
    protected override IServiceProvider Services => _services;

    /// <inheritdoc />
    protected override Type DeepCopierType => typeof(DeepCopier<>);
}
