using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.ContractProbes;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Runs the shared <see cref="WalStorageProviderContractTestsBase"/> suite
/// against <see cref="InMemoryWalStorageProvider"/>.
/// <para>
/// The provider is volatile, so a reopen is a no-op: the instance outliving the
/// grain that reads from it is what a reactivation looks like here.
/// </para>
/// </summary>
[TestFixture]
public sealed class InMemoryWalStorageProviderContractTests : WalStorageProviderContractTestsBase
{
    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<WalRecord>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    protected override Task<IWalStorageProviderContractProbe> CreateProbeAsync() =>
        Task.FromResult<IWalStorageProviderContractProbe>(
            new WalStorageProviderContractProbe(() => new InMemoryWalStorageProvider(), _serializer, durable: false));
}
