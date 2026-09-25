using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NUnit.Framework;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.ContractProbes;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Serialization;
using Orleans.Serialization.Session;

namespace Orleans.Lattice.Storage.File.Tests;

/// <summary>
/// Runs the shared <see cref="WalStorageProviderContractTestsBase"/> suite
/// against <see cref="FileWalStorageProvider"/>. A reopen disposes the provider
/// and rebuilds it over the same directory, so every post-reopen assertion runs
/// through the real on-disk recovery path.
/// </summary>
[TestFixture]
public sealed class FileWalStorageProviderContractTests : WalStorageProviderContractTestsBase
{
    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;
    private string _root = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<WalRecord>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(
            Path.GetTempPath(),
            "lattice-wal-provider-contract",
            Guid.NewGuid().ToString("N"));
        System.IO.Directory.CreateDirectory(_root);
    }

    [TearDown]
    public void TearDown()
    {
        try
        {
            if (System.IO.Directory.Exists(_root))
            {
                System.IO.Directory.Delete(_root, recursive: true);
            }
        }
        catch (IOException)
        {
            // Best-effort cleanup; a leaked temp directory does not fail the test.
        }
    }

    protected override Task<IWalStorageProviderContractProbe> CreateProbeAsync()
    {
        var root = _root;
        var serializer = _serializer;
        var routing = new WalRecordRoutingReader(_services.GetRequiredService<SerializerSessionPool>());

        // The shape AddFileWalStorage registers, routing reader included, so the
        // suite exercises the filtered read's prefix classification (issue
        // #3565) rather than only its full-decode fallback.
        return Task.FromResult<IWalStorageProviderContractProbe>(
            new WalStorageProviderContractProbe(
                () => new FileWalStorageProvider(
                    Options.Create(new FileWalStorageOptions
                    {
                        RootDirectory = root,
                        FlushToDisk = true,
                    }),
                    serializer,
                    GcWalReadPressureGovernor.Instance,
                    PhysicalFileWalFileSystem.Instance,
                    routing),
                serializer,
                durable: true));
    }
}
