using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.ContractProbes;
using Orleans.Serialization;
using Orleans.Serialization.Session;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Runs the shared <see cref="WalStorageProviderContractTestsBase"/> suite
/// against <see cref="AzureTableWalStorageProvider"/>.
/// <para>
/// Every append crosses <see cref="AzureTableWalStorageProvider.FlushPhaseTwoAsync"/>,
/// the barrier the provider's real caller crosses before treating an append as
/// durable: reads and trims walk manifest rows, which phase 2 lands.
/// </para>
/// <para>
/// Emulator-gated like the other Azure fixtures. When Azurite is absent these
/// tests report neither pass nor fail - only a lower <c>Total</c> - so a green
/// run is not evidence this provider was covered.
/// </para>
/// </summary>
[TestFixture]
[Category("AzureStorageEmulator")]
public sealed class AzureTableWalStorageProviderContractTests : WalStorageProviderContractTestsBase
{
    private const string AzuriteConnectionString = "UseDevelopmentStorage=true";

    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;
    private TableServiceClient _adminClient = null!;
    private string _tableName = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<WalRecord>>();
        _adminClient = new TableServiceClient(AzuriteConnectionString);

        try
        {
            await foreach (var _ in _adminClient.QueryAsync(maxPerPage: 1))
            {
                break;
            }
        }
        catch (Exception ex)
        {
            Assert.Inconclusive(
                $"Azurite is not reachable on the default development endpoint ({AzuriteConnectionString}). "
                + $"Start it via 'azurite --silent --location <dir>' or skip the AzureStorageEmulator category. "
                + $"Underlying error: {ex.GetType().Name}: {ex.Message}");
        }
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [SetUp]
    public void SetUp() => _tableName = "T" + Guid.NewGuid().ToString("N");

    [TearDown]
    public async Task TearDown()
    {
        try
        {
            await _adminClient.DeleteTableAsync(_tableName);
        }
        catch (RequestFailedException)
        {
            // Best-effort cleanup; a missing table is acceptable.
        }
    }

    protected override Task<IWalStorageProviderContractProbe> CreateProbeAsync()
    {
        var tableName = _tableName;
        var serializer = _serializer;
        var routing = new WalRecordRoutingReader(_services.GetRequiredService<SerializerSessionPool>());

        // The shape AddAzureTableWalStorage registers, routing reader included,
        // so the suite exercises the filtered read's prefix classification
        // (issue #3565) rather than only its full-decode fallback.
        return Task.FromResult<IWalStorageProviderContractProbe>(
            new WalStorageProviderContractProbe(
                () => new AzureTableWalStorageProvider(
                    Options.Create(new AzureTableWalStorageOptions
                    {
                        ConnectionString = AzuriteConnectionString,
                        TableName = tableName,
                        Compression = LatticeCompression.None,
                    }),
                    serializer,
                    saturationSignal: null,
                    compressors: null,
                    routing),
                serializer,
                durable: true,
                durabilityBarrier: static (provider, ct) =>
                    ((AzureTableWalStorageProvider)provider).FlushPhaseTwoAsync(ct)));
    }
}
