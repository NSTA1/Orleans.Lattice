using System.Text;
using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Serialization;
using Orleans.TestingHost;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Regression coverage for issue #5445 against a real Azure Table endpoint
/// (Azurite on the default development connection string): a managed WAL move
/// with <b>default</b> options to an Azure Table WAL provider copies a partition
/// holding more than one 256-entry copy page. Before the fix the copy handed
/// each whole page to <see cref="AzureTableWalStorageProvider.AppendEncodedBatchAsync"/>,
/// which refuses any batch above its 100-action transaction cap, so the move
/// failed in phase <c>Copying</c> as soon as the partition held more than 100
/// entries.
/// <para>
/// Gated under the <c>AzureStorageEmulator</c> category; <see cref="OneTimeSetUp"/>
/// probes Azurite and reports <see cref="Assert.Inconclusive(string)"/> when it
/// is unreachable.
/// </para>
/// </summary>
[TestFixture]
[Category("AzureStorageEmulator")]
public sealed class AzureTableWalMoveIntegrationTests
{
    private const string AzuriteConnectionString = "UseDevelopmentStorage=true";
    private const string AzureKey = "azure";
    private const int KeyCount = 300;

    private TestCluster _cluster = null!;

    /// <summary>The providers the silo resolves, held statically so the test can inspect each store.</summary>
    private static class Providers
    {
        public static string TableName { get; set; } = string.Empty;

        public static InMemoryWalStorageProvider Baseline { get; set; } = new();

        public static AzureTableWalStorageProvider? Azure { get; set; }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.AddWalStorage(_ => Providers.Baseline);
            siloBuilder.AddLatticeWalStorageProvider(AzureKey, sp => Providers.Azure = new AzureTableWalStorageProvider(
                Options.Create(new AzureTableWalStorageOptions
                {
                    ConnectionString = AzuriteConnectionString,
                    TableName = Providers.TableName,
                    Compression = LatticeCompression.None,
                }),
                sp.GetRequiredService<Serializer<WalRecord>>()));
            siloBuilder.UseInMemoryReminderService();
        }
    }

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        try
        {
            await foreach (var _ in new TableServiceClient(AzuriteConnectionString).QueryAsync(maxPerPage: 1))
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

        Providers.TableName = "Tmove" + Guid.NewGuid().ToString("N");
        Providers.Baseline = new InMemoryWalStorageProvider();
        Providers.Azure = null;

        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }

        try
        {
            await new TableServiceClient(AzuriteConnectionString).DeleteTableAsync(Providers.TableName);
        }
        catch (RequestFailedException)
        {
            // Best-effort cleanup; the table name is unique per run.
        }
    }

    private static async Task<List<long>> ReadOffsetsAsync(IWalStorageProvider provider, string treeId)
    {
        var offsets = new List<long>();
        await foreach (var entry in provider.ReadAsync(treeId, 0, -1, 10_000, CancellationToken.None))
        {
            offsets.Add(entry.Offset);
        }
        return offsets;
    }

    [Test]
    public async Task A_move_with_default_options_copies_a_partition_larger_than_a_copy_page_to_Azure_Table()
    {
        var treeId = $"move-azure-{Guid.NewGuid():N}";
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = 1, WalPartitions = 1 });
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);
        var physical = (await tree.GetRoutingAsync()).PhysicalTreeId;

        for (var i = 0; i < KeyCount; i++)
        {
            await tree.SetAsync($"k-{i:D4}", Encoding.UTF8.GetBytes($"v-{i}"));
        }

        // More than one default 256-entry copy page, so the copy must split a
        // page across several appends to stay under the 100-action cap.
        await TestPoll.UntilAsync(
            async () => await Providers.Baseline.GetHighestOffsetAsync(physical, 0, CancellationToken.None)
                >= WalMoveOptions.DefaultCopyPageSize,
            $"the baseline WAL for '{physical}' to hold more than {WalMoveOptions.DefaultCopyPageSize} entries",
            TimeSpan.FromSeconds(30),
            TimeSpan.FromMilliseconds(50));

        var admin = _cluster.Client.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey);
        var receipt = await admin.ExecuteWalMoveAsync(treeId, 0, AzureKey);

        Assert.That(receipt.Outcome, Is.EqualTo(WalMoveOutcome.Moved));
        var azure = Providers.Azure;
        Assert.That(azure, Is.Not.Null, "the move must have resolved the Azure Table provider");

        // ReadAsync streams only batches whose phase-2 manifest commit has
        // landed, and the copy's final append returns before its own phase 2
        // does, so read the target back behind the provider's visibility
        // barrier rather than racing that commit.
        await azure!.FlushPhaseTwoAsync(CancellationToken.None);
        var sourceOffsets = await ReadOffsetsAsync(Providers.Baseline, physical);
        var targetOffsets = await ReadOffsetsAsync(azure, physical);
        Assert.Multiple(() =>
        {
            Assert.That(sourceOffsets, Has.Count.GreaterThan(WalMoveOptions.DefaultCopyPageSize));
            Assert.That(targetOffsets, Is.EqualTo(sourceOffsets),
                "every source entry lands on the Azure Table target at its original offset");
            Assert.That(receipt.TargetHighestOffset, Is.EqualTo(sourceOffsets[^1]),
                "the verification read of the target saw the whole copy (#5466)");
        });

        var placement = await admin.GetWalPlacementAsync(treeId);
        Assert.That(placement.Partitions[0].ProviderKey, Is.EqualTo(AzureKey));

        // Post-move writes extend the Azure Table WAL, and the moved data still reads back.
        var movedHighest = targetOffsets[^1];
        await tree.SetAsync("after-move", Encoding.UTF8.GetBytes("after"));
        await TestPoll.UntilAsync(
            async () => await azure!.GetHighestOffsetAsync(physical, 0, CancellationToken.None) > movedHighest,
            "a post-move write to extend the Azure Table WAL",
            TimeSpan.FromSeconds(30),
            TimeSpan.FromMilliseconds(50));
        var first = await tree.GetAsync("k-0000");
        Assert.That(first, Is.EqualTo(Encoding.UTF8.GetBytes("v-0")));
    }
}
