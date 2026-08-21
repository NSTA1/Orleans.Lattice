using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Grain-level coverage for <see cref="LatticeGrain.ScanEntryHistoryAsync"/> when the
/// tree HAS opted into a durable history view - the branch that resolves the view
/// registration, scans the active-generation view tree, decodes each stored
/// <see cref="HistoryRow"/>, filters by source key, honours the HLC bounds, and pages
/// at the effective limit. Also covers the history-retention getter/setter that read
/// and write the registry entry. The grain is constructed in-process with substituted
/// services and a real <see cref="HistoryRowCodec"/>, so every branch is deterministic
/// and needs no cluster.
/// </summary>
[TestFixture]
public sealed class LatticeGrainHistoryViewScanTests
{
    private const string TreeId = "hist-view-scan";
    private const string ViewName = "hist-view";
    private const string ActiveViewTreeId = "view-active";

    private ServiceProvider _serializerServices = null!;
    private HistoryRowCodec _codec = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _serializerServices = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _codec = new HistoryRowCodec(_serializerServices.GetRequiredService<Serializer<HistoryRow>>());
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _serializerServices.Dispose();

    private static HybridLogicalClock Clock(long wall) => new() { WallClockTicks = wall };

    private byte[] Row(string sourceKey, long wall) => _codec.Encode(new HistoryRow
    {
        Timestamp = Clock(wall),
        Kind = HistoryRowKind.Set,
        SourceKey = sourceKey,
        Value = new byte[] { (byte)(wall & 0xFF) },
        ValueLength = 1,
    });

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> Entries(
        IEnumerable<KeyValuePair<string, byte[]>> rows)
    {
        foreach (var row in rows)
        {
            yield return row;
        }

        await Task.CompletedTask;
    }

    /// <summary>
    /// Builds a grain whose services expose a durable history view over
    /// <see cref="TreeId"/>, backed by a substituted view tree that yields the given
    /// encoded rows in order.
    /// </summary>
    private LatticeGrain CreateGrainWithHistoryView(
        ILatticeRegistry registry,
        IEnumerable<KeyValuePair<string, byte[]>> viewRows)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", TreeId));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        var maintainer = Substitute.For<IViewMaintainerGrain>();
        maintainer.GetActiveTreeIdAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(ActiveViewTreeId));
        grainFactory.GetGrain<IViewMaintainerGrain>(ViewName).Returns(maintainer);

        var viewTree = Substitute.For<ILattice>();
        viewTree.EntriesAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ => Entries(viewRows));
        grainFactory.GetGrain<ILattice>(ActiveViewTreeId).Returns(viewTree);

        var catalog = Substitute.For<IViewCatalog>();
        catalog.All().Returns(new[]
        {
            new ViewRegistration(ViewName, TreeId, new HistoryLatticeViewProjection(_codec), Accumulative: true),
        });

        var services = Substitute.For<IServiceProvider>();
        services.GetService(typeof(IViewCatalog)).Returns(catalog);
        services.GetService(typeof(HistoryRowCodec)).Returns(_codec);

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions { WalPartitions = 1 });
        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory, new LatticeOptions { WalPartitions = 1 });

        return new LatticeGrain(
            context, grainFactory, optionsMonitor, optionsResolver, services, NullLogger<LatticeGrain>.Instance);
    }

    private static ILatticeRegistry RegistryReturning(TreeRegistryEntry? entry)
    {
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult(entry));
        return registry;
    }

    [Test]
    public async Task ScanEntryHistoryAsync_view_path_returns_only_the_keys_rows_in_order()
    {
        var registry = RegistryReturning(new TreeRegistryEntry());
        var grain = CreateGrainWithHistoryView(registry, new[]
        {
            new KeyValuePair<string, byte[]>("k/0001", Row("k", 10)),
            new KeyValuePair<string, byte[]>("other/0002", Row("other", 11)),
            new KeyValuePair<string, byte[]>("k/0003", Row("k", 12)),
        });

        var page = await grain.ScanEntryHistoryAsync("k", null, null, 100, null);

        Assert.Multiple(() =>
        {
            Assert.That(page.Source, Is.EqualTo(EntryHistorySource.View));
            Assert.That(page.Revisions, Has.Count.EqualTo(2));
            Assert.That(page.Revisions[0].Hlc.WallClockTicks, Is.EqualTo(10));
            Assert.That(page.Revisions[1].Hlc.WallClockTicks, Is.EqualTo(12));
            Assert.That(page.Truncated, Is.False);
            Assert.That(page.Continuation, Is.Null);
        });
    }

    [Test]
    public async Task ScanEntryHistoryAsync_view_path_stops_at_the_upper_hlc_bound()
    {
        var registry = RegistryReturning(new TreeRegistryEntry());
        var grain = CreateGrainWithHistoryView(registry, new[]
        {
            new KeyValuePair<string, byte[]>("k/0001", Row("k", 10)),
            new KeyValuePair<string, byte[]>("k/0002", Row("k", 20)),
            new KeyValuePair<string, byte[]>("k/0003", Row("k", 30)),
        });

        var page = await grain.ScanEntryHistoryAsync("k", null, Clock(20), 100, null);

        Assert.Multiple(() =>
        {
            Assert.That(page.Revisions.Select(r => r.Hlc.WallClockTicks), Is.EqualTo(new long[] { 10, 20 }));
            Assert.That(page.Continuation, Is.Null, "the break at the upper bound ends the timeline");
        });
    }

    [Test]
    public async Task ScanEntryHistoryAsync_view_path_skips_rows_below_the_lower_hlc_bound()
    {
        var registry = RegistryReturning(new TreeRegistryEntry());
        var grain = CreateGrainWithHistoryView(registry, new[]
        {
            new KeyValuePair<string, byte[]>("k/0001", Row("k", 10)),
            new KeyValuePair<string, byte[]>("k/0002", Row("k", 20)),
            new KeyValuePair<string, byte[]>("k/0003", Row("k", 30)),
        });

        var page = await grain.ScanEntryHistoryAsync("k", Clock(20), null, 100, null);

        Assert.That(page.Revisions.Select(r => r.Hlc.WallClockTicks), Is.EqualTo(new long[] { 20, 30 }));
    }

    [Test]
    public async Task ScanEntryHistoryAsync_view_path_pages_at_the_effective_limit()
    {
        var registry = RegistryReturning(new TreeRegistryEntry());
        var grain = CreateGrainWithHistoryView(registry, new[]
        {
            new KeyValuePair<string, byte[]>("k/0001", Row("k", 10)),
            new KeyValuePair<string, byte[]>("k/0002", Row("k", 20)),
            new KeyValuePair<string, byte[]>("k/0003", Row("k", 30)),
        });

        var page = await grain.ScanEntryHistoryAsync("k", null, null, 2, null);

        Assert.Multiple(() =>
        {
            Assert.That(page.Revisions.Select(r => r.Hlc.WallClockTicks), Is.EqualTo(new long[] { 10, 20 }));
            Assert.That(page.Continuation, Is.EqualTo("k/0002"), "the limit-reached row's view key resumes paging");
        });
    }

    [Test]
    public async Task GetHistoryRetentionAsync_maps_the_registry_entry_mode_and_window()
    {
        var window = TimeSpan.FromHours(2);
        var registry = RegistryReturning(new TreeRegistryEntry
        {
            HistoryRetentionMode = HistoryRetentionMode.FullValue,
            HistoryRetentionWindowTicks = window.Ticks,
        });
        var grain = CreateGrainWithHistoryView(registry, Array.Empty<KeyValuePair<string, byte[]>>());

        var settings = await grain.GetHistoryRetentionAsync();

        Assert.Multiple(() =>
        {
            Assert.That(settings.Mode, Is.EqualTo(HistoryRetentionMode.FullValue));
            Assert.That(settings.Window, Is.EqualTo(window));
        });
    }

    [Test]
    public async Task GetHistoryRetentionAsync_defaults_to_metadata_only_when_no_entry()
    {
        var registry = RegistryReturning(null);
        var grain = CreateGrainWithHistoryView(registry, Array.Empty<KeyValuePair<string, byte[]>>());

        var settings = await grain.GetHistoryRetentionAsync();

        Assert.Multiple(() =>
        {
            Assert.That(settings.Mode, Is.EqualTo(HistoryRetentionMode.MetadataOnly));
            Assert.That(settings.Window, Is.EqualTo(TimeSpan.Zero));
        });
    }

    [Test]
    public void GetHistoryRetentionAsync_honours_a_cancelled_token()
    {
        var registry = RegistryReturning(new TreeRegistryEntry());
        var grain = CreateGrainWithHistoryView(registry, Array.Empty<KeyValuePair<string, byte[]>>());
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await grain.GetHistoryRetentionAsync(cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task SetHistoryRetentionAsync_writes_the_mode_and_window_to_the_registry()
    {
        var registry = RegistryReturning(new TreeRegistryEntry());
        var grain = CreateGrainWithHistoryView(registry, Array.Empty<KeyValuePair<string, byte[]>>());
        var window = TimeSpan.FromMinutes(30);

        await grain.SetHistoryRetentionAsync(HistoryRetentionMode.Hybrid, window);

        await registry.Received(1).SetHistoryRetentionAsync(TreeId, HistoryRetentionMode.Hybrid, window);
    }

    [Test]
    public void SetHistoryRetentionAsync_honours_a_cancelled_token()
    {
        var registry = RegistryReturning(new TreeRegistryEntry());
        var grain = CreateGrainWithHistoryView(registry, Array.Empty<KeyValuePair<string, byte[]>>());
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await grain.SetHistoryRetentionAsync(HistoryRetentionMode.FullValue, null, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }
}
