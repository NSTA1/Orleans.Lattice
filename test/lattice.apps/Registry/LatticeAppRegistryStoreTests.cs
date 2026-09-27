using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeAppRegistryStore"/> over a substituted
/// <see cref="ILattice"/>: it addresses the reserved <c>sys-app-registry</c> tree, runs
/// every call under system-origin, and stores records in the Orleans wire format.
/// </summary>
[TestFixture]
public sealed class LatticeAppRegistryStoreTests
{
    private ServiceProvider _services = null!;
    private Serializer<AppRegistryRecord> _serializer = null!;
    private ILattice _lattice = null!;
    private LatticeAppRegistryStore _store = null!;

    [SetUp]
    public void SetUp()
    {
        _services = new ServiceCollection()
            .AddSerializer(builder => builder
                .AddAssembly(typeof(AppRegistryRecord).Assembly)
                .AddAssembly(typeof(LatticeScope).Assembly)
                .AddAssembly(typeof(TenantId).Assembly))
            .BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<AppRegistryRecord>>();
        _lattice = Substitute.For<ILattice>();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(AppRegistryTreeNames.RegistryTree, Arg.Any<string?>()).Returns(_lattice);
        _store = new LatticeAppRegistryStore(grainFactory, _serializer);
    }

    [TearDown]
    public void TearDown() => _services.Dispose();

    [Test]
    public async Task GetAsync_reads_the_registry_tree_under_system_origin_and_deserializes()
    {
        var record = AppRegistryTestData.Record(AppRegistryLifecycleState.Enabled);
        var version = new HybridLogicalClock { WallClockTicks = 42 };
        var observedSystemOrigin = false;
        _lattice.GetWithVersionAsync("default/notes", Arg.Any<CancellationToken>()).Returns(_ =>
        {
            observedSystemOrigin = LatticeSystemOrigin.IsActive;
            return Task.FromResult(new VersionedValue { Value = _serializer.SerializeToArray(record), Version = version });
        });

        var read = await _store.GetAsync("default/notes", CancellationToken.None);

        Assert.That(observedSystemOrigin, Is.True);
        Assert.That(LatticeSystemOrigin.IsActive, Is.False, "the system-origin scope does not leak to the caller");
        Assert.That(read.Version, Is.EqualTo(version));
        Assert.That(read.Record!.State, Is.EqualTo(AppRegistryLifecycleState.Enabled));
        Assert.That(read.Record.Slug, Is.EqualTo(record.Slug));
    }

    [Test]
    public async Task GetAsync_miss_returns_null_at_version_zero()
    {
        _lattice.GetWithVersionAsync("default/notes", Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new VersionedValue { Value = null, Version = new HybridLogicalClock { WallClockTicks = 5 } }));

        var read = await _store.GetAsync("default/notes", CancellationToken.None);

        Assert.That(read.Record, Is.Null);
        Assert.That(read.Version, Is.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public async Task TrySetAsync_writes_conditionally_under_system_origin_in_the_orleans_format()
    {
        var record = AppRegistryTestData.Record(AppRegistryLifecycleState.Installed);
        var expected = new HybridLogicalClock { WallClockTicks = 7 };
        byte[]? written = null;
        var observedSystemOrigin = false;
        _lattice.SetIfVersionAsync("default/notes", Arg.Any<byte[]>(), expected, Arg.Any<CancellationToken>()).Returns(call =>
        {
            observedSystemOrigin = LatticeSystemOrigin.IsActive;
            written = call.ArgAt<byte[]>(1);
            return Task.FromResult(true);
        });

        var applied = await _store.TrySetAsync("default/notes", record, expected, CancellationToken.None);

        Assert.That(applied, Is.True);
        Assert.That(observedSystemOrigin, Is.True);
        Assert.That(_serializer.Deserialize(written!).State, Is.EqualTo(AppRegistryLifecycleState.Installed));
    }

    [Test]
    public async Task ScanAsync_passes_the_range_and_deserializes_every_entry_under_system_origin()
    {
        var first = AppRegistryTestData.Record(AppRegistryLifecycleState.Enabled, slug: AppSlug.Parse("alpha"));
        var second = AppRegistryTestData.Record(AppRegistryLifecycleState.Disabled, slug: AppSlug.Parse("beta"));
        var observedSystemOrigin = false;
        _lattice.EntriesAsync("acme/", "acme0", false, Arg.Any<bool?>(), Arg.Any<CancellationToken>()).Returns(_ =>
        {
            observedSystemOrigin = LatticeSystemOrigin.IsActive;
            return Entries(("acme/alpha", first), ("acme/beta", second));
        });

        var records = await _store.ScanAsync("acme/", "acme0", CancellationToken.None).ToListAsync();

        Assert.That(observedSystemOrigin, Is.True);
        Assert.That(records.Select(r => r.Slug.Value), Is.EqualTo(new[] { "alpha", "beta" }));
    }

    [Test]
    public void Constructor_null_arguments_throw()
    {
        Assert.That(() => new LatticeAppRegistryStore(null!, _serializer), Throws.ArgumentNullException);
        Assert.That(() => new LatticeAppRegistryStore(Substitute.For<IGrainFactory>(), null!), Throws.ArgumentNullException);
    }

    private async IAsyncEnumerable<KeyValuePair<string, byte[]>> Entries(params (string Key, AppRegistryRecord Record)[] entries)
    {
        foreach (var (key, record) in entries)
        {
            yield return new KeyValuePair<string, byte[]>(key, _serializer.SerializeToArray(record));
        }

        await Task.CompletedTask;
    }
}
