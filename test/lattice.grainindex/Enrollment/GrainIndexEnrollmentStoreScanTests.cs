using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.GrainIndex.Enrollment;
using Orleans.Lattice.GrainIndex.Registry;
using Orleans.Serialization;

namespace Orleans.Lattice.GrainIndex.Tests.Enrollment;

/// <summary>
/// Covers the seen-marker range scan the background backfill uses to skip
/// already-indexed grains: the registry range it reads and the grain keys it
/// hands back.
/// </summary>
[TestFixture]
public sealed class GrainIndexEnrollmentStoreScanTests
{
    private ServiceProvider _provider = null!;
    private GrainIndexEnrollmentStore _store = null!;
    private ILattice _registry = null!;

    [SetUp]
    public void SetUp()
    {
        var services = new ServiceCollection();
        services.AddSerializer();
        _provider = services.BuildServiceProvider();

        _registry = Substitute.For<ILattice>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILattice>(Arg.Any<string>(), Arg.Any<string?>()).Returns(_registry);

        _store = new GrainIndexEnrollmentStore(
            factory,
            new OrleansGrainIndexSerializer<GrainIndexEnrollmentRecord>(
                _provider.GetRequiredService<Serializer<GrainIndexEnrollmentRecord>>()),
            new OrleansGrainIndexSerializer<GrainIndexPendingProjection>(
                _provider.GetRequiredService<Serializer<GrainIndexPendingProjection>>()));
    }

    [TearDown]
    public void TearDown() => _provider.Dispose();

    private void Yielding(params string[] registryKeys) =>
        _registry
            .KeysAsync(
                Arg.Any<string?>(),
                Arg.Any<string?>(),
                Arg.Any<bool>(),
                Arg.Any<bool?>(),
                Arg.Any<CancellationToken>())
            .Returns(AsAsync(registryKeys));

    private static async IAsyncEnumerable<string> AsAsync(string[] keys)
    {
        foreach (var key in keys)
        {
            yield return key;
            await Task.Yield();
        }
    }

    private static async Task<List<string>> CollectAsync(IAsyncEnumerable<string> keys)
    {
        var collected = new List<string>();
        await foreach (var key in keys)
            collected.Add(key);

        return collected;
    }

    [Test]
    public async Task The_scan_reads_the_half_open_range_that_covers_the_inclusive_bounds()
    {
        Yielding();

        await CollectAsync(_store.ScanSeenKeysAsync("users", "a", "m", CancellationToken.None));

        var prefix = GrainIndexRegistryKeys.SeenPrefix("users");
        _registry.Received(1).KeysAsync(
            prefix + "a",
            prefix + "m" + "\u0000",
            Arg.Any<bool>(),
            Arg.Any<bool?>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task The_scan_strips_the_marker_prefix_and_returns_grain_keys()
    {
        var prefix = GrainIndexRegistryKeys.SeenPrefix("users");
        Yielding(prefix + "alice", prefix + "bob");

        var keys = await CollectAsync(_store.ScanSeenKeysAsync("users", "a", "z", CancellationToken.None));

        Assert.That(keys, Is.EqualTo(new[] { "alice", "bob" }),
            "The crawl compares grain keys, so the marker prefix is the store's business and not "
            + "something a caller should have to strip.");
    }

    [Test]
    public async Task A_range_with_no_markers_yields_nothing()
    {
        Yielding();

        var keys = await CollectAsync(_store.ScanSeenKeysAsync("users", "a", "z", CancellationToken.None));

        Assert.That(keys, Is.Empty);
    }

    [Test]
    public async Task A_single_key_range_is_expressible()
    {
        var prefix = GrainIndexRegistryKeys.SeenPrefix("users");
        Yielding(prefix + "alice");

        var keys = await CollectAsync(_store.ScanSeenKeysAsync("users", "alice", "alice", CancellationToken.None));

        Assert.That(keys, Is.EqualTo(new[] { "alice" }),
            "A batch of one is a legitimate batch, and its upper bound has to include its own key.");
    }

    [Test]
    public void Every_argument_is_null_checked()
    {
        Yielding();

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await CollectAsync(_store.ScanSeenKeysAsync(null!, "a", "z", CancellationToken.None)),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await CollectAsync(_store.ScanSeenKeysAsync("users", null!, "z", CancellationToken.None)),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await CollectAsync(_store.ScanSeenKeysAsync("users", "a", null!, CancellationToken.None)),
                Throws.ArgumentNullException);
        });
    }
}
