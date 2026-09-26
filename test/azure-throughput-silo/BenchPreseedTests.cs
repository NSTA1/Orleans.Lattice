using NSubstitute;
using Orleans.Lattice;
using VehicleFleetSimulator.AzureThroughput.Engine;

namespace VehicleFleetSimulator.AzureThroughput.Silo.Tests;

/// <summary>
/// Pins the read-mode preseed shared by the single-VM silo and the Layer 3
/// producer (#3474): it must write exactly the keys the producer's generator
/// reads, only for the read modes, in bounded slices.
/// </summary>
[TestFixture]
public class BenchPreseedTests
{
    // The producer's ChannelGenerator recipe, verbatim (uint literals).
    private static string GeneratorKey(int i)
    {
        Span<byte> idBytes = stackalloc byte[16];
        BitConverter.TryWriteBytes(idBytes[..4], i);
        BitConverter.TryWriteBytes(idBytes.Slice(4, 4), 0xC0FFEE);
        BitConverter.TryWriteBytes(idBytes.Slice(8, 4), 0xDEADBEEF);
        BitConverter.TryWriteBytes(idBytes.Slice(12, 4), 0xCAFEBABE);
        return new Guid(idBytes).ToString("N");
    }

    [TestCase(0)]
    [TestCase(1)]
    [TestCase(255)]
    [TestCase(1_000_000)]
    public void KeyFor_matches_generator_vehicle_key(int index)
    {
        Assert.That(BenchPreseed.KeyFor(index), Is.EqualTo(GeneratorKey(index)));
        Assert.That(BenchPreseed.KeyFor(index), Has.Length.EqualTo(32));
    }

    [TestCase(BenchWorkloadMode.GetPoint, 10, true)]
    [TestCase(BenchWorkloadMode.GetMany, 10, true)]
    [TestCase(BenchWorkloadMode.GetPoint, 0, false)]
    [TestCase(BenchWorkloadMode.SetPoint, 10, false)]
    [TestCase(BenchWorkloadMode.SetPointMv, 10, false)]
    [TestCase(BenchWorkloadMode.SetMany, 10, false)]
    public void IsRequired_only_for_read_modes_with_keys(BenchWorkloadMode mode, int keyCount, bool expected)
    {
        Assert.That(BenchPreseed.IsRequired(mode, keyCount), Is.EqualTo(expected));
    }

    [Test]
    public void BuildEntries_is_deterministic_with_fixed_payload_size()
    {
        var entries = BenchPreseed.BuildEntries(3);

        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { GeneratorKey(0), GeneratorKey(1), GeneratorKey(2) }));
        Assert.That(entries.All(e => e.Value.Length == BenchPreseed.PayloadBytes), Is.True);
        Assert.That(entries[1].Value[0], Is.EqualTo(1));
        Assert.That(entries[1].Value[BenchPreseed.PayloadBytes - 1], Is.EqualTo((byte)BenchPreseed.PayloadBytes));
        Assert.That(BenchPreseed.BuildEntries(3)[2].Value, Is.EqualTo(entries[2].Value));
    }

    [Test]
    public async Task SeedAsync_writes_every_key_in_bounded_slices()
    {
        var lattice = Substitute.For<ILattice>();
        var sliceSizes = new List<int>();
        var keys = new List<string>();
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                var slice = ci.Arg<List<KeyValuePair<string, byte[]>>>();
                sliceSizes.Add(slice.Count);
                keys.AddRange(slice.Select(e => e.Key));
                return Task.CompletedTask;
            });

        var written = await BenchPreseed.SeedAsync(lattice, count: 10, sliceSize: 4, CancellationToken.None);

        Assert.That(written, Is.EqualTo(10));
        Assert.That(sliceSizes, Is.EqualTo(new[] { 4, 4, 2 }));
        Assert.That(keys, Is.EqualTo(Enumerable.Range(0, 10).Select(GeneratorKey).ToArray()));
    }

    [Test]
    public async Task SeedAsync_resumes_from_the_last_landed_slice_after_a_failure()
    {
        var lattice = Substitute.For<ILattice>();
        var keys = new List<string>();
        var calls = 0;
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                if (++calls == 2)
                {
                    return Task.FromException(new InvalidOperationException("saturated"));
                }

                keys.AddRange(ci.Arg<List<KeyValuePair<string, byte[]>>>().Select(e => e.Key));
                return Task.CompletedTask;
            });
        var landed = 0;

        Assert.ThrowsAsync<InvalidOperationException>(() =>
            BenchPreseed.SeedAsync(lattice, count: 10, sliceSize: 4, CancellationToken.None, landed, o => landed = o));
        Assert.That(landed, Is.EqualTo(4));

        var reported = new List<int>();
        var written = await BenchPreseed.SeedAsync(lattice, 10, 4, CancellationToken.None, landed, o => { landed = o; reported.Add(o); });

        Assert.That(written, Is.EqualTo(10));
        Assert.That(reported, Is.EqualTo(new[] { 8, 10 }));
        Assert.That(keys, Is.EqualTo(Enumerable.Range(0, 10).Select(GeneratorKey).ToArray()), "no landed slice is re-written");
    }

    [Test]
    public async Task SeedAsync_with_start_offset_at_count_writes_nothing()
    {
        var lattice = Substitute.For<ILattice>();

        var written = await BenchPreseed.SeedAsync(lattice, 10, 4, CancellationToken.None, startOffset: 10);

        Assert.That(written, Is.EqualTo(10));
        await lattice.DidNotReceiveWithAnyArgs().SetManyAsync(default!, default);
    }

    [TestCase(-1)]
    [TestCase(11)]
    public void SeedAsync_rejects_a_start_offset_outside_the_seed(int startOffset)
    {
        var lattice = Substitute.For<ILattice>();

        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => BenchPreseed.SeedAsync(lattice, 10, 4, CancellationToken.None, startOffset));
    }

    [Test]
    public void SeedAsync_rejects_non_positive_slice_size()
    {
        var lattice = Substitute.For<ILattice>();

        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => BenchPreseed.SeedAsync(lattice, 1, 0, CancellationToken.None));
    }
}
