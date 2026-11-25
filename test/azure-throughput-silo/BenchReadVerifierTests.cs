using NSubstitute;
using NUnit.Framework;
using Orleans.Lattice;
using VehicleFleetSimulator.AzureThroughput.Engine;

namespace VehicleFleetSimulator.AzureThroughput.Silo.Tests;

[TestFixture]
public sealed class BenchReadVerifierTests
{
    [Test]
    public void Observe_classifies_correct_wrong_and_absent_values()
    {
        var verifier = new BenchReadVerifier();
        var wrong = BenchPreseed.PayloadFor(7);
        wrong[3] ^= 0xFF;

        verifier.Observe(BenchPreseed.KeyFor(7), BenchPreseed.PayloadFor(7));
        verifier.Observe(BenchPreseed.KeyFor(7), wrong);
        verifier.Observe(BenchPreseed.KeyFor(7), BenchPreseed.PayloadFor(8));
        verifier.Observe(BenchPreseed.KeyFor(9), null);
        verifier.Observe("not-a-bench-key", BenchPreseed.PayloadFor(1));

        Assert.Multiple(() =>
        {
            Assert.That(verifier.Verified, Is.EqualTo(1));
            Assert.That(verifier.Mismatch, Is.EqualTo(2));
            Assert.That(verifier.Missing, Is.EqualTo(1));
            Assert.That(verifier.Unverifiable, Is.EqualTo(1));
            Assert.That(verifier.Format(), Is.EqualTo("verified=1 mismatch=2 missing=1 unverifiable=1 sample=1"));
        });
    }

    [Test]
    public void Sampling_checks_one_read_in_every_stride()
    {
        var verifier = new BenchReadVerifier(sampleEvery: 4);
        for (var i = 0; i < 40; i++) verifier.Observe(BenchPreseed.KeyFor(i), null);

        Assert.That(verifier.Missing, Is.EqualTo(10));
    }

    [TestCase(null, 1)]
    [TestCase("", 1)]
    [TestCase("garbage", 1)]
    [TestCase("16", 16)]
    public void FromEnvironmentValue_parses_stride(string? raw, int expected)
    {
        Assert.That(BenchReadVerifier.FromEnvironmentValue(raw)!.SampleEvery, Is.EqualTo(expected));
    }

    [Test]
    public void FromEnvironmentValue_zero_disables()
    {
        Assert.That(BenchReadVerifier.FromEnvironmentValue("0"), Is.Null);
    }

    [Test]
    public async Task GetPoint_dispatch_feeds_each_read_to_the_verifier()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.GetAsync(BenchPreseed.KeyFor(1), Arg.Any<CancellationToken>()).Returns(_ => Task.FromResult<byte[]?>(BenchPreseed.PayloadFor(1)));
        lattice.GetAsync(BenchPreseed.KeyFor(2), Arg.Any<CancellationToken>()).Returns(_ => Task.FromResult<byte[]?>(null));
        var batch = new List<KeyValuePair<string, byte[]>>
        {
            new(BenchPreseed.KeyFor(1), Array.Empty<byte>()),
            new(BenchPreseed.KeyFor(2), Array.Empty<byte>()),
        };
        var verifier = new BenchReadVerifier();

        await BenchWorkloadDispatcher.DispatchAsync(
            BenchWorkloadMode.GetPoint, lattice, batch,
            atomicBatchSize: 64, parallelism: 2, ct: default, readVerifier: verifier);

        Assert.Multiple(() =>
        {
            Assert.That(verifier.Verified, Is.EqualTo(1));
            Assert.That(verifier.Missing, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task GetMany_dispatch_counts_keys_absent_from_the_result_as_missing()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.GetManyAsync(Arg.Any<List<string>>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new Dictionary<string, byte[]>
            {
                [BenchPreseed.KeyFor(1)] = BenchPreseed.PayloadFor(1),
                [BenchPreseed.KeyFor(2)] = BenchPreseed.PayloadFor(3),
            }));
        var batch = Enumerable.Range(1, 3)
            .Select(i => new KeyValuePair<string, byte[]>(BenchPreseed.KeyFor(i), Array.Empty<byte>()))
            .ToList();
        var verifier = new BenchReadVerifier();

        await BenchWorkloadDispatcher.DispatchAsync(
            BenchWorkloadMode.GetMany, lattice, batch,
            atomicBatchSize: 64, parallelism: 2, ct: default, readVerifier: verifier);

        Assert.That(verifier.Format(), Is.EqualTo("verified=1 mismatch=1 missing=1 unverifiable=0 sample=1"));
    }
}
