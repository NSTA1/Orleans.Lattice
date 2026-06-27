using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage for the opt-in shared-dictionary training-sample hook on
/// <see cref="ReplicationMutationObserver"/>: committed <see cref="MutationKind.Set"/>
/// values are fed into the injected provider's
/// <see cref="ILatticeCompressionDictionarySampler"/> only when the per-tree
/// <see cref="LatticeReplicationOptions.AutoSharedDictionaryEnabled"/> switch is
/// on, and the provider must implement the sampler seam.
/// </summary>
[TestFixture]
public class ReplicationMutationObserverSamplingTests
{
    private sealed class CapturingSink : IReplogSink
    {
        public Task WriteAsync(string treeId, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class AllowAllResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }

    private sealed class RecordingProvider
        : ILatticeCompressionDictionaryProvider, ILatticeCompressionDictionarySampler
    {
        public List<byte[]> Observed { get; } = new();

        public void Observe(ReadOnlySpan<byte> payload) => Observed.Add(payload.ToArray());

        public bool TryGetDictionary(uint dictionaryId, out ReadOnlyMemory<byte> dictionary)
        {
            dictionary = ReadOnlyMemory<byte>.Empty;
            return false;
        }
    }

    private static IOptionsMonitor<LatticeReplicationOptions> Monitor(LatticeReplicationOptions options)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private static LatticeMutation SetMutation(byte[] value) => new()
    {
        TreeId = "tree",
        Kind = MutationKind.Set,
        Key = "k",
        Value = value,
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
    };

    [Test]
    public async Task Set_value_is_sampled_when_auto_shared_dictionary_enabled()
    {
        var provider = new RecordingProvider();
        var observer = new ReplicationMutationObserver(
            new CapturingSink(),
            Monitor(new LatticeReplicationOptions { ClusterId = "site-a", AutoSharedDictionaryEnabled = true }),
            new AllowAllResolver(),
            provider);

        await observer.OnMutationAsync(SetMutation(new byte[] { 1, 2, 3 }), CancellationToken.None);

        Assert.That(provider.Observed, Has.Count.EqualTo(1));
        Assert.That(provider.Observed[0], Is.EqualTo(new byte[] { 1, 2, 3 }));
    }

    [Test]
    public async Task Set_value_is_not_sampled_when_auto_shared_dictionary_disabled()
    {
        var provider = new RecordingProvider();
        var observer = new ReplicationMutationObserver(
            new CapturingSink(),
            Monitor(new LatticeReplicationOptions { ClusterId = "site-a", AutoSharedDictionaryEnabled = false }),
            new AllowAllResolver(),
            provider);

        await observer.OnMutationAsync(SetMutation(new byte[] { 1, 2, 3 }), CancellationToken.None);

        Assert.That(provider.Observed, Is.Empty);
    }

    [Test]
    public async Task Delete_is_never_sampled_even_when_enabled()
    {
        var provider = new RecordingProvider();
        var observer = new ReplicationMutationObserver(
            new CapturingSink(),
            Monitor(new LatticeReplicationOptions { ClusterId = "site-a", AutoSharedDictionaryEnabled = true }),
            new AllowAllResolver(),
            provider);

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = "tree",
            Kind = MutationKind.Delete,
            Key = "k",
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        }, CancellationToken.None);

        Assert.That(provider.Observed, Is.Empty);
    }

    [Test]
    public async Task Sampling_is_inert_when_provider_is_not_a_sampler()
    {
        // The default operator-supplied provider does not implement the
        // sampler seam, so an enabled flag must not throw or sample.
        var observer = new ReplicationMutationObserver(
            new CapturingSink(),
            Monitor(new LatticeReplicationOptions { ClusterId = "site-a", AutoSharedDictionaryEnabled = true }),
            new AllowAllResolver(),
            OperatorSuppliedCompressionDictionaryProvider.Empty);

        Assert.That(
            async () => await observer.OnMutationAsync(SetMutation(new byte[] { 9 }), CancellationToken.None),
            Throws.Nothing);
    }
}
