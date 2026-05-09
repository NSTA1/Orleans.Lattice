using Microsoft.Extensions.Logging.Abstractions;
using MultiSiteManufacturing.Host.Federation;
using MultiSiteManufacturing.Host.Replication;
using MultiSiteManufacturing.Tests.Federation;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace MultiSiteManufacturing.Tests.Replication;

/// <summary>
/// Unit tests for <see cref="ChaosReplicationApplier"/>: the
/// inbound-side counterpart to <see cref="ChaosReplicationTransport"/>
/// that gates <see cref="IReplicationApplier.ApplyAsync"/> and
/// <see cref="IReplicationApplier.ApplyBatchAsync"/> on the operator-driven
/// <see cref="IReplicationDisconnectGrain"/> chaos flag.
/// </summary>
[TestFixture]
public sealed class ChaosReplicationApplierTests
{
    private FederationTestClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FederationTestClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [SetUp]
    public async Task ClearDisconnectFlag()
    {
        // The disconnect grain is a singleton across the test cluster,
        // so per-test cleanup avoids cross-test bleed when one test
        // leaves the flag set and the next assumes it is cleared.
        await _fixture.GrainFactory
            .GetGrain<IReplicationDisconnectGrain>(IReplicationDisconnectGrain.SingletonKey)
            .SetDisconnectedAsync(false);
    }

    /// <summary>
    /// Hand-rolled <see cref="IReplicationApplier"/> stub that records
    /// every call so the decorator's gate behaviour can be asserted
    /// without standing up the full apply pipeline. The repo doesn't
    /// reference NSubstitute / Moq from the sample test project, so
    /// the stub is done explicitly.
    /// </summary>
    private sealed class StubInnerApplier(ApplyResult result) : IReplicationApplier
    {
        public int ApplyCalls { get; private set; }
        public int BatchCalls { get; private set; }

        public Task<ApplyResult> ApplyAsync(WalRecord entry, CancellationToken cancellationToken = default)
        {
            ApplyCalls++;
            return Task.FromResult(result);
        }

        public Task<ApplyResult> ApplyBatchAsync(
            IReadOnlyList<WalRecord> entries,
            CancellationToken cancellationToken = default)
        {
            BatchCalls++;
            return Task.FromResult(result);
        }
    }

    private (ChaosReplicationApplier Decorator, StubInnerApplier Inner) Build()
    {
        var inner = new StubInnerApplier(new ApplyResult { Applied = true });
        var decorator = new ChaosReplicationApplier(
            inner,
            _fixture.GrainFactory,
            NullLogger<ChaosReplicationApplier>.Instance);
        return (decorator, inner);
    }

    private static WalRecord NewEntry() => new()
    {
        TreeId = "mfg-facts",
        Key = "HPT-PART-2028-99999/forge",
        Op = MutationKind.Set,
        Timestamp = HybridLogicalClock.Zero,
        Value = [0x01, 0x02, 0x03],
        OriginClusterId = "peer",
    };

    private async Task SetDisconnectAsync(bool disconnected) =>
        await _fixture.GrainFactory
            .GetGrain<IReplicationDisconnectGrain>(IReplicationDisconnectGrain.SingletonKey)
            .SetDisconnectedAsync(disconnected);

    // ---------------------------------------------------------------
    // ApplyAsync gate
    // ---------------------------------------------------------------

    [Test]
    public async Task ApplyAsync_when_flag_clear_delegates_to_inner()
    {
        var (decorator, inner) = Build();

        var result = await decorator.ApplyAsync(NewEntry());

        Assert.That(inner.ApplyCalls, Is.EqualTo(1));
        Assert.That(result.Applied, Is.True);
    }

    [Test]
    public async Task ApplyAsync_when_flag_set_throws_and_does_not_call_inner()
    {
        var (decorator, inner) = Build();
        await SetDisconnectAsync(true);

        Assert.That(
            async () => await decorator.ApplyAsync(NewEntry()),
            Throws.InstanceOf<InvalidOperationException>()
                .With.Message.Contain("Chaos replication-disconnect"));

        Assert.That(inner.ApplyCalls, Is.Zero);
    }

    // ---------------------------------------------------------------
    // ApplyBatchAsync gate
    // ---------------------------------------------------------------

    [Test]
    public async Task ApplyBatchAsync_when_flag_clear_delegates_to_inner()
    {
        var (decorator, inner) = Build();

        var entries = new[] { NewEntry(), NewEntry() };
        var result = await decorator.ApplyBatchAsync(entries);

        Assert.That(inner.BatchCalls, Is.EqualTo(1));
        Assert.That(result.Applied, Is.True);
    }

    [Test]
    public async Task ApplyBatchAsync_when_flag_set_throws_and_does_not_call_inner()
    {
        var (decorator, inner) = Build();
        await SetDisconnectAsync(true);

        var entries = new[] { NewEntry(), NewEntry(), NewEntry() };

        Assert.That(
            async () => await decorator.ApplyBatchAsync(entries),
            Throws.InstanceOf<InvalidOperationException>()
                .With.Message.Contain("Chaos replication-disconnect"));

        Assert.That(inner.BatchCalls, Is.Zero);
    }

    [Test]
    public void ApplyBatchAsync_with_null_entries_throws_argument_null()
    {
        var (decorator, _) = Build();

        Assert.That(
            async () => await decorator.ApplyBatchAsync(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    // ---------------------------------------------------------------
    // Toggle behaviour: gate respects the live flag value, not a
    // value cached at construction time.
    // ---------------------------------------------------------------

    [Test]
    public async Task ApplyBatchAsync_resumes_delegating_after_flag_is_cleared()
    {
        var (decorator, inner) = Build();
        var entries = new[] { NewEntry() };

        // 1. Flag set: decorator throws, inner not called.
        await SetDisconnectAsync(true);
        Assert.That(
            async () => await decorator.ApplyBatchAsync(entries),
            Throws.InstanceOf<InvalidOperationException>());
        Assert.That(inner.BatchCalls, Is.Zero);

        // 2. Flag cleared: decorator delegates to inner again.
        await SetDisconnectAsync(false);
        var result = await decorator.ApplyBatchAsync(entries);
        Assert.That(result.Applied, Is.True);
        Assert.That(inner.BatchCalls, Is.EqualTo(1));
    }
}
