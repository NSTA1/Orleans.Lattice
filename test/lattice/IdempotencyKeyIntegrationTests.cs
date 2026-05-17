using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests;

/// <summary>
/// End-to-end integration tests for the
/// <see cref="LatticeIdempotencyContext"/> + <see cref="LwwValue{T}"/>
/// stamping pipeline. Verifies that two foreground writes issued under
/// the same idempotency key produce a single observable mutation
/// (identical stored HLC), while two writes without a key produce
/// distinct HLCs.
/// </summary>
[TestFixture]
[Category("Integration")]
public class IdempotencyKeyIntegrationTests
{
    private ClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [SetUp]
    public void ResetAmbient()
    {
        LatticeIdempotencyContext.Current = null;
    }

    private ILattice NewTree() =>
        _cluster.GrainFactory.GetGrain<ILattice>($"idemp-{Guid.NewGuid():N}");

    [Test]
    public async Task SetAsync_under_same_idempotency_key_stamps_identical_HLC()
    {
        var tree = NewTree();
        var key = LatticeIdempotencyKey.Fresh();

        VersionedValue first, second;
        using (LatticeIdempotencyContext.With(key))
        {
            await tree.SetAsync("k", Encoding.UTF8.GetBytes("v1"));
            first = await tree.GetWithVersionAsync("k");

            await tree.SetAsync("k", Encoding.UTF8.GetBytes("v2"));
            second = await tree.GetWithVersionAsync("k");
        }

        Assert.That(first.Version, Is.EqualTo(key.Timestamp),
            "First write under the key must stamp the key's HLC verbatim.");
        Assert.That(second.Version, Is.EqualTo(key.Timestamp),
            "Second write under the same key must re-stamp the same HLC (LWW tie => no advance).");
    }

    [Test]
    public async Task SetAsync_without_idempotency_key_advances_HLC_on_each_write()
    {
        var tree = NewTree();
        await tree.SetAsync("k", Encoding.UTF8.GetBytes("v1"));
        var first = await tree.GetWithVersionAsync("k");

        await tree.SetAsync("k", Encoding.UTF8.GetBytes("v2"));
        var second = await tree.GetWithVersionAsync("k");

        Assert.That(second.Version, Is.Not.EqualTo(first.Version),
            "Without an ambient key, the leaf must mint a fresh HLC per write.");
    }

    [Test]
    public async Task PnCounter_IncrementAsync_with_same_idempotency_key_advances_once()
    {
        var tree = NewTree();
        var counter = tree.PnCounter("pn");

        using (LatticeIdempotencyContext.NewScope())
        {
            await counter.IncrementAsync("r1", 5);
            await counter.IncrementAsync("r1", 5);
        }

        var value = await counter.ValueAsync();
        Assert.That(value, Is.EqualTo(5),
            "Two increments under the same idempotency key must collapse to one observable advance.");
    }

    [Test]
    public async Task PnCounter_IncrementAsync_with_distinct_idempotency_keys_advances_twice()
    {
        var tree = NewTree();
        var counter = tree.PnCounter("pn");

        using (LatticeIdempotencyContext.NewScope())
        {
            await counter.IncrementAsync("r1", 5);
        }

        using (LatticeIdempotencyContext.NewScope())
        {
            await counter.IncrementAsync("r1", 5);
        }

        var value = await counter.ValueAsync();
        Assert.That(value, Is.EqualTo(10),
            "Distinct keys must NOT trigger dedup - the counter advances per increment.");
    }

    [Test]
    public async Task Caller_side_BoundedExponentialRetryPolicy_under_same_key_collapses_to_single_mutation()
    {
        var tree = NewTree();
        var key = LatticeIdempotencyKey.Fresh();
        var policy = new BoundedExponentialRetryPolicy(
            maxAttempts: 3,
            initialDelay: TimeSpan.Zero,
            maxDelay: TimeSpan.Zero);

        // Caller-side retry pattern: ambient idempotency scope + explicit
        // policy invocation. The first attempt's "transient failure" is
        // simulated by throwing after the lattice write has already
        // succeeded - the retry then issues a second write under the
        // same key, which the LWW tie collapses to a no-op so the
        // observable state is identical to a single successful write.
        var attempts = 0;
        using (LatticeIdempotencyContext.With(key))
        {
            await policy.ExecuteAsync(async _ =>
            {
                attempts++;
                await tree.SetAsync("k", Encoding.UTF8.GetBytes($"v-{attempts}"));
                if (attempts < 2)
                    throw new InvalidOperationException("simulated post-commit transient");
            }, default);
        }

        Assert.That(attempts, Is.EqualTo(2), "Policy must have retried exactly once.");
        var stored = await tree.GetWithVersionAsync("k");
        Assert.That(stored.Version, Is.EqualTo(key.Timestamp),
            "Both attempts must stamp the key's HLC verbatim, so the stored version equals the key.");
    }

    [Test]
    public async Task Default_RetryPolicy_is_null_so_no_ambient_cost_is_introduced()
    {
        // Negative-control: confirm that the LatticeOptions on this
        // silo have no retry policy registered by default, satisfying
        // the "zero ambient cost" acceptance criterion.
        var monitor = _cluster.Silos
            .OfType<InProcessSiloHandle>()
            .First()
            .SiloHost
            .Services
            .GetRequiredService<Microsoft.Extensions.Options.IOptionsMonitor<LatticeOptions>>();
        var opts = monitor.Get(Microsoft.Extensions.Options.Options.DefaultName);
        Assert.That(opts.RetryPolicy, Is.Null,
            "Default LatticeOptions.RetryPolicy must be null - the policy is strictly opt-in.");
    }
}
