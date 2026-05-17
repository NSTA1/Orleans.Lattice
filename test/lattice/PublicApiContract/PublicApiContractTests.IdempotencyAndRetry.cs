using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

public partial class PublicApiContractTests
{
    // ── LatticeIdempotencyKey: value-type surface ───────────────────────

    [Test]
    public void LatticeIdempotencyKey_Fresh_mints_a_non_default_HLC()
    {
        var key = LatticeIdempotencyKey.Fresh();
        Assert.That(key.Timestamp, Is.Not.EqualTo(default(HybridLogicalClock)));
    }

    [Test]
    public void LatticeIdempotencyKey_Fresh_calls_produce_distinct_keys()
    {
        var a = LatticeIdempotencyKey.Fresh();
        var b = LatticeIdempotencyKey.Fresh();
        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void LatticeIdempotencyKey_equality_compares_Timestamp()
    {
        var hlc = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var a = new LatticeIdempotencyKey { Timestamp = hlc };
        var b = new LatticeIdempotencyKey { Timestamp = hlc };
        Assert.That(a, Is.EqualTo(b));
        Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
    }

    // ── LatticeIdempotencyContext: ambient scope surface ────────────────

    [Test]
    public void LatticeIdempotencyContext_Current_defaults_to_null()
    {
        Assert.That(LatticeIdempotencyContext.Current, Is.Null);
        Assert.That(LatticeIdempotencyContext.IsActive, Is.False);
    }

    [Test]
    public void LatticeIdempotencyContext_With_sets_and_restores_Current()
    {
        var key = LatticeIdempotencyKey.Fresh();
        using (LatticeIdempotencyContext.With(key))
        {
            Assert.That(LatticeIdempotencyContext.Current, Is.EqualTo(key));
            Assert.That(LatticeIdempotencyContext.IsActive, Is.True);
        }
        Assert.That(LatticeIdempotencyContext.Current, Is.Null);
        Assert.That(LatticeIdempotencyContext.IsActive, Is.False);
    }

    [Test]
    public void LatticeIdempotencyContext_With_null_clears_outer_scope()
    {
        var outer = LatticeIdempotencyKey.Fresh();
        using (LatticeIdempotencyContext.With(outer))
        {
            using (LatticeIdempotencyContext.With(null))
            {
                Assert.That(LatticeIdempotencyContext.Current, Is.Null);
                Assert.That(LatticeIdempotencyContext.IsActive, Is.False);
            }
            Assert.That(LatticeIdempotencyContext.Current, Is.EqualTo(outer));
        }
    }

    [Test]
    public void LatticeIdempotencyContext_NewScope_opens_with_a_fresh_key()
    {
        Assert.That(LatticeIdempotencyContext.IsActive, Is.False);
        using (LatticeIdempotencyContext.NewScope())
        {
            Assert.That(LatticeIdempotencyContext.IsActive, Is.True);
            Assert.That(LatticeIdempotencyContext.Current, Is.Not.Null);
            Assert.That(
                LatticeIdempotencyContext.Current!.Value.Timestamp,
                Is.Not.EqualTo(default(HybridLogicalClock)));
        }
        Assert.That(LatticeIdempotencyContext.IsActive, Is.False);
    }

    // ── End-to-end: ambient key stamps the LwwValue HLC ─────────────────

    [Test]
    public async Task LatticeIdempotencyContext_With_pins_the_stored_HLC_to_the_key()
    {
        var treeId = "pac-idemp-stamp-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        var key = LatticeIdempotencyKey.Fresh();

        VersionedValue first, second;
        using (LatticeIdempotencyContext.With(key))
        {
            await tree.SetAsync("k", Bytes("v1"));
            first = await tree.GetWithVersionAsync("k");

            await tree.SetAsync("k", Bytes("v2"));
            second = await tree.GetWithVersionAsync("k");
        }

        Assert.That(first.Version, Is.EqualTo(key.Timestamp),
            "First write under the key must stamp the key's HLC verbatim.");
        Assert.That(second.Version, Is.EqualTo(key.Timestamp),
            "Second write under the same key must re-stamp the same HLC (LWW tie, no advance).");
    }

    [Test]
    public async Task Mutations_without_an_ambient_key_advance_the_stored_HLC()
    {
        var treeId = "pac-idemp-noscope-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        await tree.SetAsync("k", Bytes("v1"));
        var first = await tree.GetWithVersionAsync("k");

        await tree.SetAsync("k", Bytes("v2"));
        var second = await tree.GetWithVersionAsync("k");

        Assert.That(second.Version, Is.Not.EqualTo(first.Version),
            "Without an ambient key the leaf must mint a fresh HLC per write.");
    }

    [Test]
    public async Task LatticeIdempotencyContext_does_not_stamp_OriginClusterId()
    {
        // Contract: the idempotency key never carries origin.
        // Authoring cluster id flows exclusively through
        // LatticeOriginContext / ILatticeOriginClusterIdResolver. Asserting
        // here pins that contract so a future regression that re-adds an
        // origin field on the key would surface as an observable stamp.
        var treeId = "pac-idemp-origin-isolated-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        PublicApiContractClusterFixture.DrainObserverEvents();

        using (LatticeIdempotencyContext.NewScope())
        {
            await tree.SetAsync("k", Bytes("v"));
        }

        var captured = await CaptureMutationsForTreeAsync(treeId, expectedMin: 1);
        var setEvent = captured.First(m => m.Kind == MutationKind.Set);
        Assert.That(setEvent.OriginClusterId, Is.Null,
            "An ambient idempotency scope with no LatticeOriginContext must not stamp origin.");
    }

    [Test]
    public async Task LatticeIdempotencyContext_composes_with_LatticeOriginContext()
    {
        var treeId = "pac-idemp-origin-compose-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        PublicApiContractClusterFixture.DrainObserverEvents();

        using (LatticeOriginContext.With("remote-cluster-X"))
        using (LatticeIdempotencyContext.NewScope())
        {
            await tree.SetAsync("k", Bytes("v"));
        }

        var captured = await CaptureMutationsForTreeAsync(treeId, expectedMin: 1);
        var setEvent = captured.First(m => m.Kind == MutationKind.Set);
        Assert.That(setEvent.OriginClusterId, Is.EqualTo("remote-cluster-X"),
            "Origin must come from LatticeOriginContext even when an idempotency scope is open.");
    }

    // ── BoundedExponentialRetryPolicy: construction surface ─────────────

    [Test]
    public void BoundedExponentialRetryPolicy_default_constructor_uses_shipped_defaults()
    {
        // Construction must not throw with the shipped defaults so a host
        // can wire `new BoundedExponentialRetryPolicy()` with no surprise.
        Assert.That(() => new BoundedExponentialRetryPolicy(), Throws.Nothing);
    }

    [Test]
    public void BoundedExponentialRetryPolicy_rejects_zero_maxAttempts()
    {
        Assert.That(
            () => new BoundedExponentialRetryPolicy(maxAttempts: 0),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void BoundedExponentialRetryPolicy_rejects_negative_initialDelay()
    {
        Assert.That(
            () => new BoundedExponentialRetryPolicy(
                maxAttempts: 1,
                initialDelay: TimeSpan.FromMilliseconds(-1)),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void BoundedExponentialRetryPolicy_rejects_maxDelay_below_initialDelay()
    {
        Assert.That(
            () => new BoundedExponentialRetryPolicy(
                maxAttempts: 1,
                initialDelay: TimeSpan.FromMilliseconds(100),
                maxDelay: TimeSpan.FromMilliseconds(50)),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void BoundedExponentialRetryPolicy_options_constructor_throws_on_null_options()
    {
        Assert.That(
            () => new BoundedExponentialRetryPolicy(options: null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void BoundedExponentialRetryPolicyOptions_defaults_are_shipped_values()
    {
        var options = new BoundedExponentialRetryPolicyOptions();
        Assert.That(options.MaxAttempts, Is.EqualTo(4));
        Assert.That(options.InitialDelay, Is.EqualTo(TimeSpan.FromMilliseconds(50)));
        Assert.That(options.MaxDelay, Is.EqualTo(TimeSpan.FromSeconds(2)));
        Assert.That(options.RetryableExceptionClassifier, Is.Null);
    }

    // ── ILatticeRetryPolicy: behavioural contract ───────────────────────

    [Test]
    public async Task ILatticeRetryPolicy_ExecuteAsync_untyped_runs_the_operation_once_on_success()
    {
        ILatticeRetryPolicy policy = new BoundedExponentialRetryPolicy(
            maxAttempts: 3,
            initialDelay: TimeSpan.Zero,
            maxDelay: TimeSpan.Zero);

        var calls = 0;
        await policy.ExecuteAsync(_ => { calls++; return Task.CompletedTask; }, default);
        Assert.That(calls, Is.EqualTo(1));
    }

    [Test]
    public async Task ILatticeRetryPolicy_ExecuteAsync_typed_returns_the_operation_result()
    {
        ILatticeRetryPolicy policy = new BoundedExponentialRetryPolicy(
            maxAttempts: 3,
            initialDelay: TimeSpan.Zero,
            maxDelay: TimeSpan.Zero);

        var result = await policy.ExecuteAsync(_ => Task.FromResult(42), default);
        Assert.That(result, Is.EqualTo(42));
    }

    [Test]
    public async Task ILatticeRetryPolicy_ExecuteAsync_retries_transient_failures_to_the_budget()
    {
        ILatticeRetryPolicy policy = new BoundedExponentialRetryPolicy(
            maxAttempts: 3,
            initialDelay: TimeSpan.Zero,
            maxDelay: TimeSpan.Zero);

        var calls = 0;
        await policy.ExecuteAsync(_ =>
        {
            calls++;
            if (calls < 3)
            {
                throw new InvalidOperationException("simulated transient");
            }
            return Task.CompletedTask;
        }, default);

        Assert.That(calls, Is.EqualTo(3));
    }

    [Test]
    public void ILatticeRetryPolicy_ExecuteAsync_rethrows_original_failure_on_budget_exhaustion()
    {
        ILatticeRetryPolicy policy = new BoundedExponentialRetryPolicy(
            maxAttempts: 2,
            initialDelay: TimeSpan.Zero,
            maxDelay: TimeSpan.Zero);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await policy.ExecuteAsync(_ => throw new InvalidOperationException("always-bad"), default));

        Assert.That(ex!.Message, Is.EqualTo("always-bad"));
    }

    [Test]
    public void ILatticeRetryPolicy_ExecuteAsync_does_not_retry_when_classifier_rejects_exception()
    {
        ILatticeRetryPolicy policy = new BoundedExponentialRetryPolicy(
            maxAttempts: 10,
            initialDelay: TimeSpan.Zero,
            maxDelay: TimeSpan.Zero,
            retryableExceptionClassifier: _ => false);

        var calls = 0;
        Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await policy.ExecuteAsync(_ =>
            {
                calls++;
                throw new InvalidOperationException("not-retryable");
            }, default));

        Assert.That(calls, Is.EqualTo(1), "Classifier rejection must short-circuit the retry loop.");
    }

    [Test]
    public void ILatticeRetryPolicy_ExecuteAsync_untyped_throws_on_null_operation()
    {
        ILatticeRetryPolicy policy = new BoundedExponentialRetryPolicy();
        Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await policy.ExecuteAsync((Func<CancellationToken, Task>)null!, default));
    }

    [Test]
    public void ILatticeRetryPolicy_ExecuteAsync_typed_throws_on_null_operation()
    {
        ILatticeRetryPolicy policy = new BoundedExponentialRetryPolicy();
        Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await policy.ExecuteAsync((Func<CancellationToken, Task<int>>)null!, default));
    }

    [Test]
    public async Task ILatticeRetryPolicy_honours_caller_cancellation_between_attempts()
    {
        ILatticeRetryPolicy policy = new BoundedExponentialRetryPolicy(
            maxAttempts: 5,
            initialDelay: TimeSpan.FromMilliseconds(10),
            maxDelay: TimeSpan.FromMilliseconds(10));

        using var cts = new CancellationTokenSource();
        var calls = 0;

        var run = policy.ExecuteAsync(_ =>
        {
            calls++;
            cts.Cancel();
            throw new InvalidOperationException("transient");
        }, cts.Token);

        Assert.That(async () => await run,
            Throws.InstanceOf<OperationCanceledException>().Or.InstanceOf<TaskCanceledException>());
        await Task.Yield();
        Assert.That(calls, Is.EqualTo(1),
            "After the caller cancels, the policy must not start another attempt.");
    }

    // ── LatticeOptions.RetryPolicy slot ─────────────────────────────────

    [Test]
    public void LatticeOptions_RetryPolicy_defaults_to_null()
    {
        var options = new LatticeOptions();
        Assert.That(options.RetryPolicy, Is.Null,
            "The library must not install a retry policy by default - the slot is strictly opt-in.");
    }

    [Test]
    public void LatticeOptions_RetryPolicy_is_assignable()
    {
        var policy = new BoundedExponentialRetryPolicy(
            maxAttempts: 2,
            initialDelay: TimeSpan.Zero,
            maxDelay: TimeSpan.Zero);
        var options = new LatticeOptions { RetryPolicy = policy };
        Assert.That(options.RetryPolicy, Is.SameAs(policy));
    }

    // ── DI extension: AddLatticeRetryPolicy ─────────────────────────────

    [Test]
    public void AddLatticeRetryPolicy_throws_on_null_builder()
    {
        Assert.That(
            () => LatticeServiceCollectionExtensions.AddLatticeRetryPolicy(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void AddLatticeRetryPolicy_with_no_configure_uses_shipped_defaults_on_resolved_options()
    {
        var monitor = ResolveSiloOptionsMonitor();
        var opts = monitor.Get(Options.DefaultName);
        // The PublicApiContractClusterFixture does not wire AddLatticeRetryPolicy,
        // so on the contract fixture the slot stays null. This pins the
        // "no ambient cost" invariant from the API surface side.
        Assert.That(opts.RetryPolicy, Is.Null);
    }

    [Test]
    public async Task End_to_end_RetryPolicy_under_idempotency_scope_collapses_a_retried_Set()
    {
        // Caller-side retry + ambient idempotency scope = single observable
        // mutation. This is the headline retry-collapse acceptance: pin it
        // inside the [Category("API")] suite so any regression that breaks
        // the boundary helper surfaces here, not only in the integration
        // tests.
        var treeId = "pac-idemp-retry-collapse-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var policy = new BoundedExponentialRetryPolicy(
            maxAttempts: 3,
            initialDelay: TimeSpan.Zero,
            maxDelay: TimeSpan.Zero);
        var key = LatticeIdempotencyKey.Fresh();
        var attempts = 0;

        using (LatticeIdempotencyContext.With(key))
        {
            await policy.ExecuteAsync(async _ =>
            {
                attempts++;
                await tree.SetAsync("k", Bytes($"v-{attempts}"));
                if (attempts < 2)
                {
                    throw new InvalidOperationException("simulated post-commit transient");
                }
            }, default);
        }

        Assert.That(attempts, Is.EqualTo(2), "Policy must have retried exactly once.");
        var stored = await tree.GetWithVersionAsync("k");
        Assert.That(stored.Version, Is.EqualTo(key.Timestamp),
            "Both attempts must stamp the key's HLC verbatim, so the stored version equals the key.");
    }

    private static IOptionsMonitor<LatticeOptions> ResolveSiloOptionsMonitor()
    {
        var silo = PublicApiContractClusterFixture.SiloServices
            ?? throw new InvalidOperationException("Silo services not yet captured.");
        return silo.GetRequiredService<IOptionsMonitor<LatticeOptions>>();
    }
}
