namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeWriteInterceptorEnforcement"/>: the shared
/// primitive that short-circuits the null interceptor and the system-origin
/// bypass, consults a registered interceptor, and translates its
/// <see cref="LatticeWriteDecision"/> into the effect the choke point applies
/// (proceed / transform / drop / throw), including the atomic-batch abort.
/// </summary>
[TestFixture]
public class LatticeWriteInterceptorEnforcementTests
{
    private const string Tree = "orders";

    /// <summary>
    /// A configurable interceptor test double: returns a caller-supplied decision
    /// per request, counts calls, and can opt into system-origin ingest.
    /// </summary>
    private sealed class StubWriteInterceptor(
        Func<LatticeWriteRequest, LatticeWriteDecision> decide,
        bool interceptsSystemOrigin = false) : ILatticeWriteInterceptor
    {
        public int Calls { get; private set; }

        public bool InterceptsSystemOrigin => interceptsSystemOrigin;

        public ValueTask<LatticeWriteDecision> OnWriteAsync(
            in LatticeWriteRequest request, CancellationToken cancellationToken = default)
        {
            Calls++;
            return new ValueTask<LatticeWriteDecision>(decide(request));
        }
    }

    private static List<KeyValuePair<string, byte[]>> Entries(params (string Key, byte[] Value)[] items)
    {
        var list = new List<KeyValuePair<string, byte[]>>(items.Length);
        foreach (var (key, value) in items)
        {
            list.Add(new KeyValuePair<string, byte[]>(key, value));
        }

        return list;
    }

    // ---- Skips -----------------------------------------------------------

    [Test]
    public void Skips_is_true_for_the_null_interceptor()
    {
        Assert.That(LatticeWriteInterceptorEnforcement.Skips(new NullLatticeWriteInterceptor()), Is.True);
    }

    [Test]
    public void Skips_is_false_for_a_real_interceptor_on_a_user_turn()
    {
        var interceptor = new StubWriteInterceptor(_ => LatticeWriteDecision.Accept());

        Assert.That(LatticeWriteInterceptorEnforcement.Skips(interceptor), Is.False);
    }

    [Test]
    public void Skips_is_true_on_a_system_origin_turn_for_a_non_opting_interceptor()
    {
        var interceptor = new StubWriteInterceptor(_ => LatticeWriteDecision.Accept());

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            Assert.That(LatticeWriteInterceptorEnforcement.Skips(interceptor), Is.True);
        }
    }

    [Test]
    public void Skips_is_false_on_a_system_origin_turn_for_an_opting_interceptor()
    {
        var interceptor = new StubWriteInterceptor(_ => LatticeWriteDecision.Accept(), interceptsSystemOrigin: true);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            Assert.That(LatticeWriteInterceptorEnforcement.Skips(interceptor), Is.False);
        }
    }

    // ---- InterceptPointAsync ---------------------------------------------

    [Test]
    public async Task InterceptPoint_null_interceptor_proceeds_with_original_value_without_calling()
    {
        var interceptor = new NullLatticeWriteInterceptor();
        var value = new byte[] { 1, 2, 3 };

        var outcome = await LatticeWriteInterceptorEnforcement.InterceptPointAsync(
            interceptor, Tree, LatticeOperation.Write, "k1", value, ttl: null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Proceed, Is.True);
            Assert.That(outcome.Value, Is.SameAs(value));
        });
    }

    [Test]
    public async Task InterceptPoint_accept_proceeds_with_the_original_value()
    {
        var interceptor = new StubWriteInterceptor(_ => LatticeWriteDecision.Accept());
        var value = new byte[] { 1, 2, 3 };

        var outcome = await LatticeWriteInterceptorEnforcement.InterceptPointAsync(
            interceptor, Tree, LatticeOperation.Write, "k1", value, ttl: null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(interceptor.Calls, Is.EqualTo(1));
            Assert.That(outcome.Proceed, Is.True);
            Assert.That(outcome.Value, Is.SameAs(value));
        });
    }

    [Test]
    public async Task InterceptPoint_accept_transformed_proceeds_with_the_replacement_value()
    {
        var replacement = new byte[] { 9, 9 };
        var interceptor = new StubWriteInterceptor(_ => LatticeWriteDecision.AcceptTransformed(replacement));

        var outcome = await LatticeWriteInterceptorEnforcement.InterceptPointAsync(
            interceptor, Tree, LatticeOperation.Write, "k1", [1, 2, 3], ttl: null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Proceed, Is.True);
            Assert.That(outcome.Value, Is.SameAs(replacement));
        });
    }

    [Test]
    public async Task InterceptPoint_dead_letter_does_not_proceed()
    {
        var interceptor = new StubWriteInterceptor(_ => LatticeWriteDecision.DeadLetter("quarantined"));

        var outcome = await LatticeWriteInterceptorEnforcement.InterceptPointAsync(
            interceptor, Tree, LatticeOperation.Write, "k1", [1], ttl: null, CancellationToken.None);

        Assert.That(outcome.Proceed, Is.False);
    }

    [Test]
    public void InterceptPoint_reject_throws_with_context()
    {
        var interceptor = new StubWriteInterceptor(_ => LatticeWriteDecision.Reject("schema mismatch"));

        var ex = Assert.ThrowsAsync<LatticeWriteRejectedException>(async () =>
            await LatticeWriteInterceptorEnforcement.InterceptPointAsync(
                interceptor, Tree, LatticeOperation.Write, "k1", [1], ttl: null, CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.TreeId, Is.EqualTo(Tree));
            Assert.That(ex.Key, Is.EqualTo("k1"));
            Assert.That(ex.Reason, Is.EqualTo("schema mismatch"));
        });
    }

    [Test]
    public async Task InterceptPoint_default_decision_is_treated_as_accept()
    {
        // default(LatticeWriteDecision) has Kind = Accept, so a stub that returns
        // it proceeds with the original value unchanged.
        var interceptor = new StubWriteInterceptor(_ => default);
        var value = new byte[] { 7 };

        var outcome = await LatticeWriteInterceptorEnforcement.InterceptPointAsync(
            interceptor, Tree, LatticeOperation.Write, "k1", value, ttl: null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Proceed, Is.True);
            Assert.That(outcome.Value, Is.SameAs(value));
        });
    }

    [Test]
    public async Task InterceptPoint_skipped_system_origin_proceeds_without_calling()
    {
        var interceptor = new StubWriteInterceptor(_ => LatticeWriteDecision.Reject("should not run"));
        var value = new byte[] { 1 };

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            var outcome = await LatticeWriteInterceptorEnforcement.InterceptPointAsync(
                interceptor, Tree, LatticeOperation.Write, "k1", value, ttl: null, CancellationToken.None);

            Assert.Multiple(() =>
            {
                Assert.That(interceptor.Calls, Is.EqualTo(0), "a non-opting interceptor is bypassed on a system turn");
                Assert.That(outcome.Proceed, Is.True);
                Assert.That(outcome.Value, Is.SameAs(value));
            });
        }
    }

    [Test]
    public void InterceptPoint_opting_interceptor_runs_on_system_origin_turn()
    {
        var interceptor = new StubWriteInterceptor(_ => LatticeWriteDecision.Reject("enforced"), interceptsSystemOrigin: true);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            Assert.ThrowsAsync<LatticeWriteRejectedException>(async () =>
                await LatticeWriteInterceptorEnforcement.InterceptPointAsync(
                    interceptor, Tree, LatticeOperation.Write, "k1", [1], ttl: null, CancellationToken.None));
        }
    }

    // ---- InterceptEntriesAsync -------------------------------------------

    [Test]
    public async Task InterceptEntries_null_interceptor_returns_the_same_list()
    {
        var interceptor = new NullLatticeWriteInterceptor();
        var entries = Entries(("a", [1]), ("b", [2]));

        var result = await LatticeWriteInterceptorEnforcement.InterceptEntriesAsync(
            interceptor, Tree, LatticeOperation.Write, entries, atomic: false, CancellationToken.None);

        Assert.That(result, Is.SameAs(entries));
    }

    [Test]
    public async Task InterceptEntries_all_accept_returns_the_same_list_reference()
    {
        var interceptor = new StubWriteInterceptor(_ => LatticeWriteDecision.Accept());
        var entries = Entries(("a", [1]), ("b", [2]));

        var result = await LatticeWriteInterceptorEnforcement.InterceptEntriesAsync(
            interceptor, Tree, LatticeOperation.Write, entries, atomic: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.SameAs(entries));
            Assert.That(interceptor.Calls, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task InterceptEntries_transform_replaces_only_the_targeted_value()
    {
        var replacement = new byte[] { 42 };
        var interceptor = new StubWriteInterceptor(request =>
            request.Key == "b" ? LatticeWriteDecision.AcceptTransformed(replacement) : LatticeWriteDecision.Accept());
        var entries = Entries(("a", [1]), ("b", [2]), ("c", [3]));

        var result = await LatticeWriteInterceptorEnforcement.InterceptEntriesAsync(
            interceptor, Tree, LatticeOperation.Write, entries, atomic: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.Not.SameAs(entries));
            Assert.That(result, Has.Count.EqualTo(3));
            Assert.That(result[0].Key, Is.EqualTo("a"));
            Assert.That(result[1].Value, Is.SameAs(replacement));
            Assert.That(result[2].Key, Is.EqualTo("c"));
        });
    }

    [Test]
    public async Task InterceptEntries_non_atomic_dead_letter_drops_the_entry()
    {
        var interceptor = new StubWriteInterceptor(request =>
            request.Key == "b" ? LatticeWriteDecision.DeadLetter("drop b") : LatticeWriteDecision.Accept());
        var entries = Entries(("a", [1]), ("b", [2]), ("c", [3]));

        var result = await LatticeWriteInterceptorEnforcement.InterceptEntriesAsync(
            interceptor, Tree, LatticeOperation.Write, entries, atomic: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result, Has.Count.EqualTo(2));
            Assert.That(result.Select(e => e.Key), Is.EqualTo(new[] { "a", "c" }));
        });
    }

    [Test]
    public void InterceptEntries_atomic_dead_letter_aborts_the_batch()
    {
        var interceptor = new StubWriteInterceptor(request =>
            request.Key == "b" ? LatticeWriteDecision.DeadLetter("drop b") : LatticeWriteDecision.Accept());
        var entries = Entries(("a", [1]), ("b", [2]), ("c", [3]));

        var ex = Assert.ThrowsAsync<LatticeWriteRejectedException>(async () =>
            await LatticeWriteInterceptorEnforcement.InterceptEntriesAsync(
                interceptor, Tree, LatticeOperation.Write, entries, atomic: true, CancellationToken.None));

        Assert.That(ex!.Key, Is.EqualTo("b"));
    }

    [Test]
    public void InterceptEntries_reject_aborts_the_batch_on_both_modes()
    {
        var interceptor = new StubWriteInterceptor(request =>
            request.Key == "b" ? LatticeWriteDecision.Reject("bad b") : LatticeWriteDecision.Accept());
        var entries = Entries(("a", [1]), ("b", [2]));

        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<LatticeWriteRejectedException>(async () =>
                await LatticeWriteInterceptorEnforcement.InterceptEntriesAsync(
                    interceptor, Tree, LatticeOperation.Write, entries, atomic: false, CancellationToken.None));
            Assert.ThrowsAsync<LatticeWriteRejectedException>(async () =>
                await LatticeWriteInterceptorEnforcement.InterceptEntriesAsync(
                    interceptor, Tree, LatticeOperation.Write, entries, atomic: true, CancellationToken.None));
        });
    }

    [Test]
    public async Task InterceptEntries_empty_list_returns_the_same_list_without_calling()
    {
        var interceptor = new StubWriteInterceptor(_ => LatticeWriteDecision.Reject("should not run"));
        var entries = Entries();

        var result = await LatticeWriteInterceptorEnforcement.InterceptEntriesAsync(
            interceptor, Tree, LatticeOperation.Write, entries, atomic: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.SameAs(entries));
            Assert.That(interceptor.Calls, Is.EqualTo(0));
        });
    }
}
