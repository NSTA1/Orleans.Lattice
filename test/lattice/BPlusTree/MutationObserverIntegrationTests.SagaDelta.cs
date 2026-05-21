using System.Text;
using NUnit.Framework;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end integration tests asserting that <c>SetManyAtomicAsync</c>
/// captures the caller's ambient <see cref="LatticeDeltaContext"/> on
/// the first <c>Prepare</c>, persists it on the saga state, and
/// re-stamps it onto every per-key <c>SetAsync</c> / <c>DeleteAsync</c>
/// the saga issues - including compensation rolls - so observers see
/// <see cref="LatticeMutation.Delta"/> populated identically on every
/// emit.
/// </summary>
public sealed partial class MutationObserverIntegrationTests
{
    private static readonly byte[] SagaDeltaPayload = Encoding.UTF8.GetBytes("{\"op\":\"saga\"}");

    private static List<KeyValuePair<string, byte[]>> SagaEntries(params (string Key, string Value)[] pairs)
    {
        var list = new List<KeyValuePair<string, byte[]>>(pairs.Length);
        foreach (var (k, v) in pairs) list.Add(new(k, Encoding.UTF8.GetBytes(v)));
        return list;
    }

    [Test]
    public async Task SetManyAtomicAsync_emits_null_delta_when_caller_unset()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-saga-delta-null");

        await tree.SetManyAtomicAsync(SagaEntries(("k1", "A"), ("k2", "B")));

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.TreeId == "obs-e2e-saga-delta-null"
            && m.Key == "k1");

        Assert.That(m.Delta, Is.Null);
    }

    [Test]
    public async Task SetManyAtomicAsync_propagates_caller_delta_context_to_every_per_key_emit()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-saga-delta-prop");

        using (LatticeDeltaContext.With(SagaDeltaPayload))
        {
            await tree.SetManyAtomicAsync(SagaEntries(("a", "A"), ("b", "B"), ("c", "C")));
        }

        var m1 = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.TreeId == "obs-e2e-saga-delta-prop"
            && m.Key == "a");
        var m2 = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.TreeId == "obs-e2e-saga-delta-prop"
            && m.Key == "b");
        var m3 = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.TreeId == "obs-e2e-saga-delta-prop"
            && m.Key == "c");

        foreach (var m in new[] { m1, m2, m3 })
        {
            Assert.That(m.Delta, Is.EqualTo(SagaDeltaPayload));
        }

        // Every per-key emit shares the same TransactionId
        // (transaction-id ambient stamp) - pin alongside the delta to
        // catch a regression that decouples the two saga ambient stamps.
        Assert.That(m2.TransactionId, Is.EqualTo(m1.TransactionId));
        Assert.That(m3.TransactionId, Is.EqualTo(m1.TransactionId));
    }

    [Test]
    public async Task SetManyAtomicAsync_caller_delta_context_does_not_leak_across_calls()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-saga-delta-no-leak");

        using (LatticeDeltaContext.With(SagaDeltaPayload))
        {
            await tree.SetManyAtomicAsync(SagaEntries(("first", "1")));
        }
        MutationObserverClusterFixture.Drain();

        // No ambient context - saga should re-emit with null delta.
        await tree.SetManyAtomicAsync(SagaEntries(("second", "2")));

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.TreeId == "obs-e2e-saga-delta-no-leak"
            && m.Key == "second");

        Assert.That(m.Delta, Is.Null);
    }

    [Test]
    public async Task SetManyAtomicAsync_idempotent_replay_with_same_operationId_does_not_throw()
    {
        // Caller-supplied operationId enables idempotent saga re-attach.
        // First call captures the delta context and persists it on
        // AtomicWriteState. Second call with the same operationId (no
        // caller context this time) observes the prior terminal outcome
        // and must not throw - persisted Delta acts as "already captured"
        // on PrepareAsync replay.
        var tree = await _fixture.CreateTreeAsync("obs-e2e-saga-delta-replay");
        var entries = SagaEntries(("k1", "A"), ("k2", "B"));
        const string operationId = "saga-replay-op-1";

        using (LatticeDeltaContext.With(SagaDeltaPayload))
        {
            await tree.SetManyAtomicAsync(entries, operationId);
        }

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.TreeId == "obs-e2e-saga-delta-replay"
            && m.Key == "k1"
            && m.Delta is not null);
        Assert.That(m.Delta, Is.EqualTo(SagaDeltaPayload));

        Assert.DoesNotThrowAsync(async () =>
            await tree.SetManyAtomicAsync(entries, operationId));
    }
}