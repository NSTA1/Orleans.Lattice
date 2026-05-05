using System.Text;
using NUnit.Framework;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end integration tests asserting that <c>SetManyAtomicAsync</c>
/// captures the caller's ambient
/// <see cref="LatticeVectorClockContext"/> on the first <c>Prepare</c>,
/// persists it on the saga state, and re-stamps it onto every per-key
/// <c>SetAsync</c> the saga issues during Execute so observers see the
/// identical <see cref="LatticeMutation.VectorClock"/> on every emit
/// across the batch — closing the per-key VC drift a remote receiver
/// would otherwise see as a partial-set state.
/// </summary>
public sealed partial class MutationObserverIntegrationTests
{
    private static List<KeyValuePair<string, byte[]>> SagaVcEntries(params (string Key, string Value)[] pairs)
    {
        var list = new List<KeyValuePair<string, byte[]>>(pairs.Length);
        foreach (var (k, v) in pairs) list.Add(new(k, Encoding.UTF8.GetBytes(v)));
        return list;
    }

    [Test]
    public async Task SetManyAtomicAsync_emits_null_VectorClock_when_caller_unset()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-saga-vc-null");

        await tree.SetManyAtomicAsync(SagaVcEntries(("k1", "A"), ("k2", "B")));

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.TreeId == "obs-e2e-saga-vc-null"
            && m.Key == "k1");

        Assert.That(m.VectorClock, Is.Null);
    }

    [Test]
    public async Task SetManyAtomicAsync_propagates_caller_VectorClock_to_every_per_key_emit()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-saga-vc-prop");

        var vc = new VersionVector();
        vc.Tick("origin-peer");

        using (LatticeVectorClockContext.With(vc))
        {
            await tree.SetManyAtomicAsync(SagaVcEntries(("a", "A"), ("b", "B"), ("c", "C")));
        }

        var m1 = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.TreeId == "obs-e2e-saga-vc-prop"
            && m.Key == "a");
        var m2 = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.TreeId == "obs-e2e-saga-vc-prop"
            && m.Key == "b");
        var m3 = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.TreeId == "obs-e2e-saga-vc-prop"
            && m.Key == "c");

        // Every per-key emit carries a VectorClock equal to the captured
        // frontier — the core R-089 invariant.
        foreach (var m in new[] { m1, m2, m3 })
        {
            Assert.That(m.VectorClock, Is.Not.Null);
            Assert.That(m.VectorClock!.GetClock("origin-peer"),
                Is.EqualTo(vc.GetClock("origin-peer")));
        }

        // Every per-key emit shares the same TransactionId — pin
        // alongside the VC to catch a regression that decouples the
        // saga ambient stamps.
        Assert.That(m2.TransactionId, Is.EqualTo(m1.TransactionId));
        Assert.That(m3.TransactionId, Is.EqualTo(m1.TransactionId));
    }

    [Test]
    public async Task SetManyAtomicAsync_caller_VectorClock_does_not_leak_across_calls()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-saga-vc-no-leak");

        var vc = new VersionVector();
        vc.Tick("origin-peer");

        using (LatticeVectorClockContext.With(vc))
        {
            await tree.SetManyAtomicAsync(SagaVcEntries(("first", "1")));
        }
        MutationObserverClusterFixture.Drain();

        // No ambient context — saga should re-emit with null VC.
        await tree.SetManyAtomicAsync(SagaVcEntries(("second", "2")));

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.TreeId == "obs-e2e-saga-vc-no-leak"
            && m.Key == "second");

        Assert.That(m.VectorClock, Is.Null);
    }

    [Test]
    public async Task SetManyAtomicAsync_idempotent_replay_with_same_operationId_carries_persisted_VectorClock()
    {
        // Caller-supplied operationId enables idempotent saga re-attach.
        // First call captures the VC ambient and persists it on
        // AtomicWriteState[Id(11)]. Second call with the same
        // operationId (no caller context this time) observes the prior
        // terminal outcome and must not throw - the persisted
        // VectorClock acts as "already captured" on PrepareAsync replay,
        // exactly like the parallel DeltaKind / DeltaPayload capture.
        var tree = await _fixture.CreateTreeAsync("obs-e2e-saga-vc-replay");
        var entries = SagaVcEntries(("k1", "A"), ("k2", "B"));
        const string operationId = "saga-vc-replay-op-1";

        var vc = new VersionVector();
        vc.Tick("origin-peer");

        using (LatticeVectorClockContext.With(vc))
        {
            await tree.SetManyAtomicAsync(entries, operationId);
        }

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.TreeId == "obs-e2e-saga-vc-replay"
            && m.Key == "k1"
            && m.VectorClock != null);
        Assert.That(m.VectorClock!.GetClock("origin-peer"),
            Is.EqualTo(vc.GetClock("origin-peer")));

        // Re-submit with same operationId, no ambient context - must not throw.
        Assert.DoesNotThrowAsync(async () =>
            await tree.SetManyAtomicAsync(entries, operationId));
    }
}
