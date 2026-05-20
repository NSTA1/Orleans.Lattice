using System.Text;
using NUnit.Framework;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end integration tests asserting that <c>SetManyAtomicAsync</c>
/// stamps <see cref="LatticeMutation.AtomicBatchSize"/> /
/// <see cref="LatticeMutation.AtomicBatchIndex"/> on every per-key emit
/// produced by the saga. The size is the canonical sibling
/// count a remote receiver-side staging buffer reads to detect when
/// every entry of a batch has arrived; the index is the deterministic
/// per-key position within the batch. Single-key writes outside a
/// saga must continue to emit <c>0</c> / <c>0</c>.
/// </summary>
public sealed partial class MutationObserverIntegrationTests
{
    private static List<KeyValuePair<string, byte[]>> AtomicBatchEntries(params (string Key, string Value)[] pairs)
    {
        var list = new List<KeyValuePair<string, byte[]>>(pairs.Length);
        foreach (var (k, v) in pairs) list.Add(new(k, Encoding.UTF8.GetBytes(v)));
        return list;
    }

    [Test]
    public async Task SetManyAtomicAsync_stamps_AtomicBatchSize_equal_to_entry_count_on_every_per_key_emit()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-atomic-batch-size");

        await tree.SetManyAtomicAsync(AtomicBatchEntries(
            ("k1", "A"), ("k2", "B"), ("k3", "C"), ("k4", "D"), ("k5", "E")));

        var m1 = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.TreeId == "obs-e2e-atomic-batch-size"
            && m.Key == "k1");
        var m2 = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.TreeId == "obs-e2e-atomic-batch-size"
            && m.Key == "k2");
        var m3 = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.TreeId == "obs-e2e-atomic-batch-size"
            && m.Key == "k3");
        var m4 = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.TreeId == "obs-e2e-atomic-batch-size"
            && m.Key == "k4");
        var m5 = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.TreeId == "obs-e2e-atomic-batch-size"
            && m.Key == "k5");

        // Every per-key emit carries AtomicBatchSize equal to the
        // submitted entry count.
        foreach (var m in new[] { m1, m2, m3, m4, m5 })
        {
            Assert.That(m.AtomicBatchSize, Is.EqualTo(5));
        }

        // Every per-key emit shares the same TransactionId - pin
        // alongside the size to catch a regression that decouples
        // the saga ambient stamps.
        Assert.That(m2.TransactionId, Is.EqualTo(m1.TransactionId));
        Assert.That(m3.TransactionId, Is.EqualTo(m1.TransactionId));
        Assert.That(m4.TransactionId, Is.EqualTo(m1.TransactionId));
        Assert.That(m5.TransactionId, Is.EqualTo(m1.TransactionId));

        // Indices cover 0..N-1 exactly once each (set equality, not
        // ordering - saga execute order matches submission order
        // today, but the contract is "exactly-once-per-index").
        var indices = new[] { m1, m2, m3, m4, m5 }
            .Select(m => m.AtomicBatchIndex)
            .OrderBy(i => i)
            .ToArray();
        Assert.That(indices, Is.EqualTo(new[] { 0, 1, 2, 3, 4 }));
    }

    [Test]
    public async Task SetAsync_emits_zero_AtomicBatchSize_outside_a_saga()
    {
        // Single-key non-saga write: ambient context is unset, the
        // publish helper reads `null`, and both wire slots stamp 0.
        // This is the "not-in-a-saga" sentinel a receiver reads to
        // route the entry as a point write.
        var tree = await _fixture.CreateTreeAsync("obs-e2e-atomic-batch-non-saga");

        await tree.SetAsync("k", Encoding.UTF8.GetBytes("v"));

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.TreeId == "obs-e2e-atomic-batch-non-saga"
            && m.Key == "k");

        Assert.That(m.AtomicBatchSize, Is.EqualTo(0));
        Assert.That(m.AtomicBatchIndex, Is.EqualTo(0));
    }

    [Test]
    public async Task DeleteAsync_emits_zero_AtomicBatchSize_outside_a_saga()
    {
        // Single-key non-saga delete: exercises the PublishDeleteAsync
        // helper on BPlusLeafGrain (separate publish site from the
        // commit-time emit and from PublishSetAsync). Ambient context
        // is unset, both wire slots stamp 0 - the "not-in-a-saga"
        // sentinel applies uniformly to every MutationKind a leaf
        // grain emits.
        var tree = await _fixture.CreateTreeAsync("obs-e2e-atomic-batch-non-saga-delete");

        await tree.SetAsync("k", Encoding.UTF8.GetBytes("v"));
        await tree.DeleteAsync("k");

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.Delete
            && m.TreeId == "obs-e2e-atomic-batch-non-saga-delete"
            && m.Key == "k");

        Assert.That(m.AtomicBatchSize, Is.EqualTo(0));
        Assert.That(m.AtomicBatchIndex, Is.EqualTo(0));
    }
}