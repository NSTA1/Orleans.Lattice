using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.CompilerServices;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #2823. The arms for the axis the revision cookie and the expiry
/// horizon together still cannot see: a settled read that resolved uncommitted
/// transactional writes against the transaction registry while answering.
/// <para>
/// Such an answer is a function of registry decision state as well as of the
/// leaf's rows. A decision tombstone expiring by the clock flips a resolved
/// outcome with nothing written to the leaf, so the cookie holds, and the row
/// it flips was never surfaced, so it contributes nothing to the horizon. Both
/// halves of the original gate pass and the settled page is stale. The leaf
/// therefore records the cookie at which a read consulted the registry, and
/// the gate refuses a settled read whose issue-time cookie is that value.
/// </para>
/// <para>
/// Every refusal arm here holds the cookie <em>equal</em> and the horizon
/// unbounded, so the only clause that can refuse is the one under test. The
/// control arm is what proves the signal is reaching the gate at all: it
/// publishes a signal naming a different cookie and requires reuse.
/// </para>
/// </summary>
public partial class ShardRootGrainScanPageLeafReadCoalescingTests
{
    /// <summary>
    /// Reflective handle on the static registry <see cref="BPlusLeafGrain"/>
    /// records transactional reads into, with the same fail-loud contract as
    /// <see cref="RegistryForTest"/>.
    /// </summary>
    private static ConcurrentDictionary<GrainId, StrongBox<long>> TransactionalReadRegistryForTest()
    {
        var field = typeof(BPlusLeafGrain).GetField(
            "LeafTransactionalReadRegistry",
            BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException(
                "LeafTransactionalReadRegistry field not found on BPlusLeafGrain - the static "
                + "field's name has changed. These arms drive it directly; update this helper.");

        var value = field.GetValue(null)
            ?? throw new InvalidOperationException("LeafTransactionalReadRegistry field returned null");

        return value as ConcurrentDictionary<GrainId, StrongBox<long>>
            ?? throw new InvalidOperationException(
                "LeafTransactionalReadRegistry is not a ConcurrentDictionary<GrainId, StrongBox<long>> "
                + $"(got {value.GetType().FullName}).");
    }

    /// <summary>
    /// Records that a read of <paramref name="leafId"/> consulted the
    /// transaction registry at <paramref name="revision"/>, as a real leaf does
    /// at the end of a range read over a non-empty pending set, and asserts the
    /// shard's accessor reads it back.
    /// </summary>
    private void PublishTransactionalRead(GrainId leafId, long revision)
    {
        TransactionalReadRegistryForTest().AddOrUpdate(
            leafId,
            _ => new StrongBox<long>(revision),
            (_, box) =>
            {
                Volatile.Write(ref box.Value, revision);
                return box;
            });

        if (!_publishedRevisions.Contains(leafId))
        {
            _publishedRevisions.Add(leafId);
        }

        Assert.That(BPlusLeafGrain.TryGetLeafTransactionalReadRevision(leafId, out var readBack), Is.True,
            "precondition: the signal must be readable through the accessor the shard uses");
        Assert.That(readBack, Is.EqualTo(revision),
            "precondition: and must read back the value just published");
    }

    /// <summary>
    /// The perturbation arm the issue asks for. The settled read consulted the
    /// transaction registry at its issue-time cookie, then a decision tombstone
    /// expires: nothing is written, so the cookie holds, and the horizon is
    /// unbounded because no surfaced row expires. Without the transactional
    /// clause both halves of the original gate admit the settled page, which
    /// was resolved against a decision that no longer holds.
    /// </summary>
    [Test]
    public async Task A_settled_read_that_resolved_pending_transactions_is_refused_after_a_decision_expires()
    {
        var chain = CreateParkableLeaf(TimeSpan.FromMilliseconds(200), leafKey: "reuse-tx-decision");
        PublishRevision(chain.LeafId, 7070);

        var stalled = await AttemptAsync(chain.Grain);
        Assert.That(stalled, Is.Null, "precondition: the first attempt stalls on the parked read");

        // The leaf held prepared writes while answering, so its read resolved
        // them against the registry and recorded the cookie it ran at.
        PublishTransactionalRead(chain.LeafId, 7070);

        chain.ReleasePark();
        await Task.Yield();

        // A decision tombstone expires by the clock. No writer runs: the
        // cookie and the horizon are exactly as the settled read left them.
        Assert.That(BPlusLeafGrain.TryGetLeafRevision(chain.LeafId, out var unchanged), Is.True);
        Assert.That(unchanged, Is.EqualTo(7070),
            "precondition: the cookie must still be the issue-time value, or this arm would be "
            + "reddening through the advanced-cookie clause and proving nothing about the registry");
        Assert.That(BPlusLeafGrain.TryGetLeafExpiryHorizon(chain.LeafId, out var horizon), Is.True);
        Assert.That(horizon, Is.EqualTo(long.MaxValue),
            "precondition: the horizon must be unbounded, or this arm would be reddening through "
            + "the expiry clause and proving nothing about the registry");

        chain.Park = false;
        var page = await chain.Grain.GetSortedEntriesBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 64, continuationToken: null);

        Assert.Multiple(() =>
        {
            Assert.That(chain.Reads, Has.Count.EqualTo(2),
                "a settled read that resolved pending transactions must not be reused: its answer "
                + "depends on registry decisions that can change with nothing written to the leaf, "
                + "so an unchanged cookie and horizon prove nothing about it");
            Assert.That(page.Entries, Is.Not.Empty,
                "vacuity control: the fresh read must actually have produced a page");
        });
    }

    /// <summary>
    /// The control for the arm above, and the proof that the signal is keyed to
    /// the cookie rather than to the leaf. A transactional read recorded at an
    /// <em>earlier</em> cookie - one the pending set has since drained past -
    /// says nothing about a read issued at the current cookie, and reuse must
    /// resume.
    /// <para>
    /// Without this, a gate that refused whenever any signal was present would
    /// satisfy the refusal arm while disabling reuse for the rest of the leaf's
    /// activation after its first transaction, turning the optimisation off
    /// silently with every test green.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_settled_read_is_reused_when_the_only_transactional_read_was_at_an_earlier_cookie()
    {
        var chain = CreateParkableLeaf(TimeSpan.FromMilliseconds(200), leafKey: "reuse-tx-drained");

        // A read at cookie 8080 consulted the registry; the pending set then
        // drained, which advanced the cookie to 8081.
        PublishTransactionalRead(chain.LeafId, 8080);
        PublishRevision(chain.LeafId, 8081);

        var stalled = await AttemptAsync(chain.Grain);
        Assert.That(stalled, Is.Null, "precondition: the first attempt stalls on the parked read");

        chain.ReleasePark();
        await Task.Yield();

        chain.Park = false;
        var page = await chain.Grain.GetSortedEntriesBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 64, continuationToken: null);

        Assert.Multiple(() =>
        {
            Assert.That(chain.Reads, Has.Count.EqualTo(1),
                "a transactional read at an earlier cookie must not refuse a settled read issued "
                + "after the pending set drained; refusing here would disable reuse for the rest "
                + "of the activation");
            Assert.That(page.Entries.Select(e => e.Key), Is.EqualTo(chain.Rows.Select(r => r.Key)),
                "and the rows it serves must be the leaf's rows");
        });
    }
}
