using System.Text;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Convergence tests for the CRDT shapes the retrieval layer relies on: the
/// add-wins membership <see cref="OrSet"/> of live source identifiers converges
/// under a concurrent add and retire, and the per-vector metadata's
/// last-writer-wins content-address register converges to the later re-embed
/// regardless of merge order - the two invariants that let concurrent replicas
/// agree on the same search set after a re-embed.
/// </summary>
[TestFixture]
public sealed class VectorRetrievalConvergenceTests
{
    private static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    private static IReadOnlyList<string> Members(OrSet set)
        => set.Elements().Select(e => Encoding.UTF8.GetString(e)).OrderBy(s => s, StringComparer.Ordinal).ToList();

    [Test]
    public void Concurrent_add_and_retire_of_source_ids_converge_add_wins()
    {
        // Both replicas start from a baseline that already observed source "s1".
        var seed = new OrSet();
        seed.Add(Encoding.UTF8.GetBytes("s1"), "seed", 1);

        var a = seed.Clone();
        a.Add(Encoding.UTF8.GetBytes("s2"), "A", 1); // A adds a new source concurrently.

        var b = seed.Clone();
        b.Remove(Encoding.UTF8.GetBytes("s1")); // B retires the seed source concurrently.

        var ab = OrSet.Merge(a, b);
        var ba = OrSet.Merge(b, a);

        Assert.Multiple(() =>
        {
            Assert.That(Members(ab), Is.EqualTo(new[] { "s2" }), "The retire removes the observed s1; the concurrent add of s2 survives.");
            Assert.That(Members(ba), Is.EqualTo(Members(ab)), "Merge is commutative.");
        });
    }

    [Test]
    public void Reembed_content_address_register_converges_to_the_later_write()
    {
        var space = new EmbeddingSpaceTag("m", 8, VectorNormalization.UnitL2);
        var baseline = new VectorMetadataRecord { RepoId = "acme", VectorId = "v", Space = space };

        // The same source re-embedded: an earlier write recorded addr-old, a later
        // (higher HLC) write recorded addr-new. Whatever the merge order, the later
        // write must win so both replicas agree on the live content address.
        var old = baseline with { ContentAddress = RepoContextValues.Lww("addr-old", Clock(100)) };
        var fresh = baseline with { ContentAddress = RepoContextValues.Lww("addr-new", Clock(200)) };

        Assert.Multiple(() =>
        {
            Assert.That(
                RepoContextValues.ReadString(VectorMetadataRecord.Merge(old, fresh).ContentAddress),
                Is.EqualTo("addr-new"));
            Assert.That(
                RepoContextValues.ReadString(VectorMetadataRecord.Merge(fresh, old).ContentAddress),
                Is.EqualTo("addr-new"));
        });
    }
}
