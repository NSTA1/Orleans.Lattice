using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class LatticeGrainTests
{
    // --- Domain-fault absorption regression tests (issue #3361) ---
    //
    // The stale-alias catch clauses in LatticeGrain catch the framework
    // InvalidOperationException raised when a cached alias points at a deleted
    // physical tree. A Lattice exception that merely happens to derive from the
    // same base was absorbed by those clauses too, which discarded the whole
    // routing cache and re-issued the call - remediation chosen for a fault
    // that did not occur.
    //
    // The call-count assertion is what makes these regression tests rather than
    // smoke tests. Asserting only that the exception surfaces would pass both
    // before and after the fix, because the absorbed first attempt is followed
    // by a second attempt that rethrows. Exactly one call is the observable
    // difference between declining the fault and absorbing it.
    //
    // The sibling tests in LatticeGrainTests.StaleAlias.cs are the guard
    // against over-filtering: they prove the genuine stale-alias retry still
    // happens for a framework InvalidOperationException.

    [Test]
    public void GetAsync_does_not_absorb_a_domain_fault_as_a_stale_alias()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        var callCount = 0;
        shardRoot.GetAsync("k1").Returns<Task<byte[]?>>(_ =>
        {
            callCount++;
            throw new LatticeSaturatedException("Write-ahead log is saturated.", "my-tree");
        });

        Assert.That(
            async () => await grain.GetAsync("k1"),
            Throws.TypeOf<LatticeSaturatedException>());

        Assert.That(callCount, Is.EqualTo(1), "a domain fault must not be retried as a stale alias");
    }

    [Test]
    public void SetManyAsync_does_not_absorb_a_domain_fault_as_a_stale_alias()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        SetupCompactionGrain(factory, "my-tree");
        var callCount = 0;
        shardRoot.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>()).Returns<Task>(_ =>
        {
            callCount++;
            throw new LatticeSaturatedException("Write-ahead log is saturated.", "my-tree");
        });

        Assert.That(
            async () => await grain.SetManyAsync([new KeyValuePair<string, byte[]>("k1", [1])]),
            Throws.TypeOf<LatticeSaturatedException>());

        Assert.That(callCount, Is.EqualTo(1), "a domain fault must not be retried as a stale alias");
    }

    [Test]
    public void SetAsync_does_not_absorb_a_domain_fault_as_a_stale_alias()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        SetupCompactionGrain(factory, "my-tree");
        var callCount = 0;
        shardRoot.SetAsync("k1", Arg.Any<byte[]>()).Returns<Task>(_ =>
        {
            callCount++;
            throw new LatticeSaturatedException("Write-ahead log is saturated.", "my-tree");
        });

        Assert.That(
            async () => await grain.SetAsync("k1", [1]),
            Throws.TypeOf<LatticeSaturatedException>());

        Assert.That(callCount, Is.EqualTo(1), "a domain fault must not be retried as a stale alias");
    }
}
