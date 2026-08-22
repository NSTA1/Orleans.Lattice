using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Fast, dependency-free unit tests for <see cref="SplitBoundary"/> - the shared
/// split-boundary ownership rule the production leaf <c>ShouldApplyDuringReplay</c>
/// range half executes. These pin the half-open <c>[low, high)</c> semantics so the
/// split-boundary sealing (a sealed donor never owning a key at or past the split
/// key) is caught here rather than only by a slow reshard chaos run.
/// </summary>
[TestFixture]
public sealed class SplitBoundaryTests
{
    [Test]
    public void Null_bounds_own_every_key()
    {
        Assert.That(SplitBoundary.Owns("anything", lowInclusive: null, highExclusive: null), Is.True);
    }

    [Test]
    public void Low_bound_is_inclusive()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SplitBoundary.Owns("m", lowInclusive: "m", highExclusive: null), Is.True);
            Assert.That(SplitBoundary.Owns("l", lowInclusive: "m", highExclusive: null), Is.False);
            Assert.That(SplitBoundary.Owns("n", lowInclusive: "m", highExclusive: null), Is.True);
        });
    }

    [Test]
    public void High_bound_is_exclusive_so_split_key_belongs_to_sibling()
    {
        // A donor sealed at split key "m" must not own "m": it now belongs to the
        // destination sibling. This is the split-boundary sealing invariant.
        Assert.Multiple(() =>
        {
            Assert.That(SplitBoundary.Owns("m", lowInclusive: null, highExclusive: "m"), Is.False);
            Assert.That(SplitBoundary.Owns("l", lowInclusive: null, highExclusive: "m"), Is.True);
            Assert.That(SplitBoundary.Owns("n", lowInclusive: null, highExclusive: "m"), Is.False);
        });
    }

    [Test]
    public void Both_bounds_define_a_half_open_range()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SplitBoundary.Owns("d", lowInclusive: "d", highExclusive: "m"), Is.True);
            Assert.That(SplitBoundary.Owns("g", lowInclusive: "d", highExclusive: "m"), Is.True);
            Assert.That(SplitBoundary.Owns("m", lowInclusive: "d", highExclusive: "m"), Is.False);
            Assert.That(SplitBoundary.Owns("c", lowInclusive: "d", highExclusive: "m"), Is.False);
        });
    }

    [Test]
    public void Null_key_throws()
    {
        Assert.That(
            () => SplitBoundary.Owns(null!, lowInclusive: null, highExclusive: null),
            Throws.ArgumentNullException);
    }
}
