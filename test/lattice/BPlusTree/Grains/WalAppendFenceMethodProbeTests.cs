using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The definite "method unknown" predicate of the append fence (epic #5605): only
/// the receiver's refusal to deserialise an invokable it does not know is
/// re-issued unstamped; every other failure keeps today's semantics. The
/// failure's real shape is pinned against a silo missing the invokables by
/// <c>WalAppendFenceMixedVersionTests</c>.
/// </summary>
[TestFixture]
public sealed class WalAppendFenceMethodProbeTests
{
    private const string Unresolved = "Unable to resolve type alias \"[inv,Orleans.Runtime.GrainReference,ol.wsg,1a2b3c4d]\".";

    [Test]
    public void An_unresolved_invokable_alias_is_a_definite_method_unknown()
    {
        Assert.Multiple(() =>
        {
            Assert.That(WalAppendFenceMethodProbe.IsDefiniteMethodUnknown(new TypeLoadException(Unresolved)), Is.True);
            Assert.That(WalAppendFenceMethodProbe.IsDefiniteMethodUnknown(
                new InvalidOperationException("wrapped", new TypeLoadException(Unresolved))), Is.True);
            Assert.That(WalAppendFenceMethodProbe.IsDefiniteMethodUnknown(
                new AggregateException(new TimeoutException(), new TypeLoadException(Unresolved))), Is.True);
        });
    }

    [Test]
    public void Ambiguous_failures_are_never_method_unknown()
    {
        Assert.Multiple(() =>
        {
            Assert.That(WalAppendFenceMethodProbe.IsDefiniteMethodUnknown(new TimeoutException("Response did not arrive on time")), Is.False,
                "a timeout may have been allocated, so it keeps today's retry semantics");
            Assert.That(WalAppendFenceMethodProbe.IsDefiniteMethodUnknown(new TypeLoadException("Could not load type Foo")), Is.False);
            Assert.That(WalAppendFenceMethodProbe.IsDefiniteMethodUnknown(new InvalidOperationException(Unresolved)), Is.False);
        });
    }
}
