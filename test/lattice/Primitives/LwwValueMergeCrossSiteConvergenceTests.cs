using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Cross-site convergence regression for <see cref="LwwValue{T}.Merge"/>.
/// <para>
/// <see cref="LwwValueMergeConvergenceTests"/> proves the merge is a
/// commutative, associative, idempotent join - a semilattice - but every case
/// there hands both operands the <em>same</em> <c>OriginClusterId</c>. Those
/// laws guarantee replica convergence only when every replica feeds the join
/// the same inputs, and on this field they do not.
/// </para>
/// <para>
/// <c>OriginClusterId</c> is observer-relative, not a fact about the write.
/// <c>LatticeOriginContext</c> documents that "local writes leave the context
/// unset, producing a null origin", so the authoring site stores its own write
/// with a <see langword="null"/> origin while every peer stores that same write
/// stamped with the authoring cluster's id (set by
/// <c>LatticeOriginContext.With</c> inside the replication apply seam). The
/// secondary tie-break ordered on that field and null sorts before any non-null
/// id, so on a bare HLC tie each site ranked <em>its own</em> write last and
/// three sites reached up to three different answers. The fix orders every
/// replica-invariant field ahead of the observer-relative one.
/// </para>
/// </summary>
[TestFixture]
public class LwwValueMergeCrossSiteConvergenceTests
{
    private const string Alpha = "site-alpha";
    private const string Gamma = "site-gamma";

    private static readonly HybridLogicalClock Tie = new() { WallClockTicks = 12345, Counter = 0 };

    private static readonly byte[] AlphaPayload = "site-0-write-007"u8.ToArray();
    private static readonly byte[] GammaPayload = "site-2-write-007"u8.ToArray();

    /// <summary>
    /// Builds the view a site holds of a single write: <see langword="null"/>
    /// origin when that site authored it, the authoring cluster's id otherwise.
    /// </summary>
    private static LwwValue<byte[]> AsObservedBy(string observingSite, string authoringSite, byte[] payload) =>
        LwwValue<byte[]>.Create(payload, Tie) with
        {
            OriginClusterId = string.Equals(observingSite, authoringSite, StringComparison.Ordinal)
                ? null
                : authoringSite,
        };

    /// <summary>
    /// Resolves the pair of tied writes as <paramref name="observingSite"/>
    /// holds them, merging in both operand orders to keep arrival order out of
    /// the result.
    /// </summary>
    private static byte[]? ResolveAt(string observingSite)
    {
        var fromAlpha = AsObservedBy(observingSite, Alpha, AlphaPayload);
        var fromGamma = AsObservedBy(observingSite, Gamma, GammaPayload);

        var forward = LwwValue<byte[]>.Merge(fromAlpha, fromGamma);
        var reverse = LwwValue<byte[]>.Merge(fromGamma, fromAlpha);

        Assert.That(reverse.Value, Is.EqualTo(forward.Value),
            $"merge at {observingSite} must not depend on operand order");

        return forward.Value;
    }

    [Test]
    public void Merge_converges_across_sites_when_the_authoring_site_holds_its_own_write_with_a_null_origin()
    {
        // Site beta authored neither write, so it sees both origins stamped.
        var atAlpha = ResolveAt(Alpha);
        var atBeta = ResolveAt("site-beta");
        var atGamma = ResolveAt(Gamma);

        Assert.Multiple(() =>
        {
            Assert.That(atBeta, Is.EqualTo(atAlpha),
                "a site that authored neither write must agree with the site that authored one");
            Assert.That(atGamma, Is.EqualTo(atAlpha),
                "the two authoring sites must agree: each stores its own write with a null origin, "
                + "so an origin-ranked tie-break makes each of them rank its own write last");
        });
    }

    [Test]
    public void Merge_converges_across_sites_when_only_one_side_authored_locally()
    {
        // The minimal divergent pair: alpha's view (null vs gamma) against
        // gamma's view (alpha vs null). Both describe the same two writes.
        var alphaView = LwwValue<byte[]>.Merge(
            LwwValue<byte[]>.Create(AlphaPayload, Tie),
            LwwValue<byte[]>.Create(GammaPayload, Tie) with { OriginClusterId = Gamma });

        var gammaView = LwwValue<byte[]>.Merge(
            LwwValue<byte[]>.Create(AlphaPayload, Tie) with { OriginClusterId = Alpha },
            LwwValue<byte[]>.Create(GammaPayload, Tie));

        Assert.That(gammaView.Value, Is.EqualTo(alphaView.Value),
            "the same two writes must resolve to the same value on both authoring sites");
    }

    [Test]
    public void Merge_orders_the_replica_invariant_value_ahead_of_the_observer_relative_origin()
    {
        // Values differ, so the value compare must decide regardless of how the
        // two sites happen to have stamped the origin. Lexicographically
        // "site-2..." > "site-0...", so the gamma payload wins on every site.
        var atAlpha = ResolveAt(Alpha);

        Assert.That(atAlpha, Is.EqualTo(GammaPayload),
            "the lexicographic byte compare is replica-invariant and must settle the tie");
    }
}
