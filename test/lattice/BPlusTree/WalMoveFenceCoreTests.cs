using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for the pure durable-fence decisions of <see cref="WalMoveFenceCore"/>
/// (issue #4525) and the two in-memory fence seams it already carried.
/// </summary>
[TestFixture]
public sealed class WalMoveFenceCoreTests
{
    private const string Source = "default";
    private const long Now = 1_000_000;

    private static WalMoveFence Fence(string moveId = "m1", string source = Source, long expires = Now + 10)
        => new() { MoveId = moveId, SourceProviderKey = source, LeaseExpiresUtcTicks = expires };

    [Test]
    public void IsAppendAdmitted_refuses_only_a_fenced_activation()
    {
        Assert.Multiple(() =>
        {
            Assert.That(WalMoveFenceCore.IsAppendAdmitted(false), Is.True);
            Assert.That(WalMoveFenceCore.IsAppendAdmitted(true), Is.False);
        });
    }

    [Test]
    public void ShouldAbortStaleQuiesce_aborts_only_a_coordinator_behind_the_activation()
    {
        Assert.Multiple(() =>
        {
            Assert.That(WalMoveFenceCore.ShouldAbortStaleQuiesce(5, 3), Is.True);
            Assert.That(WalMoveFenceCore.ShouldAbortStaleQuiesce(3, 3), Is.False);
            Assert.That(WalMoveFenceCore.ShouldAbortStaleQuiesce(0, 7), Is.False);
        });
    }

    [Test]
    public void EvaluateActivationFence_fences_an_activation_of_the_source_while_the_lease_holds()
    {
        Assert.Multiple(() =>
        {
            Assert.That(WalMoveFenceCore.EvaluateActivationFence(null, Source, Now), Is.EqualTo(WalMoveFenceActivation.Unfenced));
            Assert.That(WalMoveFenceCore.EvaluateActivationFence(Fence(), Source, Now), Is.EqualTo(WalMoveFenceActivation.Fenced));
            Assert.That(WalMoveFenceCore.EvaluateActivationFence(Fence(expires: Now), Source, Now),
                Is.EqualTo(WalMoveFenceActivation.ReleaseExpired));
            Assert.That(WalMoveFenceCore.EvaluateActivationFence(Fence(), "secondary", Now),
                Is.EqualTo(WalMoveFenceActivation.Unfenced), "a fence on a provider the placement has left is inert");
        });
    }

    [Test]
    public void EvaluateRaise_raises_renews_takes_over_and_refuses()
    {
        Assert.Multiple(() =>
        {
            Assert.That(WalMoveFenceCore.EvaluateRaise(null, Source, "m1", renew: false, Now), Is.EqualTo(WalMoveFenceRaise.Raise));
            Assert.That(WalMoveFenceCore.EvaluateRaise(null, Source, "m1", renew: true, Now), Is.EqualTo(WalMoveFenceRaise.RefusedReleased));
            Assert.That(WalMoveFenceCore.EvaluateRaise(Fence(), Source, "m1", renew: true, Now), Is.EqualTo(WalMoveFenceRaise.Renew));
            Assert.That(WalMoveFenceCore.EvaluateRaise(Fence(), Source, "m1", renew: false, Now), Is.EqualTo(WalMoveFenceRaise.Renew));
            Assert.That(WalMoveFenceCore.EvaluateRaise(Fence("m0"), Source, "m1", renew: false, Now),
                Is.EqualTo(WalMoveFenceRaise.RefusedHeldByOtherMove));
            Assert.That(WalMoveFenceCore.EvaluateRaise(Fence("m0", expires: Now), Source, "m1", renew: false, Now),
                Is.EqualTo(WalMoveFenceRaise.TakeOver));
            Assert.That(WalMoveFenceCore.EvaluateRaise(Fence("m0", expires: Now), Source, "m1", renew: true, Now),
                Is.EqualTo(WalMoveFenceRaise.RefusedReleased), "a renewal never takes over another move's fence");
            Assert.That(WalMoveFenceCore.EvaluateRaise(Fence("m0", source: "old"), Source, "m1", renew: false, Now),
                Is.EqualTo(WalMoveFenceRaise.Raise), "a fence on a provider the placement has left is treated as absent");
        });
    }

    [Test]
    public void IsReleaseAdmitted_requires_the_moves_own_fence_and_a_lapsed_lease_when_asked()
    {
        Assert.Multiple(() =>
        {
            Assert.That(WalMoveFenceCore.IsReleaseAdmitted(null, "m1", onlyIfExpired: false, Now), Is.False);
            Assert.That(WalMoveFenceCore.IsReleaseAdmitted(Fence("m0"), "m1", onlyIfExpired: false, Now), Is.False);
            Assert.That(WalMoveFenceCore.IsReleaseAdmitted(Fence(), "m1", onlyIfExpired: false, Now), Is.True);
            Assert.That(WalMoveFenceCore.IsReleaseAdmitted(Fence(), "m1", onlyIfExpired: true, Now), Is.False);
            Assert.That(WalMoveFenceCore.IsReleaseAdmitted(Fence(expires: Now), "m1", onlyIfExpired: true, Now), Is.True);
        });
    }

    [Test]
    public void IsFlipAdmitted_requires_the_moves_own_fence_whatever_its_lease()
    {
        Assert.Multiple(() =>
        {
            Assert.That(WalMoveFenceCore.IsFlipAdmitted(null, "m1"), Is.False, "a released fence refuses the flip");
            Assert.That(WalMoveFenceCore.IsFlipAdmitted(Fence("m0"), "m1"), Is.False, "a taken-over fence refuses the flip");
            Assert.That(WalMoveFenceCore.IsFlipAdmitted(Fence(), "m1"), Is.True);
            Assert.That(WalMoveFenceCore.IsFlipAdmitted(Fence(expires: 0), "m1"), Is.True,
                "a lapsed fence nobody released still guards the source");
        });
    }
}
