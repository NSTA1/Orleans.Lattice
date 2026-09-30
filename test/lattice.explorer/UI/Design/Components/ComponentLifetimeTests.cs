using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

/// <summary>
/// <see cref="ComponentLifetime"/>: the token a component hands its work is cancelled when it
/// is renewed or the component is left, and no member ever throws for having been left
/// (issue #4011).
/// </summary>
[TestFixture]
public sealed class ComponentLifetimeTests
{
    [Test]
    public void A_new_lifetime_is_live()
    {
        var lifetime = new ComponentLifetime();

        Assert.Multiple(() =>
        {
            Assert.That(lifetime.IsLeft, Is.False);
            Assert.That(lifetime.Token.CanBeCanceled, Is.True);
            Assert.That(lifetime.Token.IsCancellationRequested, Is.False);
        });
    }

    [Test]
    public void Leave_cancels_the_work_and_every_member_still_answers()
    {
        var lifetime = new ComponentLifetime();
        var token = lifetime.Token;

        lifetime.Leave();

        Assert.Multiple(() =>
        {
            Assert.That(lifetime.IsLeft, Is.True);
            Assert.That(token.IsCancellationRequested, Is.True);
            Assert.That(() => lifetime.Token, Throws.Nothing, "a read resuming after the owner is gone must not throw");
            Assert.That(lifetime.Token.IsCancellationRequested, Is.True);
            Assert.That(() => lifetime.Token.Register(() => { }), Throws.Nothing);
        });
    }

    [Test]
    public void Leave_twice_does_nothing_more()
    {
        var lifetime = new ComponentLifetime();

        lifetime.Leave();

        Assert.That(lifetime.Leave, Throws.Nothing);
    }

    [Test]
    public void Renew_cancels_the_work_it_replaces_and_hands_out_a_live_token()
    {
        var lifetime = new ComponentLifetime();
        var first = lifetime.Token;

        var second = lifetime.Renew();

        Assert.Multiple(() =>
        {
            Assert.That(first.IsCancellationRequested, Is.True);
            Assert.That(second.IsCancellationRequested, Is.False);
            Assert.That(lifetime.Token, Is.EqualTo(second));
            Assert.That(lifetime.IsLeft, Is.False, "renewing is not leaving");
        });
    }

    [Test]
    public void Leave_cancels_the_renewed_work()
    {
        var lifetime = new ComponentLifetime();
        var renewed = lifetime.Renew();

        lifetime.Leave();

        Assert.That(renewed.IsCancellationRequested, Is.True);
    }

    [Test]
    public void Renew_after_leaving_hands_out_a_cancelled_token()
    {
        var lifetime = new ComponentLifetime();
        lifetime.Leave();

        var token = lifetime.Renew();

        Assert.Multiple(() =>
        {
            Assert.That(token.IsCancellationRequested, Is.True, "no work starts for an owner that is gone");
            Assert.That(lifetime.IsLeft, Is.True);
        });
    }
}
