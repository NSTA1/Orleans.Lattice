using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Session;

namespace Orleans.Lattice.Explorer.Tests.UI.Session;

/// <summary>
/// One circuit, one outage toast (issue #3831): a lost connection is announced once
/// when it goes from usable to faulted, a repeated fault announces nothing, and the
/// toast is withdrawn the moment the connection recovers.
/// </summary>
[TestFixture]
public sealed class SessionConnectionAnnouncerTests
{
    private static readonly LatticeConnectionStatus Connected = new(LatticeConnectionState.Connected, "http://localhost:5199", "Connected.");
    private static readonly LatticeConnectionStatus Faulted = new(LatticeConnectionState.Faulted, "http://localhost:5199", "The state API is unavailable.");

    [Test]
    public void A_lost_connection_is_announced_once_and_withdrawn_on_recovery()
    {
        var (connection, toasts, announcer) = Create();

        connection.Move(Faulted);
        connection.Move(Faulted with { Message = "Still unavailable." });
        var during = toasts.Toasts.ToArray();
        connection.Move(Connected);

        Assert.Multiple(() =>
        {
            Assert.That(during, Has.Length.EqualTo(1));
            Assert.That(during[0].Tone, Is.EqualTo(LtToastTone.Danger));
            Assert.That(during[0].Message, Is.EqualTo("Disconnected from http://localhost:5199: The state API is unavailable."));
            Assert.That(toasts.Toasts, Is.Empty, "recovery withdraws the outage toast");
            Assert.That(announcer.CurrentToast, Is.Null);
        });
    }

    [Test]
    public void A_second_outage_after_recovery_is_announced_again()
    {
        var (connection, toasts, _) = Create();

        connection.Move(Faulted);
        connection.Move(Connected);
        connection.Move(Faulted);

        Assert.That(toasts.Toasts, Has.Count.EqualTo(1));
    }

    [Test]
    public void Reconnecting_is_not_an_outage_and_does_not_withdraw_one()
    {
        var (connection, toasts, _) = Create();

        connection.Move(Connected with { State = LatticeConnectionState.Reconnecting });
        Assert.That(toasts.Toasts, Is.Empty);

        connection.Move(Faulted);
        connection.Move(Connected with { State = LatticeConnectionState.Reconnecting });
        Assert.That(toasts.Toasts, Has.Count.EqualTo(1), "only a recovery withdraws it");
    }

    [Test]
    public void Starting_twice_listens_once_and_disposing_stops_listening()
    {
        var (connection, toasts, announcer) = Create();
        announcer.Start();

        announcer.Dispose();
        connection.Move(Faulted);

        Assert.That(toasts.Toasts, Is.Empty);
    }

    [Test]
    public void A_connection_already_faulted_when_it_starts_is_not_announced_again()
    {
        var connection = new FakeStateConnection();
        connection.Seed(Faulted);
        var toasts = new LtToastService();
        using var announcer = new SessionConnectionAnnouncer(new FakeExplorerSession(connection), toasts);
        announcer.Start();

        connection.Move(Faulted);

        Assert.That(toasts.Toasts, Is.Empty);
    }

    [Test]
    public void The_announcer_rejects_missing_collaborators()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => new SessionConnectionAnnouncer(null!, new LtToastService()), Throws.ArgumentNullException);
            Assert.That(() => new SessionConnectionAnnouncer(new FakeExplorerSession(new FakeStateConnection()), null!), Throws.ArgumentNullException);
        });
    }

    private static (FakeStateConnection Connection, LtToastService Toasts, SessionConnectionAnnouncer Announcer) Create()
    {
        var connection = new FakeStateConnection();
        connection.Seed(Connected);
        var toasts = new LtToastService();
        var announcer = new SessionConnectionAnnouncer(new FakeExplorerSession(connection), toasts);
        announcer.Start();
        return (connection, toasts, announcer);
    }
}
