using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.UI.Downloads;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Downloads;

/// <summary>
/// A circuit's side of the download seam (issue #5366): it binds every ticket it mints
/// to itself, captures the connection a download runs over as this user, and withdraws
/// every ticket it minted when its sign-in or endpoint changes or the circuit ends.
/// </summary>
[TestFixture]
public sealed class ExplorerDownloadsTests
{
    private ExplorerDownloadTicketStore _store = null!;
    private IExplorerSession _session = null!;
    private IExplorerAuthSession _auth = null!;

    [SetUp]
    public void SetUp()
    {
        _store = new ExplorerDownloadTicketStore(new ManualClock());
        _session = Substitute.For<IExplorerSession>();
        _auth = Substitute.For<IExplorerAuthSession>();
    }

    [Test]
    public void Each_circuit_has_its_own_binding_and_a_ticket_redeems_only_with_it()
    {
        using var mine = new ExplorerDownloads(_store, _session, _auth);
        using var theirs = new ExplorerDownloads(_store, _session, _auth);
        var ticket = mine.Mint(new ExplorerDownloadTicketStoreTests.StubDownload());

        Assert.Multiple(() =>
        {
            Assert.That(mine.Binding, Is.Not.EqualTo(theirs.Binding));
            Assert.That(_store.Redeem(ticket.Token, theirs.Binding, out _), Is.EqualTo(ExplorerDownloadRedemption.WrongCircuit));
        });
    }

    [Test]
    public void The_captured_connection_carries_the_endpoint_sign_in_and_a_tenant_fixed_now()
    {
        var authentication = LatticeCallAuthentication.Basic("priya", "secret");
        _session.Current.Returns(new ExplorerConfiguration { Endpoint = "https://cluster.contoso.test" });
        _auth.GetAuthenticationFor("https://cluster.contoso.test").Returns(authentication);
        var live = new MutableTenant { AssertedTenant = "acme" };
        using var downloads = new ExplorerDownloads(_store, _session, _auth, new ShellAssertedTenant(live));

        var settings = downloads.CaptureConnection()!;
        live.AssertedTenant = "globex";

        Assert.Multiple(() =>
        {
            Assert.That(settings.Address, Is.EqualTo("https://cluster.contoso.test"));
            Assert.That(settings.Authentication, Is.SameAs(authentication));
            Assert.That(settings.ActiveTenantProvider!.AssertedTenant, Is.EqualTo("acme"), "a tenant switch after preparing changes nothing");
        });
    }

    [Test]
    public void No_connection_is_captured_before_an_endpoint_is_configured()
    {
        using var downloads = new ExplorerDownloads(_store, _session, _auth);

        Assert.That(downloads.CaptureConnection(), Is.Null);
    }

    [Test]
    public void A_sign_in_change_withdraws_every_ticket_the_circuit_minted()
    {
        using var downloads = new ExplorerDownloads(_store, _session, _auth);
        var ready = downloads.Mint(new ExplorerDownloadTicketStoreTests.StubDownload());
        var running = downloads.Mint(new ExplorerDownloadTicketStoreTests.StubDownload());
        _store.Redeem(running.Token, downloads.Binding, out _);

        _auth.AuthenticationChanged += Raise.Event<Action>();

        Assert.Multiple(() =>
        {
            Assert.That(ready.State, Is.EqualTo(ExplorerDownloadState.Revoked));
            Assert.That(_store.Redeem(ready.Token, downloads.Binding, out _), Is.EqualTo(ExplorerDownloadRedemption.Unknown));
            Assert.That(running.Withdrawn.IsCancellationRequested, Is.True);
        });
    }

    [Test]
    public void A_new_endpoint_withdraws_every_ticket_the_circuit_minted()
    {
        using var downloads = new ExplorerDownloads(_store, _session, _auth);
        var ticket = downloads.Mint(new ExplorerDownloadTicketStoreTests.StubDownload());

        _session.ConfigurationChanged += Raise.Event<Action>();

        Assert.That(ticket.State, Is.EqualTo(ExplorerDownloadState.Revoked));
    }

    [Test]
    public void Ending_the_circuit_withdraws_its_tickets_and_refuses_new_ones()
    {
        var downloads = new ExplorerDownloads(_store, _session, _auth);
        var ticket = downloads.Mint(new ExplorerDownloadTicketStoreTests.StubDownload());

        downloads.Dispose();
        downloads.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(ticket.State, Is.EqualTo(ExplorerDownloadState.Revoked));
            Assert.That(() => downloads.Mint(new ExplorerDownloadTicketStoreTests.StubDownload()), Throws.InstanceOf<ObjectDisposedException>());
            Assert.That(_store.Outstanding, Is.Zero);
        });
    }

    [Test]
    public void Withdrawing_one_ticket_leaves_the_others_and_ignores_a_stranger()
    {
        using var downloads = new ExplorerDownloads(_store, _session, _auth);
        using var other = new ExplorerDownloads(_store, _session, _auth);
        var first = downloads.Mint(new ExplorerDownloadTicketStoreTests.StubDownload());
        var second = downloads.Mint(new ExplorerDownloadTicketStoreTests.StubDownload());
        var foreign = other.Mint(new ExplorerDownloadTicketStoreTests.StubDownload());

        downloads.Withdraw(first);
        downloads.Withdraw(foreign);

        Assert.Multiple(() =>
        {
            Assert.That(first.State, Is.EqualTo(ExplorerDownloadState.Revoked));
            Assert.That(second.State, Is.EqualTo(ExplorerDownloadState.Ready));
            Assert.That(foreign.State, Is.EqualTo(ExplorerDownloadState.Ready), "a circuit withdraws only what it minted");
        });
    }

    [Test]
    public void A_head_without_a_session_or_sign_in_still_mints_and_captures_nothing()
    {
        using var downloads = new ExplorerDownloads(_store);

        Assert.Multiple(() =>
        {
            Assert.That(downloads.CaptureConnection(), Is.Null);
            Assert.That(downloads.Mint(new ExplorerDownloadTicketStoreTests.StubDownload()).State, Is.EqualTo(ExplorerDownloadState.Ready));
            Assert.That(() => new ExplorerDownloads(null!), Throws.ArgumentNullException);
        });
    }

    private sealed class MutableTenant : ILatticeActiveTenantProvider
    {
        public string? AssertedTenant { get; set; }
    }
}
