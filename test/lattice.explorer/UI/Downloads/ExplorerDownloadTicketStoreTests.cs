using System.Text;
using Orleans.Lattice.Explorer.UI.Downloads;

namespace Orleans.Lattice.Explorer.Tests.UI.Downloads;

/// <summary>
/// The process-wide ticket store (issue #5366): a ticket redeems once, with its own
/// circuit's binding, within its lifetime; every other presentation is refused and
/// spends the token; and the store is bounded.
/// </summary>
[TestFixture]
public sealed class ExplorerDownloadTicketStoreTests
{
    private const string Binding = "binding-a";

    [Test]
    public void A_fresh_ticket_redeems_once_with_its_binding_and_starts_running()
    {
        var store = new ExplorerDownloadTicketStore(new ManualClock());
        var ticket = store.Mint(new StubDownload(), Binding);

        var first = store.Redeem(ticket.Token, Binding, out var redeemed);
        var second = store.Redeem(ticket.Token, Binding, out var again);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(ExplorerDownloadRedemption.Redeemed));
            Assert.That(redeemed, Is.SameAs(ticket));
            Assert.That(ticket.State, Is.EqualTo(ExplorerDownloadState.Running));
            Assert.That(second, Is.EqualTo(ExplorerDownloadRedemption.Unknown), "a token is spent by its first presentation");
            Assert.That(again, Is.Null);
            Assert.That(store.Outstanding, Is.Zero);
        });
    }

    [Test]
    public void A_ticket_presented_after_its_lifetime_is_expired_and_spent()
    {
        var clock = new ManualClock();
        var store = new ExplorerDownloadTicketStore(clock);
        var ticket = store.Mint(new StubDownload(), Binding);
        clock.Advance(ExplorerDownloadTicketStore.DefaultLifetime);

        Assert.Multiple(() =>
        {
            Assert.That(store.Redeem(ticket.Token, Binding, out _), Is.EqualTo(ExplorerDownloadRedemption.Expired));
            Assert.That(ticket.State, Is.EqualTo(ExplorerDownloadState.Expired));
            Assert.That(store.Redeem(ticket.Token, Binding, out _), Is.EqualTo(ExplorerDownloadRedemption.Unknown));
        });
    }

    [Test]
    public void A_ticket_presented_by_another_circuit_is_refused_and_burned()
    {
        var store = new ExplorerDownloadTicketStore(new ManualClock());
        var ticket = store.Mint(new StubDownload(), Binding);

        Assert.Multiple(() =>
        {
            Assert.That(store.Redeem(ticket.Token, "binding-b", out _), Is.EqualTo(ExplorerDownloadRedemption.WrongCircuit));
            Assert.That(ticket.State, Is.EqualTo(ExplorerDownloadState.Refused));
            Assert.That(store.Redeem(ticket.Token, Binding, out _), Is.EqualTo(ExplorerDownloadRedemption.Unknown), "the owner cannot redeem a burned token");
        });
    }

    [TestCase(null)]
    [TestCase("")]
    public void A_ticket_presented_without_a_binding_is_refused(string? binding)
    {
        var store = new ExplorerDownloadTicketStore(new ManualClock());
        var ticket = store.Mint(new StubDownload(), Binding);

        Assert.That(store.Redeem(ticket.Token, binding, out _), Is.EqualTo(ExplorerDownloadRedemption.WrongCircuit));
    }

    [Test]
    public void Missing_overlong_and_unknown_tokens_are_unknown()
    {
        var store = new ExplorerDownloadTicketStore(new ManualClock());

        Assert.Multiple(() =>
        {
            Assert.That(store.Redeem(null, Binding, out _), Is.EqualTo(ExplorerDownloadRedemption.Unknown));
            Assert.That(store.Redeem(string.Empty, Binding, out _), Is.EqualTo(ExplorerDownloadRedemption.Unknown));
            Assert.That(store.Redeem(new string('a', ExplorerDownloads.MaxFieldLength + 1), Binding, out _), Is.EqualTo(ExplorerDownloadRedemption.Unknown));
            Assert.That(store.Redeem("not-a-token", Binding, out _), Is.EqualTo(ExplorerDownloadRedemption.Unknown));
        });
    }

    [Test]
    public void A_revoked_ticket_cannot_be_redeemed()
    {
        var store = new ExplorerDownloadTicketStore(new ManualClock());
        var ticket = store.Mint(new StubDownload(), Binding);

        store.Revoke(ticket);

        Assert.Multiple(() =>
        {
            Assert.That(ticket.State, Is.EqualTo(ExplorerDownloadState.Revoked));
            Assert.That(store.Redeem(ticket.Token, Binding, out _), Is.EqualTo(ExplorerDownloadRedemption.Unknown));
        });
    }

    [Test]
    public void Revoking_a_running_ticket_signals_its_download_to_stop()
    {
        var store = new ExplorerDownloadTicketStore(new ManualClock());
        var ticket = store.Mint(new StubDownload(), Binding);
        store.Redeem(ticket.Token, Binding, out _);

        store.Revoke(ticket);

        Assert.Multiple(() =>
        {
            Assert.That(ticket.Withdrawn.IsCancellationRequested, Is.True);
            Assert.That(ticket.State, Is.EqualTo(ExplorerDownloadState.Running), "it finishes when its download writes the trailer");
        });
    }

    [Test]
    public void Tokens_are_distinct_unguessable_url_safe_values()
    {
        var store = new ExplorerDownloadTicketStore(new ManualClock());
        var tokens = Enumerable.Range(0, 64).Select(_ => store.Mint(new StubDownload(), Binding).Token).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(tokens, Is.Unique);
            Assert.That(tokens, Has.All.Length.EqualTo(43), "32 random bytes, base64url without padding");
            Assert.That(tokens, Has.All.Matches<string>(token => token.All(c => char.IsAsciiLetterOrDigit(c) || c is '-' or '_')));
        });
    }

    [Test]
    public void The_store_is_bounded_and_sweeps_expired_tickets_to_make_room()
    {
        var clock = new ManualClock();
        var store = new ExplorerDownloadTicketStore(clock);
        var first = store.Mint(new StubDownload(), Binding);
        for (var i = 1; i < ExplorerDownloadTicketStore.MaxOutstanding; i++)
        {
            store.Mint(new StubDownload(), Binding);
        }

        Assert.That(() => store.Mint(new StubDownload(), Binding), Throws.InvalidOperationException);

        clock.Advance(ExplorerDownloadTicketStore.DefaultLifetime);
        var after = store.Mint(new StubDownload(), Binding);

        Assert.Multiple(() =>
        {
            Assert.That(store.Outstanding, Is.EqualTo(1));
            Assert.That(first.State, Is.EqualTo(ExplorerDownloadState.Expired), "a swept ticket tells its circuit it expired");
            Assert.That(after.State, Is.EqualTo(ExplorerDownloadState.Ready));
        });
    }

    [Test]
    public void Mint_rejects_missing_arguments()
    {
        var store = new ExplorerDownloadTicketStore();

        Assert.Multiple(() =>
        {
            Assert.That(() => store.Mint(null!, Binding), Throws.ArgumentNullException);
            Assert.That(() => store.Mint(new StubDownload(), string.Empty), Throws.ArgumentException);
            Assert.That(() => store.Revoke(null!), Throws.ArgumentNullException);
        });
    }

    /// <summary>A download that writes one line and a trailer line.</summary>
    internal sealed class StubDownload : ExplorerDownload
    {
        public override string FileName => "stub.txt";

        public override string ContentType => "text/plain";

        protected override string Format => "stub/1";

        protected override async Task<ExplorerDownloadTrailer> WriteBodyAsync(Stream body, ExplorerDownloadTicket ticket, CancellationToken cancellationToken)
        {
            await body.WriteAsync("row\n"u8.ToArray(), cancellationToken);
            ticket.Report(1, 4);
            return new ExplorerDownloadTrailer(Format, 1, true, ExplorerDownloadRefusal.None, null);
        }

        protected override async ValueTask WriteTrailerAsync(Stream body, ExplorerDownloadTrailer trailer, CancellationToken cancellationToken) =>
            await body.WriteAsync(Encoding.UTF8.GetBytes($"#{trailer.Complete}\n"), cancellationToken);
    }
}
