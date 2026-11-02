using System.Text;
using System.Text.Json;
using Orleans.Lattice.Explorer.UI.Downloads;

namespace Orleans.Lattice.Explorer.Tests.UI.Downloads;

/// <summary>
/// The download base and its trailer (issue #5366): whatever happens to the body - it
/// completes, it faults, or it is stopped - the file ends with a trailer that says so,
/// and the ticket finishes with that trailer. A fault's own text never reaches the file.
/// </summary>
[TestFixture]
public sealed class ExplorerDownloadTests
{
    [Test]
    public async Task A_body_that_completes_ends_with_its_own_trailer()
    {
        var (ticket, body) = Start(new ScriptedDownload(_ => Task.CompletedTask));

        var trailer = await ticket.Download.RunAsync(body, ticket, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(Encoding.UTF8.GetString(body.ToArray()), Is.EqualTo("body\n#complete\n"));
            Assert.That(trailer.Complete, Is.True);
            Assert.That(ticket.State, Is.EqualTo(ExplorerDownloadState.Finished));
            Assert.That(ticket.Trailer, Is.SameAs(trailer));
        });
    }

    [Test]
    public async Task A_body_that_faults_ends_with_a_trailer_that_says_so_without_the_fault_text()
    {
        var (ticket, body) = Start(new ScriptedDownload(_ => throw new InvalidOperationException("secret server detail")));

        var trailer = await ticket.Download.RunAsync(body, ticket, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(trailer.Complete, Is.False);
            Assert.That(trailer.Refused, Is.EqualTo(ExplorerDownloadRefusal.Unknown));
            Assert.That(trailer.Reason, Is.EqualTo("The download stopped because the cluster could not be read."));
            Assert.That(Encoding.UTF8.GetString(body.ToArray()), Does.Not.Contain("secret"));
            Assert.That(ticket.State, Is.EqualTo(ExplorerDownloadState.Finished));
        });
    }

    [Test]
    public async Task A_stopped_body_ends_with_a_trailer_that_says_it_was_stopped()
    {
        using var stop = new CancellationTokenSource();
        var (ticket, body) = Start(new ScriptedDownload(async token =>
        {
            await stop.CancelAsync();
            token.ThrowIfCancellationRequested();
        }));

        var trailer = await ticket.Download.RunAsync(body, ticket, stop.Token);

        Assert.Multiple(() =>
        {
            Assert.That(trailer.Complete, Is.False);
            Assert.That(trailer.Reason, Is.EqualTo("The download was stopped before it finished."));
            Assert.That(Encoding.UTF8.GetString(body.ToArray()), Does.EndWith("#stopped\n"));
        });
    }

    [Test]
    public async Task A_trailer_that_cannot_be_written_still_finishes_the_ticket()
    {
        var ticket = new ExplorerDownloadTicketStore().Mint(new ScriptedDownload(_ => Task.CompletedTask), "b");
        using var closed = new MemoryStream();
        closed.Dispose();

        var trailer = await ticket.Download.RunAsync(closed, ticket, CancellationToken.None);

        Assert.That(ticket.Trailer, Is.SameAs(trailer));
    }

    [Test]
    public void The_trailer_is_written_as_a_json_object_with_every_field()
    {
        var complete = Json(new ExplorerDownloadTrailer("x/1", 3, true, ExplorerDownloadRefusal.None, null));
        var partial = Json(new ExplorerDownloadTrailer("x/1", 2, false, ExplorerDownloadRefusal.Some, "narrowed"));

        Assert.Multiple(() =>
        {
            Assert.That(complete, Is.EqualTo("{\"format\":\"x/1\",\"items\":3,\"complete\":true,\"refused\":\"none\",\"reason\":null}"));
            Assert.That(partial, Is.EqualTo("{\"format\":\"x/1\",\"items\":2,\"complete\":false,\"refused\":\"some\",\"reason\":\"narrowed\"}"));
            Assert.That(new ExplorerDownloadTrailer("x", 0, false, ExplorerDownloadRefusal.All, "r").RefusedText, Is.EqualTo("all"));
            Assert.That(ExplorerDownloadTrailer.Stopped("x", 4, "why").RefusedText, Is.EqualTo("unknown"));
        });
    }

    [Test]
    public void A_listener_fault_never_reaches_the_download()
    {
        var ticket = new ExplorerDownloadTicketStore().Mint(new ScriptedDownload(_ => Task.CompletedTask), "b");
        ticket.Changed += () => throw new InvalidOperationException("listener");

        Assert.DoesNotThrow(() => ticket.Report(1, 2));
        Assert.Multiple(() =>
        {
            Assert.That(ticket.Items, Is.EqualTo(1));
            Assert.That(ticket.Bytes, Is.EqualTo(2));
        });
    }

    private static (ExplorerDownloadTicket Ticket, MemoryStream Body) Start(ExplorerDownload download)
    {
        var store = new ExplorerDownloadTicketStore();
        var ticket = store.Mint(download, "binding");
        store.Redeem(ticket.Token, "binding", out _);
        return (ticket, new MemoryStream());
    }

    private static string Json(ExplorerDownloadTrailer trailer)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
        {
            trailer.WriteJson(writer);
        }

        return Encoding.UTF8.GetString(stream.ToArray());
    }

    private sealed class ScriptedDownload(Func<CancellationToken, Task> afterBody) : ExplorerDownload
    {
        public override string FileName => "scripted.txt";

        public override string ContentType => "text/plain";

        protected override string Format => "scripted/1";

        protected override async Task<ExplorerDownloadTrailer> WriteBodyAsync(Stream body, ExplorerDownloadTicket ticket, CancellationToken cancellationToken)
        {
            await body.WriteAsync("body\n"u8.ToArray(), cancellationToken);
            ticket.Report(1, 5);
            await afterBody(cancellationToken);
            return new ExplorerDownloadTrailer(Format, 1, true, ExplorerDownloadRefusal.None, null);
        }

        protected override async ValueTask WriteTrailerAsync(Stream body, ExplorerDownloadTrailer trailer, CancellationToken cancellationToken) =>
            await body.WriteAsync(trailer.Complete ? "#complete\n"u8.ToArray() : "#stopped\n"u8.ToArray(), cancellationToken);
    }
}
