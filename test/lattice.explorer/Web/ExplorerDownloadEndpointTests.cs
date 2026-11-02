using System.Net;
using System.Text;
using Microsoft.AspNetCore.Antiforgery;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Tests.UI.Downloads;
using Orleans.Lattice.Explorer.UI.Downloads;
using Orleans.Lattice.Explorer.Web;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests.Web;

/// <summary>
/// The download endpoint (issue #5366): a valid ticket presented with its circuit's
/// binding and an antiforgery token streams its download as an attachment; a reused,
/// expired, unknown or wrongly bound token is refused with the same empty 204; and a
/// post without antiforgery is a 400 that spends nothing.
/// </summary>
[TestFixture]
[FastInProcessHostFixture("Builds a WebApplication on the test server with no external dependency; measured at about 1 second for the fixture.")]
public sealed class ExplorerDownloadEndpointTests
{
    private const string AntiforgeryField = "__RequestVerificationToken";
    private const string Binding = "circuit-binding";

    private ManualClock _time = null!;
    private ExplorerDownloadTicketStore _store = null!;
    private WebApplication _app = null!;
    private HttpClient _client = null!;

    [SetUp]
    public async Task SetUpAsync()
    {
        _time = new ManualClock();
        _store = new ExplorerDownloadTicketStore(_time);

        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddDataProtection();
        builder.Services.AddAntiforgery();
        builder.Services.AddRouting();
        builder.Services.AddSingleton(_store);

        _app = builder.Build();
        _app.UseRouting();
        _app.UseAntiforgery();
        _app.MapGet("/test/token", (HttpContext context, IAntiforgery antiforgery) =>
            Results.Text(antiforgery.GetAndStoreTokens(context).RequestToken ?? string.Empty));
        _app.MapExplorerDownloads();
        await _app.StartAsync();

        _client = new HttpClient(new CookieJar { InnerHandler = _app.GetTestServer().CreateHandler() })
        {
            BaseAddress = new Uri("http://localhost/"),
        };
    }

    [TearDown]
    public async Task TearDownAsync()
    {
        _client.Dispose();
        await _app.DisposeAsync();
    }

    [Test]
    public async Task A_valid_ticket_streams_its_download_as_an_attachment_and_finishes_the_ticket()
    {
        var ticket = _store.Mint(new TextDownload("a\nb\n"), Binding);

        using var response = await PostAsync(ticket.Token, Binding);

        Assert.Multiple(async () =>
        {
            Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.OK));
            Assert.That(response.Content.Headers.ContentType!.MediaType, Is.EqualTo("text/plain"));
            Assert.That(response.Content.Headers.ContentDisposition!.DispositionType, Is.EqualTo("attachment"));
            Assert.That(response.Content.Headers.ContentDisposition.FileNameStar, Is.EqualTo("rows.txt"));
            Assert.That(response.Headers.CacheControl!.NoStore, Is.True);
            Assert.That(await response.Content.ReadAsStringAsync(), Is.EqualTo("a\nb\n#end 2\n"));
            Assert.That(ticket.State, Is.EqualTo(ExplorerDownloadState.Finished));
            Assert.That(ticket.Trailer!.Complete, Is.True);
        });
    }

    [Test]
    public async Task A_reused_token_is_refused()
    {
        var ticket = _store.Mint(new TextDownload("a\n"), Binding);
        using (await PostAsync(ticket.Token, Binding))
        {
        }

        using var second = await PostAsync(ticket.Token, Binding);

        Assert.Multiple(async () =>
        {
            Assert.That(second.StatusCode, Is.EqualTo(HttpStatusCode.NoContent));
            Assert.That(await second.Content.ReadAsStringAsync(), Is.Empty);
        });
    }

    [Test]
    public async Task An_expired_token_is_refused_and_the_ticket_reads_expired()
    {
        var ticket = _store.Mint(new TextDownload("a\n"), Binding);
        _time.Advance(ExplorerDownloadTicketStore.DefaultLifetime);

        using var response = await PostAsync(ticket.Token, Binding);

        Assert.Multiple(() =>
        {
            Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.NoContent));
            Assert.That(ticket.State, Is.EqualTo(ExplorerDownloadState.Expired));
        });
    }

    [Test]
    public async Task A_token_presented_by_another_circuit_is_refused_and_burned()
    {
        var ticket = _store.Mint(new TextDownload("a\n"), Binding);

        using var stranger = await PostAsync(ticket.Token, "another-circuit");
        using var owner = await PostAsync(ticket.Token, Binding);

        Assert.Multiple(() =>
        {
            Assert.That(stranger.StatusCode, Is.EqualTo(HttpStatusCode.NoContent));
            Assert.That(owner.StatusCode, Is.EqualTo(HttpStatusCode.NoContent), "a token presented by the wrong circuit is spent");
            Assert.That(ticket.State, Is.EqualTo(ExplorerDownloadState.Refused));
        });
    }

    [Test]
    public async Task An_unknown_token_and_a_missing_form_are_refused_alike()
    {
        using var unknown = await PostAsync("no-such-token", Binding);
        using var bare = await _client.PostAsync(ExplorerDownloadEndpoints.Route, new StringContent("x"));

        Assert.Multiple(() =>
        {
            Assert.That(unknown.StatusCode, Is.EqualTo(HttpStatusCode.NoContent));
            Assert.That(bare.StatusCode, Is.EqualTo(HttpStatusCode.NoContent));
        });
    }

    [Test]
    public async Task A_post_without_antiforgery_is_rejected_and_spends_nothing()
    {
        var ticket = _store.Mint(new TextDownload("a\n"), Binding);

        using var forged = await _client.PostAsync(ExplorerDownloadEndpoints.Route, new FormUrlEncodedContent(new Dictionary<string, string>
        {
            [ExplorerDownloads.TokenField] = ticket.Token,
            [ExplorerDownloads.BindingField] = Binding,
        }));

        Assert.Multiple(() =>
        {
            Assert.That(forged.StatusCode, Is.EqualTo(HttpStatusCode.BadRequest));
            Assert.That(ticket.State, Is.EqualTo(ExplorerDownloadState.Ready));
            Assert.That(_store.Outstanding, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_withdrawn_running_download_ends_with_a_trailer_that_says_so()
    {
        var download = new GatedDownload();
        var ticket = _store.Mint(download, Binding);

        var post = PostAsync(ticket.Token, Binding);
        await download.Started.Task.WaitAsync(TimeSpan.FromSeconds(30));
        _store.Revoke(ticket);
        using var response = await post;

        Assert.Multiple(async () =>
        {
            Assert.That(await response.Content.ReadAsStringAsync(), Is.EqualTo("started\n#stopped The download was stopped before it finished.\n"));
            Assert.That(ticket.Trailer!.Complete, Is.False);
        });
    }

    private async Task<HttpResponseMessage> PostAsync(string token, string binding)
    {
        var antiforgery = await _client.GetStringAsync("/test/token");
        return await _client.PostAsync(ExplorerDownloadEndpoints.Route, new FormUrlEncodedContent(new Dictionary<string, string>
        {
            [AntiforgeryField] = antiforgery,
            [ExplorerDownloads.TokenField] = token,
            [ExplorerDownloads.BindingField] = binding,
        }));
    }

    private sealed class TextDownload(string body) : ExplorerDownload
    {
        public override string FileName => "rows.txt";

        public override string ContentType => "text/plain";

        protected override string Format => "test/1";

        protected override async Task<ExplorerDownloadTrailer> WriteBodyAsync(Stream stream, ExplorerDownloadTicket ticket, CancellationToken cancellationToken)
        {
            await stream.WriteAsync(Encoding.UTF8.GetBytes(body), cancellationToken);
            var lines = body.Count(c => c == '\n');
            ticket.Report(lines, body.Length);
            return new ExplorerDownloadTrailer(Format, lines, true, ExplorerDownloadRefusal.None, null);
        }

        protected override async ValueTask WriteTrailerAsync(Stream stream, ExplorerDownloadTrailer trailer, CancellationToken cancellationToken) =>
            await stream.WriteAsync(Encoding.UTF8.GetBytes(trailer.Complete ? $"#end {trailer.Items}\n" : $"#stopped {trailer.Reason}\n"), cancellationToken);
    }

    private sealed class GatedDownload : ExplorerDownload
    {
        public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override string FileName => "gated.txt";

        public override string ContentType => "text/plain";

        protected override string Format => "test/1";

        protected override async Task<ExplorerDownloadTrailer> WriteBodyAsync(Stream stream, ExplorerDownloadTicket ticket, CancellationToken cancellationToken)
        {
            await stream.WriteAsync("started\n"u8.ToArray(), cancellationToken);
            await stream.FlushAsync(cancellationToken);
            Started.SetResult();
            await Task.Delay(Timeout.Infinite, cancellationToken);
            throw new InvalidOperationException("unreachable");
        }

        protected override async ValueTask WriteTrailerAsync(Stream stream, ExplorerDownloadTrailer trailer, CancellationToken cancellationToken) =>
            await stream.WriteAsync(Encoding.UTF8.GetBytes(trailer.Complete ? "#end\n" : $"#stopped {trailer.Reason}\n"), cancellationToken);
    }

    private sealed class CookieJar : DelegatingHandler
    {
        private readonly CookieContainer _cookies = new();

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var uri = request.RequestUri!;
            var header = _cookies.GetCookieHeader(uri);
            if (!string.IsNullOrEmpty(header))
            {
                request.Headers.Add("Cookie", header);
            }

            var response = await base.SendAsync(request, cancellationToken);
            if (response.Headers.TryGetValues("Set-Cookie", out var values))
            {
                foreach (var value in values)
                {
                    _cookies.SetCookies(uri, value);
                }
            }

            return response;
        }
    }
}
