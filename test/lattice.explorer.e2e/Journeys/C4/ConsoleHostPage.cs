using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Browser;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C4;

/// <summary>
/// One persona's console as its deployment serves it (C4, #5392): a session opened on a
/// head's public base address - the east console's gateway path <c>/console/</c>, or the
/// kiosk head's root - with what a hosting proof reads from it: which replica served a
/// document, whether anything on the page escapes the base path, the head's own files, and the
/// endpoint the head was seeded with.
/// </summary>
internal sealed class ConsoleHostPage : IAsyncDisposable
{
    private const string BasePathAuditScript =
        """
        (prefix) => {
          const origin = location.origin;
          const root = prefix.replace(/\/$/, '');
          const inside = (path) => prefix === '/' || path === root || path.startsWith(prefix);
          const escapes = [];
          const check = (kind, raw) => {
            if (!raw) { return; }
            let url;
            try { url = new URL(raw, document.baseURI); } catch (e) { escapes.push(kind + ' ' + raw + ' (unparsable)'); return; }
            if ((url.protocol === 'http:' || url.protocol === 'https:') && url.origin === origin && !inside(url.pathname)) {
              escapes.push(kind + ' ' + raw);
            }
          };
          for (const a of document.querySelectorAll('a[href]')) { check('a', a.getAttribute('href')); }
          for (const l of document.querySelectorAll('link[href]')) { check('link', l.getAttribute('href')); }
          for (const s of document.querySelectorAll('script[src]')) { check('script', s.getAttribute('src')); }
          for (const i of document.querySelectorAll('img[src], iframe[src], source[src]')) { check(i.localName, i.getAttribute('src')); }
          for (const f of document.querySelectorAll('form[action]')) { check('form', f.getAttribute('action')); }
          const failed = [];
          let resources = 0;
          for (const entry of performance.getEntriesByType('resource')) {
            let url;
            try { url = new URL(entry.name); } catch (e) { continue; }
            if (url.origin !== origin) { continue; }
            resources++;
            if (!inside(url.pathname)) { escapes.push('resource ' + url.pathname); }
            const status = entry.responseStatus || 0;
            if (status >= 400) { failed.push(url.pathname + ' (' + status + ')'); }
          }
          return { baseUri: document.baseURI, escapes, failed, resources };
        }
        """;

    private ConsoleHostPage(PersonaSession session, Uri baseUri)
    {
        Session = session;
        BaseUri = baseUri;
    }

    /// <summary>The persona's session.</summary>
    public PersonaSession Session { get; }

    /// <summary>The head's public base address, ending in <c>/</c>.</summary>
    public Uri BaseUri { get; }

    /// <summary>The base path, such as <c>/console/</c> or <c>/</c>.</summary>
    public string BasePath => BaseUri.AbsolutePath;

    /// <summary>The session's page.</summary>
    public IPage Page => Session.Page;

    /// <summary>The Explorer's chrome on the page.</summary>
    public ShellPage Shell => Session.Shell;

    /// <summary>
    /// Opens <paramref name="persona"/>'s session, signed out, at <paramref name="baseUri"/>: a
    /// head's public address, which for the east console is the gateway's <c>/console/</c>
    /// rather than any one replica.
    /// </summary>
    /// <param name="estate">The estate.</param>
    /// <param name="persona">The persona.</param>
    /// <param name="baseUri">The head's public base address.</param>
    /// <param name="region">The head's region.</param>
    public static async Task<ConsoleHostPage> OpenAsync(JourneyEstate estate, Persona persona, Uri baseUri, string region)
    {
        ArgumentNullException.ThrowIfNull(estate);
        ArgumentNullException.ThrowIfNull(persona);
        ArgumentNullException.ThrowIfNull(baseUri);
        var session = await PersonaSession.OpenAsync(baseUri, persona, region, new PersonaSessionOptions(), estate.HostFaults, estate.ArtifactsDirectory);
        return new ConsoleHostPage(session, baseUri);
    }

    /// <summary>Reloads the current document, as a person pressing reload does, and waits for its live circuit.</summary>
    /// <returns>The document's response.</returns>
    public async Task<IResponse> ReloadAsync()
    {
        await Page.EvaluateAsync("() => { window.__ltLeaving = true; }");
        var response = await Page.ReloadAsync() ?? throw new InvalidOperationException("The reload returned no response.");
        await JourneyBrowser.WaitForInteractiveAsync(Page);
        return response;
    }

    /// <summary>Closes the console: the tab leaves it for a blank page, ending its circuit cleanly.</summary>
    public async Task LeaveAsync()
    {
        await Page.EvaluateAsync("() => { window.__ltLeaving = true; }");
        await Page.GotoAsync("about:blank");
    }

    /// <summary>Opens the console again at its base address, in the same browser (its cookies kept).</summary>
    /// <returns>The document's response.</returns>
    public async Task<IResponse> ReopenAsync()
    {
        var response = await Page.GotoAsync(BaseUri.ToString()) ?? throw new InvalidOperationException("Opening the console returned no response.");
        await JourneyBrowser.WaitForInteractiveAsync(Page);
        return response;
    }

    /// <summary>The console replica that served <paramref name="response"/>, from the harness's replica header.</summary>
    /// <param name="response">A document response from the east console.</param>
    public static async Task<int> ReplicaOfAsync(IResponse response)
    {
        ArgumentNullException.ThrowIfNull(response);
        var header = await response.HeaderValueAsync(JourneyEstate.ReplicaHeader);
        return JourneyEstate.ReplicaIndexOf(header)
            ?? throw new InvalidOperationException($"The console's response named no replica ({JourneyEstate.ReplicaHeader}: '{header}').");
    }

    /// <summary>
    /// Asserts nothing the current document links to, loads, or posts to leaves the base path,
    /// that the document's base address is the head's, and that every file it loaded from the
    /// head answered.
    /// </summary>
    /// <param name="what">What the page is, for the failure message.</param>
    /// <returns>How many of the head's own files the document has loaded.</returns>
    public async Task<int> ExpectNothingEscapesBasePathAsync(string what)
    {
        var audit = await Page.EvaluateAsync<BasePathAudit>(BasePathAuditScript, BasePath);
        Assert.Multiple(() =>
        {
            Assert.That(audit.BaseUri, Is.EqualTo(BaseUri.ToString()), $"{what}: the document's base address is the console's");
            Assert.That(audit.Escapes, Is.Empty, $"{what}: links, files and form posts that leave {BasePath}");
            Assert.That(audit.Failed, Is.Empty, $"{what}: files under {BasePath} that did not resolve");
        });
        return audit.Resources;
    }

    /// <summary>Asserts the page's address is under the base path.</summary>
    public Task ExpectUnderBasePathAsync() =>
        Expect(Page).ToHaveURLAsync(new Regex("^" + Regex.Escape(BaseUri.ToString())));

    /// <summary>Requests one of the head's files at <paramref name="relative"/>, with the session's cookies, as the browser would.</summary>
    /// <param name="relative">The path relative to the base address, with no leading slash.</param>
    public Task<IAPIResponse> RequestAsync(string relative) =>
        Session.Context.APIRequest.GetAsync(new Uri(BaseUri, relative).ToString());

    /// <summary>
    /// The endpoint the connection indicator names: in the header, or in the <b>Menu</b> sheet's
    /// Connection section when compact.
    /// </summary>
    public async Task<string> ReadEndpointAsync()
    {
        var endpoint = new Regex("^https?://");
        if (await Shell.IsCompactAsync())
        {
            await Shell.OpenMenuAsync();
            var value = Shell.MenuSheet.GetByRole(AriaRole.Region, new() { Name = "Connection" }).GetByText(endpoint).First;
            await Expect(value).ToBeVisibleAsync();
            var text = (await value.TextContentAsync())?.Trim() ?? string.Empty;
            await Shell.CloseMenuAsync();
            return text;
        }

        var shown = Shell.Banner.Locator("code").Filter(new() { HasTextRegex = endpoint }).First;
        await Expect(shown).ToBeVisibleAsync();
        return (await shown.TextContentAsync())?.Trim() ?? string.Empty;
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync() => Session.DisposeAsync();

    private sealed class BasePathAudit
    {
        public string BaseUri { get; set; } = string.Empty;

        public string[] Escapes { get; set; } = [];

        public string[] Failed { get; set; } = [];

        public int Resources { get; set; }
    }
}
