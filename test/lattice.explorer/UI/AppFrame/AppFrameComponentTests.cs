using System.Text.Json;
using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI;
using Orleans.Lattice.Explorer.UI.Framing;
using Orleans.Lattice.Explorer.Tests.UI.Design;
using static Orleans.Lattice.Explorer.Tests.UI.Framing.AppFrameTestData;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Framing;

/// <summary>
/// The frame component under bUnit, over fakes and a scripted host module: the exact frame
/// attribute set, the handshake, verified delivery, relaying, and every failure state. The
/// handshake timeout runs on a <see cref="ManualTimeProvider"/>, so nothing waits on time.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed partial class AppFrameComponentTests : ShellDesignTestContext
{
    private FakeAppWorkspace _workspace = null!;
    private FakeAppBridge _bridge = null!;
    private ManualTimeProvider _time = null!;
    private BunitJSModuleInterop _module = null!;
    private JSRuntimeInvocationHandler<bool> _attach = null!;

    [SetUp]
    public void Arrange()
    {
        _workspace = Workspace();
        _bridge = new FakeAppBridge();
        _time = new ManualTimeProvider();
        Services.AddKeyedSingleton<ILatticeAppWorkspace>(ShellFacades.Key, (_, _) => _workspace);
        Services.AddKeyedSingleton<ILatticeAppBridge>(ShellFacades.Key, _bridge);
        Services.AddSingleton<TimeProvider>(_time);
        Services.AddLogging();
        Services.AddLatticeExplorerShell();

        _module = JSInterop.SetupModule(AppFrameAssets.HostModuleImport);
        _attach = _module.Setup<bool>("attach", _ => true);
        foreach (var method in new[] { "stageAsset", "sendBundle", "post", "revoke", "detach", "focusAddressLine" })
        {
            _module.Setup<bool>(method, _ => true).SetResult(true);
        }
    }

    [Test]
    public void The_frame_carries_exactly_the_sandbox_referrer_title_and_src_attributes()
    {
        var cut = RenderFrame();
        var frame = cut.Find("iframe");

        var names = frame.Attributes
            .Select(attribute => attribute.Name)
            .Where(name => !name.StartsWith("blazor:", StringComparison.Ordinal))
            .Order(StringComparer.Ordinal);

        Assert.Multiple(() =>
        {
            Assert.That(names, Is.EqualTo(new[] { "referrerpolicy", "sandbox", "src", "title" }));
            Assert.That(frame.GetAttribute("sandbox"), Is.EqualTo("allow-scripts"));
            Assert.That(frame.GetAttribute("referrerpolicy"), Is.EqualTo("no-referrer"));
            Assert.That(frame.GetAttribute("title"), Is.EqualTo(DisplayName));
            Assert.That(frame.GetAttribute("src"), Is.EqualTo("/_apps/frame/v1/frame.html"));
            Assert.That(frame.HasAttribute("allow"), Is.False);
        });
    }

    [Test]
    public void The_bootstrap_src_is_rendered_only_after_the_host_module_attached()
    {
        var cut = RenderFrame(attach: false);

        Assert.That(cut.Find("iframe").HasAttribute("src"), Is.False, "no src before the listeners exist");

        cut.InvokeAsync(() => _attach.SetResult(true)).GetAwaiter().GetResult();

        // Waits on the component's next render, not on a clock.
        cut.WaitForAssertion(() => Assert.That(cut.Find("iframe").GetAttribute("src"), Is.EqualTo("/_apps/frame/v1/frame.html")));
    }

    [Test]
    public void The_attach_call_passes_the_frame_and_one_callback_reference()
    {
        RenderFrame();

        var attach = _module.Invocations["attach"].Single();
        Assert.Multiple(() =>
        {
            Assert.That(attach.Arguments, Has.Count.EqualTo(3));
            Assert.That(attach.Arguments[0], Is.InstanceOf<string>());
            Assert.That(attach.Arguments[1], Is.InstanceOf<ElementReference>());
            Assert.That(attach.Arguments[2]?.GetType().Name, Does.StartWith("DotNetObjectReference"));
        });
    }

    [Test]
    public void Leave_app_controls_sit_before_and_after_the_frame()
    {
        var cut = RenderFrame();
        var section = cut.Find("section.appframe");
        var children = section.Children.ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(children.First().QuerySelector("[data-appframe-leave=before]")?.TextContent, Is.EqualTo("Leave app"));
            Assert.That(children.Last().QuerySelector("[data-appframe-leave=after]")?.TextContent, Is.EqualTo("Leave app"));
            Assert.That(section.GetAttribute("aria-label"), Is.EqualTo(DisplayName));
            Assert.That(cut.FindAll("link[rel=stylesheet]").Single().GetAttribute("href"), Is.EqualTo(AppFrameAssets.Stylesheet));
        });
    }

    [Test]
    public void Leave_app_invokes_the_callback_when_one_is_bound()
    {
        var left = 0;
        var cut = RenderFrame(parameters => parameters.Add(p => p.OnLeave, () => left++));

        cut.Find("[data-appframe-leave=after]").Click();

        Assert.That(left, Is.EqualTo(1));
    }

    [Test]
    public void Leave_app_navigates_to_the_leave_href_when_no_callback_is_bound()
    {
        var cut = RenderFrame(parameters => parameters.Add(p => p.LeaveHref, "apps/taskboard"));

        cut.Find("[data-appframe-leave=before]").Click();

        Assert.That(Services.GetRequiredService<NavigationManager>().Uri, Does.EndWith("/apps/taskboard"));
    }

    [Test]
    public void No_new_window_link_is_offered_unless_one_is_given()
    {
        var cut = RenderFrame();

        Assert.That(cut.FindAll("[data-appframe-window]"), Is.Empty);
    }

    [Test]
    public void The_new_window_link_opens_its_target_with_no_opener_and_no_referrer()
    {
        var cut = RenderFrame(parameters => parameters.Add(p => p.WindowHref, "apps/taskboard/window"));

        var link = cut.Find(".appframe__bar:first-child a[data-appframe-window]");

        Assert.Multiple(() =>
        {
            Assert.That(link.TextContent, Is.EqualTo("Open in new window"));
            Assert.That(link.GetAttribute("href"), Is.EqualTo("apps/taskboard/window"));
            Assert.That(link.GetAttribute("target"), Is.EqualTo("_blank"));
            Assert.That(link.GetAttribute("rel")!.Split(' '), Is.EquivalentTo(new[] { "noopener", "noreferrer" }));
        });
    }

    [Test]
    public void Escape_in_the_host_chrome_returns_focus_to_the_address_line()
    {
        var cut = RenderFrame();

        cut.Find("section.appframe").KeyDown("Escape");

        Assert.That(_module.Invocations["focusAddressLine"], Has.Count.EqualTo(1));
    }

    [Test]
    public void Escape_invokes_the_callback_when_one_is_bound_and_other_keys_do_nothing()
    {
        var escaped = 0;
        var cut = RenderFrame(parameters => parameters.Add(p => p.OnEscape, () => escaped++));

        cut.Find("section.appframe").KeyDown("Enter");
        cut.Find("section.appframe").KeyDown("Escape");

        Assert.Multiple(() =>
        {
            Assert.That(escaped, Is.EqualTo(1));
            Assert.That(_module.Invocations["focusAddressLine"], Is.Empty);
        });
    }

    [Test]
    public async Task The_handshake_delivers_the_verified_bundle_over_the_port()
    {
        var cut = RenderFrame();

        await ReadyAsync(cut);

        var staged = _module.Invocations["stageAsset"];
        var bundle = JsonDocument.Parse((string)_module.Invocations["sendBundle"].Single().Arguments[1]!).RootElement;
        Assert.Multiple(() =>
        {
            Assert.That(staged.Select(invocation => invocation.Arguments[1]), Is.EqualTo(new[] { Entry, Style, Script }));
            Assert.That(bundle.GetProperty("type").GetString(), Is.EqualTo("lattice.bundle"));
            Assert.That(bundle.GetProperty("protocol").GetInt32(), Is.EqualTo(1));
            Assert.That(bundle.GetProperty("appearance").GetProperty("theme").GetString(), Is.EqualTo("paper"));
            var body = bundle.GetProperty("bundle");
            Assert.That(body.GetProperty("entry").GetString(), Is.EqualTo(Entry));
            Assert.That(body.GetProperty("styles")[0].GetString(), Is.EqualTo(Style));
            Assert.That(body.GetProperty("scripts")[0].GetProperty("path").GetString(), Is.EqualTo(Script));
            Assert.That(body.GetProperty("scripts")[0].GetProperty("module").GetBoolean(), Is.True);
            Assert.That(body.GetProperty("bundleDigest").GetString(), Is.EqualTo(Ui().BundleDigest));
            Assert.That(body.GetProperty("assets").GetProperty(Script).GetProperty("digest").GetString(), Is.EqualTo(Sha(ScriptBytes)));
            Assert.That(body.GetProperty("assets").GetProperty(Script).GetProperty("mediaType").GetString(), Is.EqualTo("text/javascript"));
            Assert.That(cut.Instance.Failure, Is.Null);
        });
    }

    [Test]
    public async Task A_second_ready_is_ignored()
    {
        var cut = RenderFrame();
        await ReadyAsync(cut);

        await ReadyAsync(cut);

        Assert.That(_module.Invocations["sendBundle"], Has.Count.EqualTo(1));
    }

    [Test]
    public async Task Port_messages_are_relayed_through_the_broker_and_answered()
    {
        var cut = RenderFrame();
        await ReadyAsync(cut);
        _bridge.Values["k"] = [42];

        var reply = await PortAsync(cut, "{\"id\":1,\"op\":\"data.read\",\"args\":{\"action\":\"get\",\"tree\":\"orders\",\"key\":\"k\"}}");

        var root = JsonDocument.Parse(reply!).RootElement;
        Assert.Multiple(() =>
        {
            Assert.That(root.GetProperty("ok").GetBoolean(), Is.True);
            Assert.That(_bridge.Calls.Single().Target.InstallRevision, Is.EqualTo(Revision));
        });
    }

    [Test]
    public async Task A_port_message_before_the_bundle_is_delivered_is_dropped()
    {
        var cut = RenderFrame();

        var reply = await PortAsync(cut, "{\"id\":1,\"op\":\"context.read\",\"args\":{}}");

        Assert.That(reply, Is.Null);
    }

    [Test]
    public async Task Nav_sync_raises_the_callback()
    {
        string? synced = null;
        var cut = RenderFrame(parameters => parameters.Add(p => p.OnNavSync, path => synced = path));
        await ReadyAsync(cut);

        await PortAsync(cut, "{\"id\":1,\"op\":\"nav.sync\",\"args\":{\"path\":\"/boards/2\"}}");

        Assert.That(synced, Is.EqualTo("/boards/2"));
    }

    [Test]
    public async Task The_address_path_is_sent_as_nav_changed_after_delivery_and_on_change()
    {
        var cut = RenderFrame(parameters => parameters.Add(p => p.Path, "boards/1"));
        await ReadyAsync(cut);

        cut.Render(parameters => parameters.Add(p => p.Path, "/boards/2"));

        var events = _module.Invocations["post"].Select(invocation => JsonDocument.Parse((string)invocation.Arguments[1]!).RootElement).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(events.Select(e => e.GetProperty("type").GetString()), Is.EqualTo(new[] { "nav.changed", "nav.changed" }));
            Assert.That(events.Select(e => e.GetProperty("data").GetProperty("path").GetString()), Is.EqualTo(new[] { "/boards/1", "/boards/2" }));
        });
    }

    [Test]
    public async Task The_address_path_is_posted_again_once_the_frame_has_loaded_its_scripts()
    {
        var cut = RenderFrame(parameters => parameters.Add(p => p.Path, "tasks/7"));
        await ReadyAsync(cut);

        // The first post happens at delivery, before any app script could listen; the
        // frame's first "loaded" repeats it, and a second "loaded" repeats nothing.
        await cut.InvokeAsync(() => cut.Instance.HandleFrameLoadedAsync());
        await cut.InvokeAsync(() => cut.Instance.HandleFrameLoadedAsync());

        var paths = _module.Invocations["post"]
            .Select(invocation => JsonDocument.Parse((string)invocation.Arguments[1]!).RootElement)
            .Where(message => message.GetProperty("type").GetString() == "nav.changed")
            .Select(message => message.GetProperty("data").GetProperty("path").GetString());
        Assert.That(paths, Is.EqualTo(new[] { "/tasks/7", "/tasks/7" }));
    }

    [Test]
    public async Task NotifyContextChangedAsync_posts_the_appearance_to_a_running_frame_only()
    {
        var cut = RenderFrame();
        await cut.InvokeAsync(() => cut.Instance.NotifyContextChangedAsync());
        Assert.That(_module.Invocations["post"], Is.Empty);

        await ReadyAsync(cut);
        await cut.InvokeAsync(() => cut.Instance.NotifyContextChangedAsync());

        var message = JsonDocument.Parse((string)_module.Invocations["post"].Single().Arguments[1]!).RootElement;
        Assert.That(message.GetProperty("type").GetString(), Is.EqualTo("context.changed"));
    }

    private IRenderedComponent<AppFrame> RenderFrame(Action<ComponentParameterCollectionBuilder<AppFrame>>? configure = null, bool attach = true)
    {
        if (attach)
        {
            _attach.SetResult(true);
        }

        return Render<AppFrame>(parameters =>
        {
            parameters.Add(p => p.AppSlug, Slug);
            configure?.Invoke(parameters);
        });
    }

    private static Task ReadyAsync(IRenderedComponent<AppFrame> cut, long protocol = 1) =>
        cut.InvokeAsync(() => cut.Instance.HandleFrameReadyAsync(protocol));

    private static async Task<string?> PortAsync(IRenderedComponent<AppFrame> cut, string message)
    {
        string? reply = null;
        await cut.InvokeAsync(async () => reply = await cut.Instance.HandlePortMessageAsync(message));
        return reply;
    }
}
