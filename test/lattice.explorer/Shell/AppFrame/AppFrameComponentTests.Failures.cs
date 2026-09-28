using Bunit;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.Shell.Framing;
using static Orleans.Lattice.Explorer.Tests.Shell.Framing.AppFrameTestData;

namespace Orleans.Lattice.Explorer.Tests.Shell.Framing;

/// <summary>Every failure state: a shell-native error replaces the frame, never partial UI.</summary>
public sealed partial class AppFrameComponentTests
{
    [Test]
    public void No_grant_renders_an_error_and_no_frame()
    {
        _workspace.RevokeAll();
        AppFrameFailure? raised = null;

        var cut = RenderFrame(parameters => parameters.Add(p => p.OnFailure, failure => raised = failure));

        AssertFailed(cut, AppFrameFailure.NoGrant, "This app is not available");
        Assert.Multiple(() =>
        {
            Assert.That(raised, Is.EqualTo(AppFrameFailure.NoGrant));
            Assert.That(_module.Invocations["attach"], Is.Empty, "a refused launch never creates a frame");
            Assert.That(cut.FindAll("[data-appframe-leave]"), Has.Count.EqualTo(2), "the way out stays");
        });
    }

    [Test]
    public void An_app_without_a_ui_renders_no_ui()
    {
        _workspace.RevokeAll();
        _workspace.Grant(Describe() with { Ui = null });

        AssertFailed(RenderFrame(), AppFrameFailure.NoUi, "This app has no interface");
    }

    [Test]
    public async Task A_digest_mismatch_at_delivery_renders_an_error_and_tears_the_frame_down()
    {
        _workspace.Assets[Script] = _workspace.Assets[Script] with { Bytes = new byte[] { 1, 2, 3 } };
        var cut = RenderFrame();

        await ReadyAsync(cut);

        AssertFailed(cut, AppFrameFailure.DigestMismatch, "The app's interface failed verification");
        Assert.Multiple(() =>
        {
            Assert.That(_module.Invocations["stageAsset"], Is.Empty, "no byte reached the frame");
            Assert.That(_module.Invocations["detach"], Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task A_bundle_digest_mismatch_renders_an_error()
    {
        _workspace.RevokeAll();
        _workspace.Grant(Describe(Ui() with { BundleDigest = new string('f', 64) }), Assets());
        var cut = RenderFrame();

        await ReadyAsync(cut);

        AssertFailed(cut, AppFrameFailure.BundleDigestMismatch, "The app's interface failed verification");
    }

    [Test]
    public void The_handshake_timeout_renders_an_error_without_waiting_on_real_time()
    {
        var cut = RenderFrame();
        Assert.That(cut.Instance.Failure, Is.Null);

        cut.InvokeAsync(() => _time.Advance(AppFrame.HandshakeTimeout - TimeSpan.FromTicks(1))).GetAwaiter().GetResult();
        Assert.That(cut.Instance.Failure, Is.Null);

        cut.InvokeAsync(() => _time.Advance(TimeSpan.FromTicks(1))).GetAwaiter().GetResult();

        AssertFailed(cut, AppFrameFailure.HandshakeTimeout, "The app did not start");
        Assert.That(_module.Invocations["detach"], Has.Count.EqualTo(1));
    }

    [Test]
    public async Task A_ready_before_the_timeout_cancels_it()
    {
        var cut = RenderFrame();
        await ReadyAsync(cut);

        await cut.InvokeAsync(() => _time.Advance(AppFrame.HandshakeTimeout * 2));

        Assert.That(cut.Instance.Failure, Is.Null);
    }

    [TestCase(0)]
    [TestCase(2)]
    [TestCase(-1)]
    public async Task A_frame_protocol_the_host_or_the_app_does_not_accept_is_unsupported(long protocol)
    {
        var cut = RenderFrame();

        await ReadyAsync(cut, protocol);

        AssertFailed(cut, AppFrameFailure.ProtocolUnsupported, "This app needs a newer Explorer");
        Assert.That(_module.Invocations["stageAsset"], Is.Empty);
    }

    [Test]
    public void A_minimum_protocol_above_the_hosts_is_refused_before_any_frame()
    {
        _workspace.RevokeAll();
        _workspace.Grant(Describe(Ui() with { MinProtocol = 2 }), Assets());

        var cut = RenderFrame();

        AssertFailed(cut, AppFrameFailure.ProtocolUnsupported, "This app needs a newer Explorer");
        Assert.That(_module.Invocations["attach"], Is.Empty);
    }

    [TestCase("digest_mismatch")]
    [TestCase("<img src=x onerror=alert(1)>")]
    [TestCase(null)]
    public async Task Lattice_failed_renders_an_error(string? code)
    {
        var cut = RenderFrame();
        await ReadyAsync(cut);

        await cut.InvokeAsync(() => cut.Instance.HandleFrameFailedAsync(code));

        AssertFailed(cut, AppFrameFailure.FrameFailed, "The app failed to start");
        Assert.That(cut.Markup, Does.Not.Contain("onerror"));
    }

    [Test]
    public async Task A_second_load_renders_an_error_and_drops_later_port_messages()
    {
        var cut = RenderFrame();
        await ReadyAsync(cut);

        await cut.InvokeAsync(() => cut.Instance.HandleFrameReloadedAsync());
        var reply = await PortAsync(cut, "{\"id\":1,\"op\":\"context.read\",\"args\":{}}");

        AssertFailed(cut, AppFrameFailure.Reloaded, "The app was closed");
        Assert.That(reply, Is.Null);
    }

    [Test]
    public async Task Revocation_on_re_navigation_replaces_the_frame()
    {
        var cut = RenderFrame();
        await ReadyAsync(cut);
        _workspace.Descriptions[Slug] = Describe(revision: Revision + 1);

        cut.Render(parameters => parameters.Add(p => p.Path, "/boards/2"));

        AssertFailed(cut, AppFrameFailure.Revoked, "This app has changed");
        Assert.That(_module.Invocations["revoke"].Single().Arguments[1], Is.EqualTo("revision"));
    }

    [Test]
    public async Task Re_navigation_while_still_current_keeps_the_frame()
    {
        var cut = RenderFrame();
        await ReadyAsync(cut);

        cut.Render(parameters => parameters.Add(p => p.Path, "/boards/2"));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Instance.Failure, Is.Null);
            Assert.That(cut.FindAll("iframe"), Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task A_bridge_revocation_replaces_the_frame()
    {
        var cut = RenderFrame();
        await ReadyAsync(cut);
        _bridge.Throw = new AppBridgeException(AppBridgeFailure.NotFound);
        _workspace.Descriptions[Slug] = Describe() with { State = AppLifecycleState.Disabled };

        var reply = await PortAsync(cut, "{\"id\":1,\"op\":\"data.read\",\"args\":{\"action\":\"get\",\"tree\":\"orders\",\"key\":\"k\"}}");

        AssertFailed(cut, AppFrameFailure.Revoked, "This app has changed");
        Assert.Multiple(() =>
        {
            Assert.That(reply, Is.Null);
            Assert.That(_module.Invocations["revoke"].Single().Arguments[1], Is.EqualTo("disabled"));
        });
    }

    [Test]
    public void A_host_module_that_will_not_attach_is_unavailable()
    {
        _attach.SetResult(false);

        var cut = RenderFrame(attach: false);

        AssertFailed(cut, AppFrameFailure.Unavailable, "The app could not be opened");
    }

    [Test]
    public async Task Switching_apps_reopens_through_the_gate()
    {
        var cut = RenderFrame();
        await ReadyAsync(cut);
        var lists = _workspace.ListCalls;

        cut.Render(parameters => parameters.Add(p => p.AppSlug, "other"));

        Assert.Multiple(() =>
        {
            Assert.That(_workspace.ListCalls, Is.EqualTo(lists + 1));
            Assert.That(cut.Instance.Failure, Is.EqualTo(AppFrameFailure.NoGrant));
            Assert.That(_module.Invocations["detach"], Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task Dispose_closes_the_frame()
    {
        var cut = RenderFrame();
        await ReadyAsync(cut);

        await cut.InvokeAsync(async () => await cut.Instance.DisposeAsync());

        Assert.That(_module.Invocations["detach"], Has.Count.EqualTo(1));
    }

    private static void AssertFailed(IRenderedComponent<AppFrame> cut, AppFrameFailure failure, string title)
    {
        Assert.Multiple(() =>
        {
            Assert.That(cut.Instance.Failure, Is.EqualTo(failure));
            Assert.That(cut.FindAll("iframe"), Is.Empty, "a failure never leaves partial UI");
            Assert.That(cut.Find("[role=alert] .lt-empty__title").TextContent, Is.EqualTo(title));
        });
    }
}
