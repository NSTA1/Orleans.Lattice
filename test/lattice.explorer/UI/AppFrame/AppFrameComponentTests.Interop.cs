using System.Reflection;
using System.Text.Json;
using System.Text.RegularExpressions;
using Bunit;
using Microsoft.JSInterop;
using Orleans.Lattice.Explorer.UI.Framing;
using Orleans.Lattice.Testing.Hygiene;
using static Orleans.Lattice.Explorer.Tests.UI.Framing.AppFrameTestData;

namespace Orleans.Lattice.Explorer.Tests.UI.Framing;

/// <summary>
/// The callback target the host module actually holds, driven the way the module drives
/// it. Every member of <see cref="AppFrameInterop"/> is a one-line delegation, so the
/// only way one can be wrong is in <em>which</em> owner method it forwards to - and a
/// test that calls the owner directly cannot see that. Each test below invokes the real
/// reference the component handed to <c>attach</c> and asserts an outcome unique to the
/// owner method it has to reach, so swapping any two delegations reddens a test.
/// </summary>
public sealed partial class AppFrameComponentTests
{
    [Test]
    public void The_host_module_is_handed_a_reference_to_the_frames_own_interop_target()
    {
        var cut = RenderFrame();

        var reference = _module.Invocations["attach"].Single().Arguments[2];

        Assert.Multiple(() =>
        {
            Assert.That(reference, Is.InstanceOf<DotNetObjectReference<AppFrameInterop>>());
            Assert.That(cut.Instance.Failure, Is.Null);
        });
    }

    [Test]
    public async Task OnFrameReady_delivers_the_bundle_through_the_owning_frame()
    {
        var cut = RenderFrame();
        var interop = Interop();

        await cut.InvokeAsync(() => interop.OnFrameReady(AppFrameProtocol.Version));

        Assert.Multiple(() =>
        {
            Assert.That(_module.Invocations["sendBundle"], Has.Count.EqualTo(1));
            Assert.That(cut.Instance.Failure, Is.Null);
        });
    }

    [Test]
    public async Task OnFrameReady_forwards_the_protocol_it_was_given_rather_than_a_constant()
    {
        var cut = RenderFrame();
        var interop = Interop();

        await cut.InvokeAsync(() => interop.OnFrameReady(AppFrameProtocol.Version + 1));

        AssertFailed(cut, AppFrameFailure.ProtocolUnsupported, "This app needs a newer Explorer");
        Assert.That(_module.Invocations["sendBundle"], Is.Empty, "an unsupported protocol delivers nothing");
    }

    [Test]
    public async Task OnPortMessage_relays_the_message_it_was_given_and_returns_the_reply()
    {
        var cut = RenderFrame();
        var interop = Interop();
        await cut.InvokeAsync(() => interop.OnFrameReady(AppFrameProtocol.Version));
        _bridge.Values["k"] = [42];

        string? reply = null;
        await cut.InvokeAsync(async () =>
            reply = await interop.OnPortMessage("{\"id\":1,\"op\":\"data.read\",\"args\":{\"action\":\"get\",\"tree\":\"orders\",\"key\":\"k\"}}"));

        var root = JsonDocument.Parse(reply!).RootElement;
        Assert.Multiple(() =>
        {
            Assert.That(root.GetProperty("ok").GetBoolean(), Is.True, "the owner's reply is returned, not discarded");
            Assert.That(_bridge.Calls.Single().Target.InstallRevision, Is.EqualTo(Revision));
        });
    }

    [Test]
    public async Task OnFrameFailed_reaches_the_frame_failed_state()
    {
        var cut = RenderFrame();
        var interop = Interop();
        await cut.InvokeAsync(() => interop.OnFrameReady(AppFrameProtocol.Version));

        await cut.InvokeAsync(() => interop.OnFrameFailed("digest_mismatch"));

        AssertFailed(cut, AppFrameFailure.FrameFailed, "The app failed to start");
    }

    [Test]
    public async Task OnFrameReloaded_reaches_the_reloaded_state_and_not_the_frame_failed_one()
    {
        var cut = RenderFrame();
        var interop = Interop();
        await cut.InvokeAsync(() => interop.OnFrameReady(AppFrameProtocol.Version));

        await cut.InvokeAsync(() => interop.OnFrameReloaded());

        // The pair that only a distinct marker separates: both end the frame, with
        // different causes and different words, so a swapped delegation is visible.
        AssertFailed(cut, AppFrameFailure.Reloaded, "The app was closed");
    }

    [Test]
    public async Task OnFrameLoaded_reposts_the_in_app_path_once_the_apps_scripts_are_listening()
    {
        var cut = RenderFrame(parameters => parameters.Add(p => p.Path, "tasks/7"));
        var interop = Interop();
        await cut.InvokeAsync(() => interop.OnFrameReady(AppFrameProtocol.Version));

        await cut.InvokeAsync(() => interop.OnFrameLoaded());
        await cut.InvokeAsync(() => interop.OnFrameLoaded());

        var paths = _module.Invocations["post"]
            .Select(invocation => JsonDocument.Parse((string)invocation.Arguments[1]!).RootElement)
            .Where(message => message.GetProperty("type").GetString() == "nav.changed")
            .Select(message => message.GetProperty("data").GetProperty("path").GetString());
        Assert.That(paths, Is.EqualTo(new[] { "/tasks/7", "/tasks/7" }), "delivery posts it, the first load repeats it, a second repeats nothing");
    }

    [Test]
    public void Every_callback_the_host_module_invokes_by_name_is_a_JSInvokable_member_of_the_target()
    {
        var declared = typeof(AppFrameInterop)
            .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(method => method.GetCustomAttribute<JSInvokableAttribute>() is not null)
            .Select(method => method.Name)
            .Order(StringComparer.Ordinal)
            .ToArray();

        var module = File.ReadAllText(Path.Combine(
            HygieneRepository.FindRepoRoot(),
            "src", "lattice.explorer", "UI", "wwwroot", "appframe", "host.mjs"));
        var invoked = Regex.Matches(module, "'(?<name>On[A-Za-z]+)'", RegexOptions.None, TimeSpan.FromSeconds(5))
            .Select(match => match.Groups["name"].Value)
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .ToArray();

        Assert.Multiple(() =>
        {
            // Nothing else compares these two artefacts: the module calls by string name,
            // so a rename on either side fails silently at run time.
            Assert.That(invoked, Is.Not.Empty, "the scan must actually find the host module's callbacks");
            Assert.That(declared, Is.EqualTo(invoked));
        });
    }

    private AppFrameInterop Interop() =>
        ((DotNetObjectReference<AppFrameInterop>)_module.Invocations["attach"].Single().Arguments[2]!).Value;
}
