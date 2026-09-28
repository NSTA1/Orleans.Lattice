using Orleans.Lattice.Apps;
using Orleans.Lattice.Explorer.Shell.Design.Components;
using Orleans.Lattice.Explorer.Shell.Framing;
using Orleans.Lattice.Explorer.Shell.Framing.Broker;
using static Orleans.Lattice.Explorer.Tests.Shell.Framing.AppFrameTestData;
using static Orleans.Lattice.Explorer.Tests.Shell.Framing.Broker.BrokerHarness;

namespace Orleans.Lattice.Explorer.Tests.Shell.Framing.Broker;

/// <summary>The operations the broker answers itself: context, navigation and notifications.</summary>
public sealed partial class AppBridgeBrokerTests
{
    [Test]
    public async Task Context_read_reports_the_launch_and_appearance_and_never_an_id()
    {
        var harness = await CreateAsync();
        harness.Host.Appearance = new AppFrameAppearance("board", "more", "compact", true);
        harness.Host.TenantDisplayName = "Contoso";

        var outcome = await harness.SendAsync(1, "context.read");
        var result = AssertOk(outcome, 1);

        Assert.Multiple(() =>
        {
            Assert.That(result.EnumerateObject().Select(property => property.Name), Is.EquivalentTo(new[]
            {
                "slug", "version", "protocol", "theme", "contrast", "density", "reducedMotion", "tenant",
            }));
            Assert.That(result.GetProperty("slug").GetString(), Is.EqualTo(Slug));
            Assert.That(result.GetProperty("version").GetString(), Is.EqualTo(AppFrameTestData.Version));
            Assert.That(result.GetProperty("protocol").GetInt32(), Is.EqualTo(1));
            Assert.That(result.GetProperty("theme").GetString(), Is.EqualTo("board"));
            Assert.That(result.GetProperty("contrast").GetString(), Is.EqualTo("more"));
            Assert.That(result.GetProperty("density").GetString(), Is.EqualTo("compact"));
            Assert.That(result.GetProperty("reducedMotion").GetBoolean(), Is.True);
            Assert.That(result.GetProperty("tenant").GetString(), Is.EqualTo("Contoso"));
            Assert.That(outcome.Reply, Does.Not.Contain("in-image").And.Not.Contain("Revision").And.Not.Contain("a/" + Slug));
        });
    }

    [Test]
    public async Task Context_read_sanitises_an_appearance_outside_the_closed_sets()
    {
        var harness = await CreateAsync();
        harness.Host.Appearance = new AppFrameAppearance("<script>", "x", "y", false);

        var result = AssertOk(await harness.SendAsync(1, "context.read"), 1);

        Assert.Multiple(() =>
        {
            Assert.That(result.GetProperty("theme").GetString(), Is.EqualTo("paper"));
            Assert.That(result.GetProperty("contrast").GetString(), Is.EqualTo("standard"));
            Assert.That(result.GetProperty("density").GetString(), Is.EqualTo("comfortable"));
        });
    }

    [Test]
    public async Task Context_read_without_a_host_context_uses_the_defaults_and_no_tenant()
    {
        var harness = await CreateAsync(withHostContext: false);

        var result = AssertOk(await harness.SendAsync(1, "context.read"), 1);

        Assert.Multiple(() =>
        {
            Assert.That(result.GetProperty("theme").GetString(), Is.EqualTo("paper"));
            Assert.That(result.GetProperty("tenant").ValueKind, Is.EqualTo(System.Text.Json.JsonValueKind.Null));
        });
    }

    [Test]
    public async Task Context_user_returns_only_the_display_name_when_consented()
    {
        var harness = await CreateAsync(Grants(null, AppUiBridgeOperations.ContextUser));
        harness.Host.UserDisplayName = "Ada Lovelace";

        var result = AssertOk(await harness.SendAsync(1, "context.user"), 1);

        Assert.Multiple(() =>
        {
            Assert.That(result.EnumerateObject().Select(property => property.Name), Is.EqualTo(new[] { "displayName" }));
            Assert.That(result.GetProperty("displayName").GetString(), Is.EqualTo("Ada Lovelace"));
        });
    }

    [Test]
    public async Task Context_user_without_a_known_display_name_is_unavailable()
    {
        var harness = await CreateAsync(Grants(null, AppUiBridgeOperations.ContextUser));

        AssertRefused(await harness.SendAsync(1, "context.user"), 1, "unavailable");
    }

    [TestCase("/")]
    [TestCase("/boards/7?tab=done")]
    [TestCase("/caf\u00e9")]
    public async Task Nav_sync_reports_the_path_as_an_effect(string path)
    {
        var harness = await CreateAsync();

        var outcome = await harness.SendAsync(1, "nav.sync", new { path });

        AssertOk(outcome, 1);
        Assert.Multiple(() =>
        {
            Assert.That(outcome.Effect, Is.EqualTo(AppBridgeEffect.NavSync));
            Assert.That(outcome.Argument, Is.EqualTo(path));
        });
    }

    [Test]
    public async Task Ui_notify_renders_text_prefixed_with_the_apps_display_name()
    {
        var harness = await CreateAsync();

        AssertOk(await harness.SendAsync(1, "ui.notify", new { text = "<b>Saved</b>" }), 1);

        var toast = harness.Toasts!.Toasts.Single();
        Assert.Multiple(() =>
        {
            Assert.That(toast.Message, Is.EqualTo(DisplayName + ": <b>Saved</b>"));
            Assert.That(toast.Tone, Is.EqualTo(LtToastTone.Info));
        });
    }

    [Test]
    public async Task Ui_notify_without_a_toast_region_is_unavailable()
    {
        var harness = await CreateAsync(withToasts: false);
        AssertRefused(await harness.SendAsync(1, "ui.notify", new { text = "hi" }), 1, "unavailable");
    }

    [TestCase("hello", 200, true)]
    [TestCase("", 200, false)]
    [TestCase(null, 200, false)]
    [TestCase("abc", 2, false)]
    [TestCase("tab\there", 200, false)]
    [TestCase("\u200fmark", 200, false)]
    [TestCase("\u2066isolate", 200, false)]
    [TestCase("emoji \ud83d\ude00", 200, true)]
    public void IsSafeText_accepts_only_plain_well_formed_text(string? text, int max, bool expected)
    {
        Assert.That(AppBridgeBroker.IsSafeText(text, max), Is.EqualTo(expected));
    }

    [Test]
    public void IsSafeText_refuses_a_lone_surrogate()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppBridgeBroker.IsSafeText("lone " + (char)0xDC00, 200), Is.False);
            Assert.That(AppBridgeBroker.IsSafeText("lone " + (char)0xD800, 200), Is.False);
        });
    }
}
