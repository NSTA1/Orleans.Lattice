using System.Reflection;
using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

/// <summary>
/// Toasts: a per-circuit queue that never times out, rendered in one polite live
/// region, each toast naming its tone in words and dismissible from the keyboard.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class LtToastTests : ShellDesignTestContext
{
    [Test]
    public void Show_queues_a_toast_with_a_fresh_id_and_announces_the_change()
    {
        var service = new LtToastService();
        var changes = 0;
        service.Changed += () => changes++;

        var first = service.Show("Tree created.", LtToastTone.Success);
        var second = service.Show("Resize started.");

        Assert.Multiple(() =>
        {
            Assert.That(service.Toasts, Is.EqualTo(new[] { first, second }));
            Assert.That(second.Id, Is.GreaterThan(first.Id));
            Assert.That(second.Tone, Is.EqualTo(LtToastTone.Info));
            Assert.That(changes, Is.EqualTo(2));
        });
    }

    [Test]
    public void The_queue_drops_the_oldest_toast_past_its_capacity()
    {
        var service = new LtToastService();
        var shown = Enumerable.Range(1, LtToastService.Capacity + 2).Select(i => service.Show($"Toast {i}")).ToArray();

        Assert.That(service.Toasts, Is.EqualTo(shown[2..]));
    }

    [Test]
    public void Dismiss_removes_one_toast_and_reports_whether_it_did()
    {
        var service = new LtToastService();
        var toast = service.Show("Tree created.");
        var changes = 0;
        service.Changed += () => changes++;

        Assert.Multiple(() =>
        {
            Assert.That(service.Dismiss(toast.Id), Is.True);
            Assert.That(service.Dismiss(toast.Id), Is.False);
            Assert.That(service.Toasts, Is.Empty);
            Assert.That(changes, Is.EqualTo(1), "a dismissal that removed nothing is not a change");
        });
    }

    [Test]
    [TestCase(null)]
    [TestCase("")]
    [TestCase("   ")]
    public void An_empty_message_is_rejected(string? message)
    {
        Assert.That(() => new LtToastService().Show(message!), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void The_region_is_always_present_as_a_polite_named_live_region()
    {
        var region = Render<LtToastRegion>().Find(".lt-toasts");

        Assert.Multiple(() =>
        {
            Assert.That(region.GetAttribute("role"), Is.EqualTo("status"));
            Assert.That(region.GetAttribute("aria-live"), Is.EqualTo("polite"));
            Assert.That(region.GetAttribute("aria-label"), Is.EqualTo("Notifications"));
            Assert.That(region.Children, Is.Empty);
        });
    }

    [Test]
    [TestCase((int)LtToastTone.Info, "info", "Notice")]
    [TestCase((int)LtToastTone.Success, "success", "Done")]
    [TestCase((int)LtToastTone.Warning, "warning", "Warning")]
    [TestCase((int)LtToastTone.Danger, "danger", "Failed")]
    public void A_toast_names_its_tone_in_words_beside_its_message(int tone, string key, string word)
    {
        var cut = Render<LtToastRegion>();
        var service = Services.GetRequiredService<LtToastService>();

        cut.InvokeAsync(() => service.Show("Resize finished.", (LtToastTone)tone));

        var toast = cut.Find(".lt-toast");
        Assert.Multiple(() =>
        {
            Assert.That(toast.GetAttribute("data-lt-tone"), Is.EqualTo(key));
            Assert.That(toast.QuerySelector(".lt-toast__tone")?.TextContent, Is.EqualTo(word));
            Assert.That(toast.QuerySelector(".lt-toast__message")?.TextContent, Is.EqualTo("Resize finished."));
        });
    }

    [Test]
    public void The_dismiss_button_removes_its_toast()
    {
        var service = Services.GetRequiredService<LtToastService>();
        service.Show("Tree created.");
        var cut = Render<LtToastRegion>();
        var dismiss = cut.Find(".lt-toast button");

        Assert.That(dismiss.GetAttribute("aria-label"), Is.EqualTo("Dismiss: Tree created."));

        dismiss.Click();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-toast"), Is.Empty);
            Assert.That(service.Toasts, Is.Empty);
        });
    }

    [Test]
    public void A_disposed_region_stops_listening()
    {
        var service = Services.GetRequiredService<LtToastService>();
        var cut = Render<LtToastRegion>();
        var changed = typeof(LtToastService).GetField(nameof(LtToastService.Changed), BindingFlags.Instance | BindingFlags.NonPublic)!;

        Assert.That(changed.GetValue(service), Is.Not.Null, "a rendered region listens for changes");

        cut.Instance.Dispose();

        Assert.That(changed.GetValue(service), Is.Null, "a disposed region must not keep the circuit's queue pointing at it");
    }
}
