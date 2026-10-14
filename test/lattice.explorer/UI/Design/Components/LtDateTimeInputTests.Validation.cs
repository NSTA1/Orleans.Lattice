using Bunit;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

public sealed partial class LtDateTimeInputTests
{
    [TestCase("14:05:07")]
    [TestCase("September 28")]
    [TestCase("2026-09")]
    public async Task A_typed_instant_without_a_complete_date_is_refused(string text)
    {
        var raised = 0;
        var cut = RenderField(p => p.Add(x => x.ValueChanged, (DateTimeOffset? _) => raised++));

        cut.Find("input.lt-datetime__input").Input(text);
        var accepted = await cut.InvokeAsync(() => cut.Instance.ConfirmAsync());

        Assert.Multiple(() =>
        {
            Assert.That(accepted, Is.False);
            Assert.That(raised, Is.Zero);
            Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("Write a time such as"));
        });
    }

    [TestCase(null)]
    [TestCase("Latest")]
    public async Task Clearing_the_picker_time_refuses_submission_until_a_time_is_chosen(string? emptyText)
    {
        var raised = new List<DateTimeOffset?>();
        var cut = RenderField(p => p
            .Add(x => x.Value, Now)
            .Add(x => x.EmptyText, emptyText)
            .Add(x => x.ValueChanged, (DateTimeOffset? value) => raised.Add(value)));
        cut.Find(".lt-datetime__toggle").Click();

        cut.Find(".lt-datetime__time input").Change(string.Empty);
        var accepted = await cut.InvokeAsync(() => cut.Instance.ConfirmAsync());

        Assert.Multiple(() =>
        {
            Assert.That(accepted, Is.False);
            Assert.That(raised, Is.Empty);
            Assert.That(cut.Instance.Value, Is.EqualTo(Now));
            Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("Choose a time in UTC."));
            Assert.That(cut.Find(".lt-datetime__time input").GetAttribute("value"), Is.Empty);
            Assert.That(cut.Find(".lt-datetime__time input").GetAttribute("aria-invalid"), Is.EqualTo("true"));
            Assert.That(cut.Find(".lt-datetime__time input").GetAttribute("aria-describedby"),
                Is.EqualTo(cut.Find(".lt-field__error").Id));
        });

        cut.Find(".lt-datetime__time input").Change("09:15:00");
        accepted = await cut.InvokeAsync(() => cut.Instance.ConfirmAsync());

        Assert.Multiple(() =>
        {
            Assert.That(accepted, Is.True);
            Assert.That(raised, Is.EqualTo(new DateTimeOffset?[] { new(2026, 9, 28, 9, 15, 0, TimeSpan.Zero) }));
            Assert.That(cut.FindAll(".lt-field__error"), Is.Empty);
        });
    }

    [Test]
    public void Disabling_an_open_field_removes_the_actionable_picker()
    {
        var raised = 0;
        var cut = RenderField(p => p
            .Add(x => x.Value, Now)
            .Add(x => x.ValueChanged, (DateTimeOffset? _) => raised++));
        cut.Find(".lt-datetime__toggle").Click();
        Assert.That(cut.FindAll(".lt-datetime__picker"), Has.Count.EqualTo(1));

        cut.Render(p => p.Add(x => x.Disabled, true));

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-datetime__picker"), Is.Empty);
            Assert.That(cut.Find(".lt-datetime__toggle").GetAttribute("aria-expanded"), Is.EqualTo("false"));
            Assert.That(cut.Find("input.lt-datetime__input").HasAttribute("disabled"), Is.True);
            Assert.That(raised, Is.Zero);
        });

        cut.Render(p => p.Add(x => x.Disabled, false));
        Assert.That(cut.FindAll(".lt-datetime__picker"), Is.Empty);
        cut.Find(".lt-datetime__toggle").Click();
        Assert.That(cut.FindAll(".lt-datetime__picker"), Has.Count.EqualTo(1));
    }

    [TestCase("typed")]
    [TestCase("day")]
    [TestCase("quick")]
    [TestCase("clear")]
    [TestCase("parameter")]
    public async Task A_new_choice_clears_the_picker_time_error(string choice)
    {
        var cut = RenderField(p => p.Add(x => x.Value, Now).Add(x => x.EmptyText, "Latest"));
        cut.Find(".lt-datetime__toggle").Click();
        cut.Find(".lt-datetime__time input").Change(string.Empty);

        switch (choice)
        {
            case "typed":
                cut.Find("input.lt-datetime__input").Change("2026-09-27T08:15:00Z");
                break;
            case "day":
                Day(cut, "Tuesday 15 September 2026").Click();
                break;
            case "quick":
                Pick(cut, "24 hours ago").Click();
                break;
            case "clear":
                cut.FindAll(".lt-datetime__actions button").Single(button => button.TextContent == "Clear (Latest)").Click();
                break;
            case "parameter":
                cut.Render(p => p.Add(x => x.Value, Now.AddDays(-1)));
                break;
        }

        Assert.That(await cut.InvokeAsync(() => cut.Instance.ConfirmAsync()), Is.True);
        Assert.That(cut.FindAll(".lt-field__error"), Is.Empty);
    }
}
