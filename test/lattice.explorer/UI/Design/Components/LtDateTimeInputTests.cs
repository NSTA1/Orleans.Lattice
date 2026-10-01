using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Design;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

/// <summary>
/// Issue #4148: the date and time field. A typed ISO 8601 entry in UTC with the zone always
/// shown, read culture-invariant and never converted silently; the reader's local time as
/// secondary text; inline validation against a minimum, a maximum and the future; and an
/// optional empty state.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed partial class LtDateTimeInputTests : ShellDesignTestContext
{
    /// <summary>The clock every test reads: Monday 28 September 2026, 14:30:15 UTC.</summary>
    private static readonly DateTimeOffset Now = new(2026, 9, 28, 14, 30, 15, TimeSpan.Zero);

    /// <summary>Pins the field's clock.</summary>
    public LtDateTimeInputTests()
    {
        Services.AddSingleton<TimeProvider>(new FixedTimeProvider(Now));
    }

    [Test]
    public void The_label_names_the_entry_and_the_zone_is_always_shown_and_announced()
    {
        var cut = RenderField();
        var input = cut.Find("input.lt-datetime__input");
        var zone = cut.Find(".lt-datetime__zone");

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("label.lt-field__label").GetAttribute("for"), Is.EqualTo(input.Id));
            Assert.That(input.Id, Is.EqualTo(cut.Instance.InputId));
            Assert.That(zone.TextContent, Is.EqualTo("UTC"));
            Assert.That(input.GetAttribute("aria-describedby")!.Split(' '), Does.Contain(zone.Id), "the zone is announced with the entry");
            Assert.That(input.ClassList, Does.Contain("lt-input--mono"));
            Assert.That(cut.Find(".lt-datetime__toggle").GetAttribute("aria-expanded"), Is.EqualTo("false"));
        });
    }

    [Test]
    public void A_value_is_shown_as_an_iso_instant_in_utc_to_the_second()
    {
        var cut = RenderField(p => p.Add(x => x.Value, new DateTimeOffset(2026, 9, 28, 15, 5, 7, 900, TimeSpan.FromHours(1))));

        Assert.That(cut.Find("input.lt-datetime__input").GetAttribute("value"), Is.EqualTo("2026-09-28T14:05:07Z"));
    }

    [Test]
    public void Typing_an_iso_instant_raises_it_in_utc()
    {
        DateTimeOffset? raised = null;
        var cut = RenderField(p => p.Add(x => x.ValueChanged, (DateTimeOffset? value) => raised = value));

        cut.Find("input.lt-datetime__input").Input("2026-09-27T08:15:00Z");

        Assert.That(raised, Is.EqualTo(new DateTimeOffset(2026, 9, 27, 8, 15, 0, TimeSpan.Zero)));
    }

    [Test]
    public void A_time_with_no_offset_is_read_as_utc_never_as_local_time()
    {
        DateTimeOffset? raised = null;
        var cut = RenderField(p => p.Add(x => x.ValueChanged, (DateTimeOffset? value) => raised = value));

        cut.Find("input.lt-datetime__input").Change("2026-09-27 08:15:00");

        Assert.Multiple(() =>
        {
            Assert.That(raised, Is.EqualTo(new DateTimeOffset(2026, 9, 27, 8, 15, 0, TimeSpan.Zero)));
            Assert.That(cut.Find("input.lt-datetime__input").GetAttribute("value"), Is.EqualTo("2026-09-27T08:15:00Z"));
        });
    }

    [Test]
    public void A_typed_offset_is_read_as_the_instant_it_names_and_rewritten_in_utc_where_it_can_be_seen()
    {
        DateTimeOffset? raised = null;
        var cut = RenderField(p => p.Add(x => x.ValueChanged, (DateTimeOffset? value) => raised = value));

        cut.Find("input.lt-datetime__input").Change("2026-09-27T09:15:00+01:00");

        Assert.Multiple(() =>
        {
            Assert.That(raised, Is.EqualTo(new DateTimeOffset(2026, 9, 27, 8, 15, 0, TimeSpan.Zero)));
            Assert.That(raised!.Value.Offset, Is.EqualTo(TimeSpan.Zero));
            Assert.That(cut.Find("input.lt-datetime__input").GetAttribute("value"), Is.EqualTo("2026-09-27T08:15:00Z"),
                "the conversion is shown in the box, not made silently");
        });
    }

    [Test]
    public async Task Text_that_is_not_a_time_is_refused_inline_and_raises_nothing()
    {
        var raised = 0;
        var cut = RenderField(p => p.Add(x => x.ValueChanged, (DateTimeOffset? _) => raised++));

        cut.Find("input.lt-datetime__input").Input("next tuesday");
        var accepted = await cut.InvokeAsync(() => cut.Instance.ConfirmAsync());

        var input = cut.Find("input.lt-datetime__input");
        var error = cut.Find(".lt-field__error");
        Assert.Multiple(() =>
        {
            Assert.That(accepted, Is.False);
            Assert.That(raised, Is.Zero);
            Assert.That(error.TextContent, Does.Contain("Write a time such as 2026-09-28T14:00:00Z."));
            Assert.That(input.GetAttribute("aria-invalid"), Is.EqualTo("true"));
            Assert.That(input.GetAttribute("aria-describedby")!.Split(' '), Does.Contain(error.Id));
            Assert.That(cut.Find(".lt-datetime__control").ClassList, Does.Contain("lt-datetime__control--invalid"));
        });
    }

    [Test]
    public void Editing_clears_the_fields_own_message()
    {
        var cut = RenderField();
        cut.Find("input.lt-datetime__input").Change("nonsense");
        Assert.That(cut.FindAll(".lt-field__error"), Has.Count.EqualTo(1));

        cut.Find("input.lt-datetime__input").Input("2026-09");

        Assert.That(cut.FindAll(".lt-field__error"), Is.Empty);
    }

    [Test]
    public async Task An_empty_field_that_may_be_empty_means_its_empty_text_and_raises_none()
    {
        DateTimeOffset? raised = Now;
        var cut = RenderField(p => p
            .Add(x => x.Value, Now)
            .Add(x => x.EmptyText, "Latest")
            .Add(x => x.ValueChanged, (DateTimeOffset? value) => raised = value));

        cut.Find("input.lt-datetime__input").Input(string.Empty);
        var accepted = await cut.InvokeAsync(() => cut.Instance.ConfirmAsync());

        Assert.Multiple(() =>
        {
            Assert.That(accepted, Is.True);
            Assert.That(raised, Is.Null);
            Assert.That(cut.Find("input.lt-datetime__input").GetAttribute("placeholder"), Is.EqualTo("Latest"), "the empty state says what it means, in prose");
        });
    }

    [Test]
    public async Task An_empty_field_that_needs_a_value_says_so()
    {
        var cut = RenderField();

        var accepted = await cut.InvokeAsync(() => cut.Instance.ConfirmAsync());

        Assert.Multiple(() =>
        {
            Assert.That(accepted, Is.False);
            Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("Choose a date and time."));
            Assert.That(cut.Find("input.lt-datetime__input").HasAttribute("placeholder"), Is.False);
        });
    }

    [Test]
    public async Task A_time_before_the_minimum_is_refused()
    {
        var cut = RenderField(p => p.Add(x => x.Min, new DateTimeOffset(2026, 9, 1, 0, 0, 0, TimeSpan.Zero)));

        cut.Find("input.lt-datetime__input").Input("2026-08-31T23:59:59Z");
        var accepted = await cut.InvokeAsync(() => cut.Instance.ConfirmAsync());

        Assert.Multiple(() =>
        {
            Assert.That(accepted, Is.False);
            Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("Choose a time no earlier than 2026-09-01 00:00:00 UTC."));
        });
    }

    [Test]
    public async Task A_time_after_the_maximum_is_refused()
    {
        var cut = RenderField(p => p.Add(x => x.Max, new DateTimeOffset(2026, 9, 1, 0, 0, 0, TimeSpan.Zero)));

        cut.Find("input.lt-datetime__input").Input("2026-09-01T00:00:01Z");
        var accepted = await cut.InvokeAsync(() => cut.Instance.ConfirmAsync());

        Assert.Multiple(() =>
        {
            Assert.That(accepted, Is.False);
            Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("Choose a time no later than 2026-09-01 00:00:00 UTC."));
        });
    }

    [Test]
    public async Task A_field_that_refuses_the_future_accepts_now_and_refuses_a_second_later()
    {
        var cut = RenderField(p => p.Add(x => x.AllowFuture, false));

        cut.Find("input.lt-datetime__input").Input("2026-09-28T14:30:15Z");
        var now = await cut.InvokeAsync(() => cut.Instance.ConfirmAsync());
        cut.Find("input.lt-datetime__input").Input("2026-09-28T14:30:16Z");
        var later = await cut.InvokeAsync(() => cut.Instance.ConfirmAsync());

        Assert.Multiple(() =>
        {
            Assert.That(now, Is.True);
            Assert.That(later, Is.False);
            Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("Choose a time that is not in the future."));
        });
    }

    [Test]
    public void A_pages_own_message_is_shown_in_place_of_the_fields()
    {
        var cut = RenderField(p => p.Add(x => x.Error, "That revision is gone.").Add(x => x.Hint, "As of when."));

        var input = cut.Find("input.lt-datetime__input");
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("That revision is gone."));
            Assert.That(input.GetAttribute("aria-describedby"), Does.Contain(cut.Find(".lt-field__hint").Id));
        });
    }

    [Test]
    public void A_new_value_from_the_page_replaces_what_is_typed()
    {
        var cut = RenderField();
        cut.Find("input.lt-datetime__input").Change("nonsense");

        cut.Render(p => p.Add(x => x.Value, Now));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("input.lt-datetime__input").GetAttribute("value"), Is.EqualTo("2026-09-28T14:30:15Z"));
            Assert.That(cut.FindAll(".lt-field__error"), Is.Empty);
        });
    }

    [Test]
    public void The_readers_local_time_is_shown_as_secondary_text_and_announced()
    {
        var module = JSInterop.SetupModule(ShellDesignAssets.DateTimeModuleSpecifier);
        module.Mode = JSRuntimeMode.Loose;
        module.Setup<LtBrowserZone?>("zone").SetResult(new LtBrowserZone { Id = "Europe/London", OffsetMinutes = 60 });

        var cut = RenderField(p => p.Add(x => x.Value, new DateTimeOffset(2026, 9, 28, 14, 5, 0, TimeSpan.Zero)));

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-datetime__local"), Has.Count.EqualTo(1)));
        var local = cut.Find(".lt-datetime__local");
        Assert.Multiple(() =>
        {
            Assert.That(local.TextContent, Is.EqualTo("Your local time: 2026-09-28 15:05:00 Europe/London (UTC+01:00)"));
            Assert.That(cut.Find("input.lt-datetime__input").GetAttribute("aria-describedby"), Does.Contain(local.Id));
            Assert.That(cut.Find("input.lt-datetime__input").GetAttribute("value"), Is.EqualTo("2026-09-28T14:05:00Z"), "the value itself stays in UTC");
        });
    }

    [Test]
    public void Without_script_or_a_value_there_is_no_local_time()
    {
        var cut = RenderField(p => p.Add(x => x.Value, Now));

        Assert.That(cut.FindAll(".lt-datetime__local"), Is.Empty, "the zone is unknown until the browser reports it");
    }

    [Test]
    public void The_script_module_is_attached_to_the_field()
    {
        var module = JSInterop.SetupModule(ShellDesignAssets.DateTimeModuleSpecifier);
        module.Mode = JSRuntimeMode.Loose;

        var cut = RenderField();

        var attach = module.VerifyInvoke("attach");
        Assert.That(((ElementReference)attach.Arguments[0]!).Id, Is.EqualTo(cut.Find(".lt-datetime").GetAttribute("blazor:elementreference")));
    }

    [Test]
    public void A_zone_the_platform_cannot_name_falls_back_to_its_offset()
    {
        var zone = LtDateTimeInput.Resolve(new LtBrowserZone { Id = "Not/AZone", OffsetMinutes = -330 });

        Assert.Multiple(() =>
        {
            Assert.That(zone, Is.Not.Null);
            Assert.That(zone!.BaseUtcOffset, Is.EqualTo(TimeSpan.FromMinutes(-330)));
            Assert.That(zone.Id, Is.EqualTo("UTC-05:30"));
            Assert.That(LtDateTimeInput.Resolve(new LtBrowserZone()), Is.Null, "nothing reported, nothing shown");
            Assert.That(LtDateTimeInput.Resolve(new LtBrowserZone { OffsetMinutes = 24 * 60 }), Is.Null, "an impossible offset is refused");
        });
    }

    [Test]
    public void Disabled_reaches_the_entry_and_the_picker_button()
    {
        var cut = RenderField(p => p.Add(x => x.Disabled, true));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("input.lt-datetime__input").HasAttribute("disabled"), Is.True);
            Assert.That(cut.Find(".lt-datetime__toggle").HasAttribute("disabled"), Is.True);
        });
    }

    [Test]
    public async Task FocusAsync_moves_focus_to_the_entry()
    {
        var cut = RenderField();

        await cut.InvokeAsync(() => cut.Instance.FocusAsync().AsTask());

        var invocation = JSInterop.VerifyFocusAsyncInvoke();
        Assert.That(((ElementReference)invocation.Arguments[0]!).Id, Is.EqualTo(cut.Find("input.lt-datetime__input").GetAttribute("blazor:elementreference")));
    }

    private IRenderedComponent<LtDateTimeInput> RenderField(Action<ComponentParameterCollectionBuilder<LtDateTimeInput>>? more = null) =>
        Render<LtDateTimeInput>(p =>
        {
            p.Add(x => x.Label, "As of");
            more?.Invoke(p);
        });

    /// <summary>A clock that never moves.</summary>
    private sealed class FixedTimeProvider(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }
}
