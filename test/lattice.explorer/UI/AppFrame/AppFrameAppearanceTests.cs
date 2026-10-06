using Orleans.Lattice.Explorer.UI.Framing;

namespace Orleans.Lattice.Explorer.Tests.UI.Framing;

/// <summary>
/// The appearance a frame is told about: what the frame host reads from the Explorer's page
/// is held to the closed sets before any of it reaches a frame, and the per-circuit host
/// context reports what it last observed.
/// </summary>
[TestFixture]
public sealed class AppFrameAppearanceTests
{
    [Test]
    public void The_pages_appearance_is_read_in_order()
    {
        Assert.That(
            AppFrameAppearance.FromDocument(["board", "more", "compact", "reduce"]),
            Is.EqualTo(new AppFrameAppearance("board", "more", "compact", true)));
    }

    [Test]
    public void Anything_outside_the_closed_sets_falls_back_value_by_value()
    {
        Assert.That(
            AppFrameAppearance.FromDocument(["<script>", "loud", null, "full"]),
            Is.EqualTo(AppFrameAppearance.Default));
    }

    [TestCase("Board", "MORE", "Compact", "Reduce")]
    [TestCase("board ", " more", "compact\u0000", "reduce ")]
    [TestCase("b\u043Eard", "m\u043Ere", "c\u043Empact", "r\u0435duce")]
    public void Only_an_exact_ordinal_match_is_accepted_so_case_padding_and_lookalikes_fall_back(string theme, string contrast, string density, string motion)
    {
        Assert.That(
            AppFrameAppearance.FromDocument([theme, contrast, density, motion]),
            Is.EqualTo(AppFrameAppearance.Default));
    }

    [Test]
    public void An_oversized_value_is_dropped_rather_than_carried_to_a_frame()
    {
        var huge = new string('b', 64 * 1024);

        var appearance = AppFrameAppearance.FromDocument([huge, huge, huge, huge]);

        Assert.That(appearance, Is.EqualTo(AppFrameAppearance.Default));
    }

    private static readonly TestCaseData[] MalformedReads =
    [
        new TestCaseData(new object?[] { null }).SetArgDisplayNames("null"),
        new TestCaseData((object)new string[0]).SetArgDisplayNames("none"),
        new TestCaseData((object)new[] { "board", "more", "compact" }).SetArgDisplayNames("three"),
        new TestCaseData((object)new[] { "board", "more", "compact", "reduce", "extra" }).SetArgDisplayNames("five"),
    ];

    [TestCaseSource(nameof(MalformedReads))]
    public void A_malformed_read_is_the_whole_default(string[]? values)
    {
        Assert.That(AppFrameAppearance.FromDocument(values), Is.SameAs(AppFrameAppearance.Default));
    }

    [Test]
    public void The_host_context_reports_the_default_until_it_observes_the_page_and_then_what_it_observed()
    {
        var context = new DefaultAppFrameHostContext();
        var before = context.Appearance;

        context.ObserveAppearance(new AppFrameAppearance("board", "loud", "compact", true));

        Assert.Multiple(() =>
        {
            Assert.That(before, Is.EqualTo(AppFrameAppearance.Default));
            Assert.That(context.Appearance, Is.EqualTo(new AppFrameAppearance("board", "standard", "compact", true)), "sanitised before it is kept");
            Assert.That(() => context.ObserveAppearance(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void A_host_context_that_does_not_keep_the_appearance_ignores_an_observation()
    {
        IAppFrameHostContext context = new FixedHostContext();

        context.ObserveAppearance(new AppFrameAppearance("board", "more", "compact", true));

        Assert.That(context.Appearance, Is.SameAs(AppFrameAppearance.Default));
    }

    /// <summary>A host context declaring only the required members, so the observation is the interface default.</summary>
    private sealed class FixedHostContext : IAppFrameHostContext
    {
        public AppFrameAppearance Appearance => AppFrameAppearance.Default;

        public string? TenantDisplayName => null;

        public string? UserDisplayName => null;
    }
}
