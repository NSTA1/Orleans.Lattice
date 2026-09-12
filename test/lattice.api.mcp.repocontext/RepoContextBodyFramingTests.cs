using ModelContextProtocol;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Unit tests for <see cref="RepoContextBodyFraming"/> and for the guard it backs
/// on the two write seams, <see cref="RepoContextToolHandlers.RememberAsync"/> and
/// <see cref="RepoContextToolHandlers.UpdateAsync"/>.
/// </summary>
/// <remarks>
/// <para>
/// A malformed MCP tool call serialises part of its own framing into the
/// <c>body</c> argument. The seam used to store that verbatim and report success.
/// In the damaging shape a <em>later argument is absorbed into the body</em>, so
/// the field it should have populated stays empty and nothing anywhere records
/// that it was ever supplied - a survey of a live store found 72 contaminated
/// entries, 17 of them of that shape, one of which silently dropped an entire
/// <c>addLinks</c> payload so four graph edges were never created.
/// </para>
/// <para>
/// <strong>This fixture builds every framing shape from character constants.</strong>
/// Spelling the sequences out would make this very file an instance of the defect
/// it tests, which has already happened twice in the field. The production
/// detector matches structurally for the same reason and likewise contains no
/// literal token.
/// </para>
/// <para>
/// The negative controls are load-bearing, not decoration. The guard must not
/// block a note that legitimately quotes one of these tokens, because such a note
/// is how the defect gets documented. Equally, several tests here assert the guard
/// <em>fires</em>: a guard evidenced only by tests that stay quiet is
/// indistinguishable from a guard that never runs at all.
/// </para>
/// </remarks>
[TestFixture]
public sealed class RepoContextBodyFramingTests
{
    private const char Lt = '<';
    private const char Gt = '>';

    /// <summary>A closing tag naming <paramref name="name"/>.</summary>
    private static string Close(string name) => $"{Lt}/{name}{Gt}";

    /// <summary>An opening tag naming <paramref name="name"/>.</summary>
    private static string Open(string name) => $"{Lt}{name}{Gt}";

    /// <summary>A structural parameter tag carrying <paramref name="name"/> as its argument name.</summary>
    private static string OpenParameter(string name) => $"{Lt}parameter name=\"{name}\"{Gt}";

    // ---- detector: it fires on the real corruption shapes --------------------

    /// <summary>
    /// The damaging shape, and the primary positive control for this change: the
    /// body's own close tag followed by the framing of the argument that was
    /// swallowed. The detector must both flag it and name what was lost.
    /// </summary>
    [Test]
    public void Inspect_flags_a_swallowed_argument_and_names_it()
    {
        var body = "A genuine note body." + "\n" + Close("body") + "\n"
            + OpenParameter("author") + "backlog-worker-2374";

        var inspection = RepoContextBodyFraming.Inspect(body);

        Assert.Multiple(() =>
        {
            Assert.That(inspection.IsContaminated, Is.True, "A trailing close-then-open framing run is the corruption shape.");
            Assert.That(inspection.DisplacedArguments, Is.EqualTo(new[] { "author" }));
        });
    }

    /// <summary>
    /// The worst observed instance: an entire <c>addLinks</c> payload absorbed, so
    /// the edges it declared were never created while the write returned success.
    /// </summary>
    [Test]
    public void Inspect_flags_a_swallowed_add_links_payload()
    {
        var body = "Decision recorded." + "\n" + Close("body") + "\n"
            + OpenParameter("addLinks") + "{\"related\":[\"repo/lattice/mem/glossary/wal\"]}";

        var inspection = RepoContextBodyFraming.Inspect(body);

        Assert.Multiple(() =>
        {
            Assert.That(inspection.IsContaminated, Is.True);
            Assert.That(inspection.DisplacedArguments, Is.EqualTo(new[] { "addLinks" }));
        });
    }

    /// <summary>
    /// Names every displaced argument, in the order the framing presents them,
    /// because a caller repairing its call needs the full list rather than the
    /// first one found.
    /// </summary>
    [Test]
    public void Inspect_names_every_displaced_argument_in_order()
    {
        var body = "Note." + "\n" + Close("body") + "\n"
            + Open("author") + "worker" + Close("author") + "\n"
            + Open("tags") + "gotchas" + Close("tags");

        Assert.That(
            RepoContextBodyFraming.Inspect(body).DisplacedArguments,
            Is.EqualTo(new[] { "author", "tags" }));
    }

    /// <summary>
    /// The benign shape: structural framing with no argument name in it. Still
    /// corrupt and still refused, but there is nothing displaced to report, so the
    /// message must not invent one.
    /// </summary>
    [Test]
    public void Inspect_flags_bare_structural_framing_with_nothing_displaced()
    {
        var body = "A genuine note body." + "\n" + Close("parameter") + "\n" + Close("invoke");

        var inspection = RepoContextBodyFraming.Inspect(body);

        Assert.Multiple(() =>
        {
            Assert.That(inspection.IsContaminated, Is.True);
            Assert.That(inspection.DisplacedArguments, Is.Empty);
        });
    }

    /// <summary>A namespaced structural tag is the same framing and must not evade the check.</summary>
    [Test]
    public void Inspect_flags_namespaced_structural_framing()
    {
        var body = "Body text." + "\n" + Close("body") + "\n" + Close("antml:invoke");

        Assert.That(RepoContextBodyFraming.Inspect(body).IsContaminated, Is.True);
    }

    // ---- detector: negative controls -----------------------------------------

    /// <summary>
    /// The load-bearing negative control. A note that quotes one of these tokens in
    /// ordinary prose is exactly how this defect gets documented, and blocking it
    /// would make the guard self-defeating.
    /// </summary>
    [Test]
    public void Inspect_accepts_a_delimiter_quoted_in_ordinary_prose()
    {
        var body = "The corruption appends " + Close("body")
            + " to the end of the stored text, which is how a repair pass recognises it.";

        Assert.That(
            RepoContextBodyFraming.Inspect(body).IsContaminated,
            Is.False,
            "One token mentioned in prose is documentation, not framing.");
    }

    /// <summary>
    /// An HTML tail ends in a close tag too, but only one of its elements is an
    /// argument name, so the two-marker requirement rules it out.
    /// </summary>
    [Test]
    public void Inspect_accepts_an_html_document_tail()
    {
        var body = "Rendered output:\n" + Close("body") + "\n" + Close("html");

        Assert.That(RepoContextBodyFraming.Inspect(body).IsContaminated, Is.False);
    }

    /// <summary>
    /// The case that forces tail anchoring. A quoted markup sample can contain
    /// several argument-named tags; what distinguishes it from framing is that the
    /// note carries on afterwards in prose.
    /// </summary>
    [Test]
    public void Inspect_accepts_markup_quoted_mid_body_and_followed_by_prose()
    {
        var body = "Example feed entry:\n"
            + Open("author") + "Ada" + Close("author") + "\n"
            + Open("title") + "Notes" + Close("title") + "\n\n"
            + "That sample is ordinary XML. The paragraph you are reading now continues well past "
            + "the markup, which is precisely what separates a quoted example from framing emitted "
            + "by a malformed call.";

        Assert.That(
            RepoContextBodyFraming.Inspect(body).IsContaminated,
            Is.False,
            "Framing is emitted at the end of a body; a sample the note goes on to discuss is not framing.");
    }

    /// <summary>A body with no markup at all, and the degenerate inputs.</summary>
    [TestCase(null)]
    [TestCase("")]
    [TestCase("An ordinary note with no markup whatsoever.")]
    public void Inspect_accepts_a_clean_body(string? body)
        => Assert.That(RepoContextBodyFraming.Inspect(body).IsContaminated, Is.False);

    /// <summary>
    /// A lone angle bracket in prose must not swallow the rest of the body while
    /// hunting for a close bracket, which would let a later legitimate token be
    /// read as one giant tag.
    /// </summary>
    [Test]
    public void Inspect_accepts_an_unterminated_angle_bracket_in_prose()
    {
        var body = "Latency was " + Lt + " 5 ms across the sweep, and the sweep then armed cleanly.";

        Assert.That(RepoContextBodyFraming.Inspect(body).IsContaminated, Is.False);
    }

    // ---- message ------------------------------------------------------------

    /// <summary>The message must name what was lost, and say that it was not applied.</summary>
    [Test]
    public void DescribeRejection_names_the_displaced_arguments()
    {
        var message = RepoContextBodyFraming.DescribeRejection(
            RepoContextBodyFraming.RememberBodyLocation, ["author", "tags"]);

        Assert.Multiple(() =>
        {
            Assert.That(message, Does.Contain("'body'"));
            Assert.That(message, Does.Contain("author"));
            Assert.That(message, Does.Contain("tags"));
            Assert.That(message, Does.Contain("NOT applied"));
        });
    }

    /// <summary>With nothing displaced the message must not claim anything was.</summary>
    [Test]
    public void DescribeRejection_reports_no_displaced_arguments_when_there_are_none()
        => Assert.That(
            RepoContextBodyFraming.DescribeRejection(RepoContextBodyFraming.RememberBodyLocation, []),
            Does.Not.Contain("NOT applied"));

    // ---- seam: remember ------------------------------------------------------

    /// <summary>
    /// Acceptance criterion 1, and the positive control for the <c>remember</c>
    /// seam: the write is refused and the message names what the malformed call
    /// displaced.
    /// </summary>
    [Test]
    public void RememberAsync_rejects_a_body_carrying_framing_and_names_the_displaced_argument()
    {
        var body = "A genuine note body." + "\n" + Close("body") + "\n"
            + OpenParameter("author") + "backlog-worker-2374";

        Assert.That(
            () => RepoContextToolHandlers.RememberAsync(null!, "lattice", "gotchas", body: body),
            Throws.InstanceOf<McpException>()
                .With.Message.Contains("'body'")
                .And.Message.Contains("author"),
            "Storing this verbatim is the defect: the author argument is lost and the write reports success.");
    }

    /// <summary>
    /// Acceptance criterion 3 at the seam. A body documenting the defect must reach
    /// service resolution - which then fails only because this test supplies no
    /// provider - proving the guard let it through rather than over-rejecting it.
    /// </summary>
    [Test]
    public async Task RememberAsync_accepts_a_body_documenting_the_defect_in_prose()
    {
        var context = await RepoContextRequestContexts.CreateAsync(services: null);
        var body = "A malformed call appends " + Close("body")
            + " to the stored text, which is how the repair pass finds it.";

        Assert.That(
            () => RepoContextToolHandlers.RememberAsync(context, "lattice", "gotchas", body: body),
            Throws.InstanceOf<InvalidOperationException>().With.Message.Contains("no service provider"),
            "The guard must not block the documentation of the very defect it prevents.");
    }

    // ---- seam: update --------------------------------------------------------

    /// <summary>
    /// Acceptance criterion 2, asserted directly rather than inferred from the
    /// <c>remember</c> test. <c>update</c> is the path a repair pass writes
    /// through, so an unguarded <c>update</c> would let corruption straight back
    /// in behind the guard.
    /// </summary>
    [Test]
    public void UpdateAsync_rejects_a_patched_body_carrying_framing_and_names_the_displaced_argument()
    {
        var body = "Repaired text." + "\n" + Close("body") + "\n"
            + OpenParameter("tags") + "gotchas";

        Assert.That(
            () => RepoContextToolHandlers.UpdateAsync(
                null!,
                "repo/lattice/mem/gotchas/example",
                fields: new Dictionary<string, string> { ["body"] = body }),
            Throws.InstanceOf<McpException>()
                .With.Message.Contains("'fields'")
                .And.Message.Contains("tags"));
    }

    /// <summary>The field name is matched without regard to case, so no casing evades the guard.</summary>
    [Test]
    public void UpdateAsync_rejects_a_patched_body_whatever_the_case_of_the_field_name()
    {
        var body = "Repaired text." + "\n" + Close("parameter") + "\n" + Close("invoke");

        Assert.That(
            () => RepoContextToolHandlers.UpdateAsync(
                null!,
                "repo/lattice/mem/gotchas/example",
                fields: new Dictionary<string, string> { ["Body"] = body }),
            Throws.InstanceOf<McpException>());
    }

    /// <summary>Acceptance criterion 3 for the <c>update</c> seam.</summary>
    [Test]
    public async Task UpdateAsync_accepts_a_patched_body_documenting_the_defect_in_prose()
    {
        var context = await RepoContextRequestContexts.CreateAsync(services: null);
        var body = "A malformed call appends " + Close("body") + " to the stored text.";

        Assert.That(
            () => RepoContextToolHandlers.UpdateAsync(
                context,
                "repo/lattice/mem/gotchas/example",
                fields: new Dictionary<string, string> { ["body"] = body }),
            Throws.InstanceOf<InvalidOperationException>().With.Message.Contains("no service provider"));
    }

    /// <summary>A patch that does not touch the body is unaffected by the guard.</summary>
    [Test]
    public async Task UpdateAsync_ignores_fields_other_than_the_body()
    {
        var context = await RepoContextRequestContexts.CreateAsync(services: null);

        Assert.That(
            () => RepoContextToolHandlers.UpdateAsync(
                context,
                "repo/lattice/mem/gotchas/example",
                fields: new Dictionary<string, string> { ["title"] = "Anything at all" }),
            Throws.InstanceOf<InvalidOperationException>().With.Message.Contains("no service provider"));
    }
}
