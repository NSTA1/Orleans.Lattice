using System.Globalization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// The detector edges of <see cref="RepoContextBodyFraming"/>: the arms of the
/// structural tag walk that the shape-level fixture alongside this one does not
/// reach.
/// <para>
/// WHY THESE ARMS ARE WORTH TESTS OF THEIR OWN. The detector sits on a write
/// seam, so both of its failure directions are silent. Under-matching stores a
/// malformed call verbatim and reports success - the original defect, which cost
/// a live store 72 contaminated entries. Over-matching refuses a legitimate note,
/// and the notes most likely to be refused are the ones documenting this very
/// defect, so the guard would erase its own explanation. Every arm below decides
/// one of those two directions, and neither announces itself when it goes wrong.
/// </para>
/// <para>
/// <strong>Like the fixture it extends, this file spells no framing token.</strong>
/// Every shape is assembled from the character constants, because a verbatim
/// example in a source file is indistinguishable from an instance of the defect -
/// which has already happened twice in the field.
/// </para>
/// </summary>
public sealed partial class RepoContextBodyFramingTests
{
    /// <summary>A structural parameter tag whose argument name is unquoted.</summary>
    private static string OpenParameterUnquoted(string name) => $"{Lt}parameter name={name}{Gt}";

    /// <summary>A structural parameter tag whose opening quote is never closed.</summary>
    private static string OpenParameterUnterminatedQuote(string name) => $"{Lt}parameter name=\"{name}{Gt}";

    // ---- the run must reach the end of the body ------------------------------

    /// <summary>
    /// The length bound on interleaved text. A displaced argument value is short;
    /// a body that happens to end with a delimiter after a long passage of prose
    /// is a note, not a malformed call.
    /// </summary>
    [Test]
    public void Inspect_accepts_markers_separated_by_more_text_than_a_displaced_value()
    {
        var filler = new string('x', 2100);
        var body = "An analysis." + "\n" + Close("body") + filler + Close("invoke");

        Assert.That(
            RepoContextBodyFraming.Inspect(body).IsContaminated,
            Is.False,
            "Two kilobytes of text between two markers is prose, so the markers are not one run.");
    }

    /// <summary>
    /// The paragraph bound, which is the same judgement by a different measure: a
    /// blank line is a prose boundary, so markers either side of one are not a run
    /// however short the text between them is.
    /// </summary>
    [Test]
    public void Inspect_accepts_markers_separated_by_a_paragraph_break()
    {
        var body = "A note." + "\n" + Close("body") + "\n\n" + Close("invoke");

        Assert.That(RepoContextBodyFraming.Inspect(body).IsContaminated, Is.False);
    }

    /// <summary>
    /// Two markers with no closing tag between them are an opening pair, which is
    /// not the shape a truncated call leaves behind.
    /// </summary>
    [Test]
    public void Inspect_accepts_a_run_of_opening_tags_with_no_closing_tag()
    {
        var body = "Fields:\n" + Open("author") + Open("tags");

        Assert.That(
            RepoContextBodyFraming.Inspect(body).IsContaminated,
            Is.False,
            "A closing tag is what evidences a serialized call rather than a quoted sample.");
    }

    /// <summary>
    /// A run that begins with an opening tag still names its displaced argument.
    /// The leading-closing-tag case is skipped because that tag closes the body
    /// itself and so displaces nothing; an opening tag displaces from the start.
    /// </summary>
    [Test]
    public void Inspect_names_the_argument_of_a_run_that_begins_with_an_opening_tag()
    {
        var body = "A note." + "\n" + Open("author") + "worker" + Close("author");

        var inspection = RepoContextBodyFraming.Inspect(body);

        Assert.Multiple(() =>
        {
            Assert.That(inspection.IsContaminated, Is.True);
            Assert.That(inspection.DisplacedArguments, Is.EqualTo(new[] { "author" }));
        });
    }

    // ---- tag scanning --------------------------------------------------------

    /// <summary>
    /// A tag cannot span a line, so a stray bracket in prose must be skipped
    /// rather than swallow everything up to the next bracket - which would hide
    /// the genuine framing that follows it.
    /// </summary>
    [Test]
    public void Inspect_skips_a_bracket_pair_spanning_a_newline_and_still_sees_the_framing_after_it()
    {
        var body = "Throughput was " + Lt + " 5k/s\nand recovery " + Gt + " 2 minutes.\n"
            + Close("body") + "\n" + OpenParameter("author") + "worker";

        var inspection = RepoContextBodyFraming.Inspect(body);

        Assert.Multiple(() =>
        {
            Assert.That(
                inspection.IsContaminated, Is.True,
                "A multi-line bracket pair is prose; skipping it must not abandon the scan.");
            Assert.That(inspection.DisplacedArguments, Is.EqualTo(new[] { "author" }));
        });
    }

    /// <summary>An empty or whitespace-only tag names nothing and is not framing.</summary>
    [TestCase("")]
    [TestCase("   ")]
    [TestCase("/")]
    public void Inspect_accepts_a_tag_that_names_no_element(string inner)
    {
        var body = "A note." + "\n" + Close("body") + "\n" + Lt + inner + Gt;

        Assert.That(
            RepoContextBodyFraming.Inspect(body).IsContaminated,
            Is.False,
            "Only one marker is recognised, and a single marker is never a run.");
    }

    // ---- the name attribute of a structural tag ------------------------------

    /// <summary>An unquoted attribute value is read the same as a quoted one.</summary>
    [Test]
    public void Inspect_reads_an_unquoted_argument_name()
    {
        var body = "A note." + "\n" + Close("body") + "\n" + OpenParameterUnquoted("provenance") + "sweep";

        Assert.That(
            RepoContextBodyFraming.Inspect(body).DisplacedArguments,
            Is.EqualTo(new[] { "provenance" }));
    }

    /// <summary>
    /// A truncated call is exactly where an opening quote never gets closed, so
    /// the reader must take the rest of the attribute rather than give up - giving
    /// up here would lose the argument name in the one case it matters most.
    /// </summary>
    [Test]
    public void Inspect_reads_an_argument_name_whose_quote_is_never_closed()
    {
        var body = "A note." + "\n" + Close("body") + "\n" + OpenParameterUnterminatedQuote("tags");

        var inspection = RepoContextBodyFraming.Inspect(body);

        Assert.Multiple(() =>
        {
            Assert.That(inspection.IsContaminated, Is.True);
            Assert.That(inspection.DisplacedArguments, Is.EqualTo(new[] { "tags" }));
        });
    }

    /// <summary>
    /// A structural tag naming something that is not an argument of either tool is
    /// still framing - it is unambiguously a serialized call - but there is no
    /// argument to report as displaced, and the detector must not invent one.
    /// </summary>
    [Test]
    public void Inspect_flags_a_structural_tag_naming_an_unknown_argument_without_reporting_it()
    {
        var body = "A note." + "\n" + Close("body") + "\n" + OpenParameter("notAnArgument") + "value";

        var inspection = RepoContextBodyFraming.Inspect(body);

        Assert.Multiple(() =>
        {
            Assert.That(inspection.IsContaminated, Is.True);
            Assert.That(inspection.DisplacedArguments, Is.Empty);
        });
    }

    /// <summary>
    /// The reported names are the canonical declared spellings, not whatever
    /// casing the malformed call happened to emit, so a caller comparing them
    /// against the tool schema finds them.
    /// </summary>
    [Test]
    public void Inspect_reports_displaced_arguments_in_their_declared_casing()
    {
        var body = "A note." + "\n" + Close("body") + "\n"
            + Open("ADDLINKS") + "{}" + Close("addlinks") + "\n"
            + OpenParameter("TTLSECONDS") + "60";

        Assert.That(
            RepoContextBodyFraming.Inspect(body).DisplacedArguments,
            Is.EqualTo(new[] { "addLinks", "ttlSeconds" }));
    }

    /// <summary>
    /// A repeated argument is named once. The list is what a caller re-issues
    /// from, so a duplicate would read as two separate lost arguments.
    /// </summary>
    [Test]
    public void Inspect_names_a_repeated_displaced_argument_only_once()
    {
        var body = "A note." + "\n" + Close("body") + "\n"
            + Open("tags") + "one" + Close("tags") + "\n"
            + Open("tags") + "two" + Close("tags");

        Assert.That(
            RepoContextBodyFraming.Inspect(body).DisplacedArguments,
            Is.EqualTo(new[] { "tags" }));
    }

    /// <summary>
    /// A tag whose element name carries stray quoting still resolves, because the
    /// framing a malformed call emits is not guaranteed to be well formed.
    /// </summary>
    [Test]
    public void Inspect_resolves_an_element_name_wrapped_in_stray_quoting()
    {
        var body = "A note." + "\n" + Close("body") + "\n" + Lt + "\"author\"" + Gt + "worker";

        Assert.That(
            RepoContextBodyFraming.Inspect(body).DisplacedArguments,
            Is.EqualTo(new[] { "author" }));
    }

    /// <summary>
    /// The location string is carried through verbatim, so the message names the
    /// argument the caller actually supplied rather than a fixed one. The two
    /// seams differ only by this string.
    /// </summary>
    [Test]
    public void DescribeRejection_names_the_update_seam_when_that_is_where_the_body_came_from()
    {
        var message = RepoContextBodyFraming.DescribeRejection(
            RepoContextBodyFraming.UpdateBodyLocation, ["author"]);

        Assert.Multiple(() =>
        {
            Assert.That(message, Does.StartWith(RepoContextBodyFraming.UpdateBodyLocation));
            Assert.That(message, Does.Contain("'fields'"));
            Assert.That(
                message, Does.Contain("describe its shape instead of reproducing it"),
                "the advice that keeps a caller from re-submitting the same body as documentation");
        });
    }

    /// <summary>
    /// A clean body that is merely long must not be rejected, and must not cost
    /// anything to classify beyond the marker scan. This also pins that the
    /// detector reads the tail of a body rather than scanning for a token anywhere
    /// in it.
    /// </summary>
    [Test]
    public void Inspect_accepts_a_long_clean_body()
    {
        var body = string.Join(
            "\n\n",
            Enumerable.Range(0, 40).Select(
                i => string.Create(CultureInfo.InvariantCulture, $"Paragraph {i} of an ordinary note.")));

        Assert.That(RepoContextBodyFraming.Inspect(body).IsContaminated, Is.False);
    }
}
