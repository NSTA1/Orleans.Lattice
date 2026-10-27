namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// The block grammar of a <c>.mutation</c> file: every <c>--- FIND</c> /
/// <c>--- REPLACE</c> pair is closed by its own <c>--- END</c>, a block
/// marker met inside an open block is refused rather than collected as text,
/// and between or after blocks only blank lines and <c>#</c> comments may
/// appear - a stray marker or text there is refused rather than skipped.
/// <para>
/// The parser used to absorb it. A file with several blocks and one final
/// <c>--- END</c> then parsed to a single edit whose replacement carried every
/// later block, markers included: the first anchor still matched, the later
/// perturbations were never applied, and the mutant reached TLC as unparseable
/// TLA+, which no pair experiment can credit as a detection. These tests drive
/// the real <see cref="SpecMutationCatalogue.Parse"/> and
/// <see cref="SpecMutation.Apply"/>; they need no JVM.
/// </para>
/// </summary>
[TestFixture]
public sealed class SpecMutationBlockGrammarTests
{
    private const string BaseModuleName = "TerminatorBase";

    private const string BaseSpecification = "---- MODULE TerminatorBase ----\nA == 1\nB == 2\n====\n";

    private const string Header = "MODULE: TerminatorMutant\nTARGET: Termination\nCLASS: Temporal\nSUMMARY: control\n\n";

    private const string GcMutationModule = "WalGcStarvationDrive";

    private static string WithEndings(string text, bool crlf) => crlf ? text.Replace("\n", "\r\n") : text;

    private static SpecMutation Parse(string body, bool crlf) =>
        SpecMutationCatalogue.Parse("TerminatorMutant", WithEndings(Header + body, crlf));

    /// <summary>
    /// The positive control: a properly terminated two-block file parses to two
    /// edits and expands with both applied, under either line ending.
    /// </summary>
    [Test]
    public void A_terminated_multi_block_mutation_applies_every_block([Values] bool crlf)
    {
        var mutation = Parse(
            "--- FIND\nA == 1\n--- REPLACE\nA == 3\n--- END\n--- FIND\nB == 2\n--- REPLACE\nB == 4\n--- END\n",
            crlf);

        Assert.Multiple(() =>
        {
            Assert.That(mutation.Edits, Is.EqualTo(new[] { new SpecEdit("A == 1", "A == 3"), new SpecEdit("B == 2", "B == 4") }));
            Assert.That(
                mutation.Apply(BaseSpecification, BaseModuleName),
                Is.EqualTo("---- MODULE TerminatorMutant ----\nA == 3\nB == 4\n====\n"));
        });
    }

    /// <summary>
    /// The defect itself: a REPLACE block missing its own <c>--- END</c>, with
    /// the next block's FIND following it, is refused at that FIND.
    /// </summary>
    [Test]
    public void A_replace_block_missing_its_end_is_refused([Values] bool crlf)
    {
        var refusal = Assert.Throws<InvalidOperationException>(() => Parse(
            "--- FIND\nA == 1\n--- REPLACE\nA == 3\n--- FIND\nB == 2\n--- REPLACE\nB == 4\n--- END\n",
            crlf));

        Assert.That(
            refusal!.Message,
            Does.StartWith("TerminatorMutant.mutation line 10 is the marker '--- FIND' inside a '--- REPLACE' block; expected '--- END' first."));
    }

    /// <summary>
    /// Every other marker out of order inside an open block is refused too,
    /// naming the line and the marker the block was waiting for.
    /// </summary>
    [TestCase("--- FIND\nA == 1\n--- FIND\nB == 2\n--- REPLACE\nB == 4\n--- END\n", 8, "--- FIND", "--- FIND", "--- REPLACE")]
    [TestCase("--- FIND\nA == 1\n--- END\n--- REPLACE\nA == 3\n--- END\n", 8, "--- END", "--- FIND", "--- REPLACE")]
    [TestCase("--- FIND\nA == 1\n--- REPLACE\nA == 3\n--- REPLACE\nA == 4\n--- END\n", 10, "--- REPLACE", "--- REPLACE", "--- END")]
    public void An_out_of_order_marker_inside_a_block_is_refused(string body, int line, string marker, string open, string close)
    {
        Assert.Multiple(() =>
        {
            foreach (var crlf in new[] { false, true })
            {
                var refusal = Assert.Throws<InvalidOperationException>(() => Parse(body, crlf));
                Assert.That(
                    refusal!.Message,
                    Does.StartWith($"TerminatorMutant.mutation line {line} is the marker '{marker}' inside a '{open}' block; expected '{close}' first."),
                    crlf ? "CRLF" : "LF");
            }
        });
    }

    /// <summary>
    /// Between and after edits, blank lines, whitespace-only lines and
    /// <c>#</c> comments separate blocks without changing them.
    /// </summary>
    [Test]
    public void Blank_lines_and_comments_between_blocks_are_accepted([Values] bool crlf)
    {
        var mutation = Parse(
            "--- FIND\nA == 1\n--- REPLACE\nA == 3\n--- END\n\n# second edit\n   \n"
            + "--- FIND\nB == 2\n--- REPLACE\nB == 4\n--- END\n\n# done\n",
            crlf);

        Assert.Multiple(() =>
        {
            Assert.That(mutation.Edits, Is.EqualTo(new[] { new SpecEdit("A == 1", "A == 3"), new SpecEdit("B == 2", "B == 4") }));
            Assert.That(
                mutation.Apply(BaseSpecification, BaseModuleName),
                Is.EqualTo("---- MODULE TerminatorMutant ----\nA == 3\nB == 4\n====\n"));
        });
    }

    /// <summary>
    /// After a closed edit, anything other than the next <c>--- FIND</c>, a
    /// blank line or a comment is refused rather than skipped: a stray
    /// <c>--- END</c> or <c>--- REPLACE</c>, text that belonged in a block, and
    /// trailing text after the last block.
    /// </summary>
    [TestCase("--- FIND\nA == 1\n--- REPLACE\nA == 3\n--- END\n--- END\n", "line 11 is the marker '--- END' outside any block; expected '--- FIND' to open the next edit.")]
    [TestCase("--- FIND\nA == 1\n--- REPLACE\nA == 3\n--- END\n--- REPLACE\nB == 4\n--- END\n", "line 11 is the marker '--- REPLACE' outside any block; expected '--- FIND' to open the next edit.")]
    [TestCase("--- FIND\nA == 1\n--- REPLACE\nA == 3\n--- END\nB == 2\n--- FIND\nB == 2\n--- REPLACE\nB == 4\n--- END\n", "line 11 is text outside any block; expected '--- FIND', a blank line or a '#' comment: 'B == 2'")]
    [TestCase("--- FIND\nA == 1\n--- REPLACE\nA == 3\n--- END\n\ntrailing\n", "line 12 is text outside any block; expected '--- FIND', a blank line or a '#' comment: 'trailing'")]
    public void Anything_else_after_a_closed_block_is_refused(string body, string expected)
    {
        Assert.Multiple(() =>
        {
            foreach (var crlf in new[] { false, true })
            {
                var refusal = Assert.Throws<InvalidOperationException>(() => Parse(body, crlf));
                Assert.That(refusal!.Message, Is.EqualTo($"TerminatorMutant.mutation {expected}"), crlf ? "CRLF" : "LF");
            }
        });
    }

    /// <summary>
    /// The five GC starvation mutations that shipped with a single shared
    /// <c>--- END</c> now carry one per block. Pinning each block count, target
    /// and bound keeps the repair from being undone by merging blocks back
    /// together, which the grammar alone would accept if done consistently.
    /// </summary>
    [TestCase("NoAwaitedDriveTimerAwaitsDrive", 4, "NoAwaitedDrive", "")]
    [TestCase("RefusalNotChargedChargeRefusal", 3, "RefusalNotCharged", "")]
    [TestCase("RefusalNotChargedRefusalAfterSuccessCharged", 3, "RefusalNotCharged", "")]
    [TestCase("RetryAfterSiblingRetryWithoutCredit", 2, "RetryAfterSibling", "LeafCount = 2")]
    [TestCase("TurnReleasedTimerRefusalKeepsTurn", 4, "TurnReleased", "")]
    public void A_repaired_gc_mutation_keeps_one_edit_per_block(string name, int edits, string target, string bounds)
    {
        var module = SpecModuleCatalogue.Repository().Single(m => m.Name == GcMutationModule);
        var mutation = module.LoadMutations().Single(m => m.Name == name);

        Assert.Multiple(() =>
        {
            Assert.That(mutation.Edits, Has.Count.EqualTo(edits));
            Assert.That(mutation.Target, Is.EqualTo(target));
            Assert.That(
                string.Join(", ", mutation.Bounds.Select(b => $"{b.Name} = {b.Value}")),
                Is.EqualTo(bounds));
            Assert.That(
                mutation.Apply(module.ReadSpecification(), module.Name),
                Does.Not.Contain("--- FIND").And.Not.Contain("--- REPLACE").And.Not.Contain("--- END"));
        });
    }
}
