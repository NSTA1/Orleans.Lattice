namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// The toolchain-free gate over production perturbations (contract C7, issue
/// #5158): the grammar of a <c>.perturbation</c> file here, and in
/// <c>RefinementPerturbationTests.Repository.cs</c> the checks over every
/// perturbation in the repository - its anchor applies exactly once to the
/// current <c>FILE</c>, its <c>ROW</c> names a behaviour row of its module's
/// refinement note, and every <c>DETECTORS</c> entry is cited by that row and
/// resolves to a test.
/// <para>
/// WHAT THIS DOES NOT PROVE. That a perturbation turns a detector red is an
/// experiment, not a static fact: it needs a build and a test run, so it is
/// the job of <c>tools/perturbations/run-perturbations.py</c>, which is never
/// run per pull request. This gate keeps every experiment runnable, so that
/// when the runner is invoked the answer is about the detectors and not about
/// a perturbation that drifted from the code it edits.
/// </para>
/// <para>
/// TLC-NEUTRAL BY CONSTRUCTION. This fixture and <see cref="RefinementPerturbation"/>
/// use verdict-affecting Formal types, but no verdict-affecting file names
/// either of them, so they are listed neutral in
/// <c>.github/workflows/tlc-inputs.json</c> and a change to them does not pay
/// for an exhaustive TLC run.
/// </para>
/// </summary>
[TestFixture]
public sealed partial class RefinementPerturbationTests
{
    private const string Header =
        "MODULE: M\nROW: Act(t)\nFILE: src/a/B.cs\nDETECTORS: FTests.One, FTests.two_b\nSUMMARY: breaks it\n\n";

    private const string Block = "--- FIND\nA = 1;\n--- REPLACE\nA = 2;\n--- END\n";

    private static string WithEndings(string text, bool crlf) => crlf ? text.Replace("\n", "\r\n") : text;

    private static RefinementPerturbation Parse(string text, bool crlf = false) =>
        RefinementPerturbation.Parse("T", WithEndings(text, crlf));

    /// <summary>
    /// The positive control: every header and both edits are read, under
    /// either line ending, with comments and blank lines between blocks.
    /// </summary>
    [Test]
    public void A_well_formed_perturbation_parses_every_header_and_edit([Values] bool crlf)
    {
        var perturbation = Parse(Header + "# first\n" + Block + "\n# second\n--- FIND\nB = 1;\n--- REPLACE\n--- END\n", crlf);

        Assert.Multiple(() =>
        {
            Assert.That(perturbation.Name, Is.EqualTo("T"));
            Assert.That(perturbation.Module, Is.EqualTo("M"));
            Assert.That(perturbation.Row, Is.EqualTo("Act(t)"));
            Assert.That(perturbation.File, Is.EqualTo("src/a/B.cs"));
            Assert.That(perturbation.Detectors, Is.EqualTo(new[] { "FTests.One", "FTests.two_b" }));
            Assert.That(perturbation.Summary, Is.EqualTo("breaks it"));
            Assert.That(perturbation.Edits, Is.EqualTo(new[] { new SpecEdit("A = 1;", "A = 2;"), new SpecEdit("B = 1;", string.Empty) }));
            Assert.That(perturbation.ToString(), Is.EqualTo("T"));
        });
    }

    /// <summary>
    /// The spec-mutation grammar's refusals carry over: a marker inside an
    /// open block, a stray marker or stray text after a closed block, and an
    /// unterminated block are each refused at the line that broke the rule.
    /// </summary>
    [TestCase("--- FIND\nA\n--- REPLACE\nB\n--- FIND\nC\n--- REPLACE\nD\n--- END\n", "T.perturbation line 11 is the marker '--- FIND' inside a '--- REPLACE' block; expected '--- END' first.")]
    [TestCase("--- FIND\nA\n--- FIND\nB\n--- REPLACE\nC\n--- END\n", "T.perturbation line 9 is the marker '--- FIND' inside a '--- FIND' block; expected '--- REPLACE' first.")]
    [TestCase(Block + "--- END\n", "T.perturbation line 12 is the marker '--- END' outside any block; expected '--- FIND' to open the next edit.")]
    [TestCase(Block + "--- REPLACE\nB\n--- END\n", "T.perturbation line 12 is the marker '--- REPLACE' outside any block; expected '--- FIND' to open the next edit.")]
    [TestCase(Block + "trailing\n", "T.perturbation line 12 is text outside any block; expected '--- FIND', a blank line or a '#' comment: 'trailing'")]
    [TestCase("--- FIND\nA\n--- REPLACE\nB\n", "T.perturbation has an unterminated block; expected '--- END'.")]
    [TestCase("--- FIND\n--- REPLACE\nB\n--- END\n", "T.perturbation edit 1 has an empty FIND block.")]
    [TestCase("", "T.perturbation declares no edits.")]
    public void A_block_grammar_violation_is_refused(string body, string expected)
    {
        Assert.Multiple(() =>
        {
            foreach (var crlf in new[] { false, true })
            {
                Assert.That(
                    () => Parse(Header + body, crlf),
                    Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo(expected),
                    crlf ? "CRLF" : "LF");
            }
        });
    }

    /// <summary>
    /// The headers are a closed set, each declared exactly once and non-empty.
    /// A typo such as <c>DETECTOR:</c> is refused rather than ignored, because
    /// an ignored header is a check that silently never ran.
    /// </summary>
    [TestCase("TARGET: x\n" + Header + Block, "T.perturbation line 1 declares the unknown header 'TARGET'. The headers are MODULE, ROW, FILE, DETECTORS, SUMMARY.")]
    [TestCase("ROW: y\n" + Header + Block, "T.perturbation line 3 declares the header 'ROW' twice.")]
    [TestCase("MODULE: M\nROW: Act(t)\nFILE: src/a/B.cs\nDETECTORS: FTests.One\n\n" + Block, "T.perturbation is missing the required header 'SUMMARY:'.")]
    [TestCase("MODULE:\nROW: Act(t)\nFILE: src/a/B.cs\nDETECTORS: FTests.One\nSUMMARY: s\n" + Block, "T.perturbation is missing the required header 'MODULE:'.")]
    [TestCase("--- REPLACE\nB\n--- END\n", "T.perturbation line 1 is the marker '--- REPLACE' before any block; expected '--- FIND' to open the first edit.")]
    [TestCase("not a header\n" + Block, "T.perturbation line 1 is neither a header ('KEY: value'), a comment, nor a block marker: 'not a header'")]
    public void A_header_violation_is_refused(string text, string expected)
    {
        Assert.That(
            () => Parse(text),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo(expected));
    }

    /// <summary>
    /// <c>FILE</c> is one production file, named the way the runner and the
    /// gate both resolve it: repository-relative, under <c>src/</c>, forward
    /// slashes, no relative segments.
    /// </summary>
    [TestCase("test/a/B.cs")]
    [TestCase("src\\a\\B.cs")]
    [TestCase("src/../a/B.cs")]
    [TestCase("src/./B.cs")]
    [TestCase("src//B.cs")]
    [TestCase("/src/a/B.cs")]
    public void A_file_outside_src_or_not_canonical_is_refused(string file)
    {
        Assert.That(
            () => Parse(Header.Replace("src/a/B.cs", file, StringComparison.Ordinal) + Block),
            Throws.TypeOf<InvalidOperationException>().With.Message.Contains("FILE must be a repository-relative path under 'src/'"));
    }

    /// <summary>
    /// Every detector is <c>Fixture.Method</c>, and none is named twice: the
    /// shape is the one the refinement notes cite, so the gate can require the
    /// row to cite it verbatim.
    /// </summary>
    [TestCase("FTests", "declares the detector 'FTests'.")]
    [TestCase("FTests.One, ", "declares the detector ''.")]
    [TestCase("fTests.One", "declares the detector 'fTests.One'.")]
    [TestCase("FTests.One.Two", "declares the detector 'FTests.One.Two'.")]
    [TestCase("FTests.One, FTests.One", "declares the detector 'FTests.One' twice.")]
    public void A_malformed_or_repeated_detector_is_refused(string detectors, string expected)
    {
        Assert.That(
            () => Parse(Header.Replace("FTests.One, FTests.two_b", detectors, StringComparison.Ordinal) + Block),
            Throws.TypeOf<InvalidOperationException>().With.Message.Contains(expected));
    }

    /// <summary>
    /// Edits apply in order, each to the text the previous one left, and the
    /// result keeps <c>\n</c> endings whatever the source used.
    /// </summary>
    [Test]
    public void Apply_runs_every_edit_in_order_against_the_previous_result([Values] bool crlf)
    {
        var perturbation = Parse(Header + "--- FIND\nA = 1;\n--- REPLACE\nA = 2;\n--- END\n--- FIND\nA = 2;\nB\n--- REPLACE\nC\n--- END\n");

        Assert.That(perturbation.Apply(WithEndings("x\nA = 1;\nB\n", crlf)), Is.EqualTo("x\nC\n"));
    }

    /// <summary>
    /// An anchor that no longer matches has drifted, and one that matches
    /// twice is ambiguous; both are refused with the anchor quoted, and so is a
    /// perturbation that changes nothing.
    /// </summary>
    [TestCase("x\n", "expected exactly one match, found 0. The production file no longer contains this text")]
    [TestCase("A = 1;\nA = 1;\n", "expected exactly one match, found 2. The anchor is ambiguous")]
    public void Apply_requires_each_anchor_to_match_exactly_once(string source, string expected)
    {
        Assert.That(
            () => Parse(Header + Block).Apply(source),
            Throws.TypeOf<InvalidOperationException>().With.Message.Contains(expected).And.Message.Contains("A = 1;"));
    }

    /// <summary>An edit whose replacement equals its anchor breaks nothing, so it is refused.</summary>
    [Test]
    public void Apply_refuses_a_perturbation_that_changes_nothing()
    {
        Assert.That(
            () => Parse(Header + "--- FIND\nA\n--- REPLACE\nA\n--- END\n").Apply("A\n"),
            Throws.TypeOf<InvalidOperationException>().With.Message.Contains("leaves src/a/B.cs unchanged"));
    }

    /// <summary>The public entry points refuse null and empty arguments.</summary>
    [Test]
    public void Parse_and_Apply_refuse_null_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => RefinementPerturbation.Parse(null!, Block), Throws.ArgumentNullException);
            Assert.That(() => RefinementPerturbation.Parse(string.Empty, Block), Throws.ArgumentException);
            Assert.That(() => RefinementPerturbation.Parse("T", null!), Throws.ArgumentNullException);
            Assert.That(() => Parse(Header + Block).Apply(null!), Throws.ArgumentNullException);
        });
    }
}
