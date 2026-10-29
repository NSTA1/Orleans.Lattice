using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// The gate over every refinement note's <c>## Deliberate abstraction gaps</c>
/// register (#5156), and the negative controls that prove each of its rules can
/// fail. See <see cref="RefinementGapRegister"/> for the form and why it exists.
/// <para>
/// Toolchain-free: it reads markdown and C# source text only, and runs no TLC.
/// </para>
/// </summary>
[TestFixture]
public sealed class RefinementGapRegisterTests
{
    private static readonly Lazy<RefinementSymbolResolver> Resolver =
        new(RefinementSymbolResolver.ForRepository);

    /// <summary>
    /// Every markdown file under <c>spec/</c> carrying a gap section, or a
    /// heading that looks like one, as a repository-relative path.
    /// </summary>
    public static IEnumerable<string> NotesWithGaps()
    {
        var root = HygieneRepository.FindRepoRoot();
        return HygieneRepository.EnumerateFiles(Path.Combine(root, "spec"), "*.md")
            .Where(path => File.ReadAllText(path).Contains("abstraction gap", StringComparison.OrdinalIgnoreCase))
            .Where(path => File.ReadLines(path).Any(line =>
                line.StartsWith('#') && line.Contains("abstraction gap", StringComparison.OrdinalIgnoreCase)))
            .Select(path => Path.GetRelativePath(root, path).Replace('\\', '/'))
            .Order(StringComparer.Ordinal);
    }

    [TestCaseSource(nameof(NotesWithGaps))]
    public void Every_gap_names_resolvable_production_symbols_or_states_why_it_names_none(string note)
    {
        var markdown = File.ReadAllText(Path.Combine(HygieneRepository.FindRepoRoot(), note));

        var problems = RefinementGapRegister.Check(markdown, note, Resolver.Value);

        Assert.That(problems, Is.Empty, string.Join(Environment.NewLine, problems));
    }

    /// <summary>
    /// The refinement index (contract C6, <see cref="RefinementIndex"/>) parses
    /// the same section independently. Both readers must agree on every gap's
    /// title and production symbols, or a consumer of the index would see a
    /// different register from the one this gate checks.
    /// </summary>
    [TestCaseSource(nameof(NotesWithGaps))]
    public void The_refinement_index_reads_the_same_gaps_as_the_register(string note)
    {
        var markdown = File.ReadAllText(Path.Combine(HygieneRepository.FindRepoRoot(), note));
        var register = RefinementGapRegister.Parse(markdown, note, out _)!
            .Select(gap => $"{gap.Title}: {string.Join(", ", gap.Symbols.Order(StringComparer.Ordinal))}")
            .Order(StringComparer.Ordinal);

        var index = RefinementIndex.BuildModule("Module", note, markdown, Resolver.Value).Gaps
            .Select(gap => $"{gap.Title}: {string.Join(", ", gap.Symbols.Order(StringComparer.Ordinal))}")
            .Order(StringComparer.Ordinal);

        Assert.That(index, Is.EqualTo(register));
    }

    /// <summary>
    /// The gate must not pass by finding nothing: a renamed heading or a broken
    /// discovery would otherwise turn every case above into a vacuous pass.
    /// </summary>
    [Test]
    public void The_register_is_found_in_the_notes_and_every_section_holds_gaps()
    {
        var root = HygieneRepository.FindRepoRoot();
        var notes = NotesWithGaps().ToArray();
        Assert.That(notes, Is.Not.Empty, "no refinement note carries a gap section, so the gate checks nothing");

        var total = 0;
        foreach (var note in notes)
        {
            var entries = RefinementGapRegister.Parse(File.ReadAllText(Path.Combine(root, note)), note, out _);
            Assert.That(entries, Is.Not.Null.And.Not.Empty, $"{note} has no parseable gap");
            total += entries!.Count;
        }

        TestContext.Out.WriteLine($"{notes.Length} notes, {total} gaps");
    }

    // -- Negative controls: each rule, alone, turns a well-formed register red. --

    private static readonly string WellFormed = WellFormedText.ReplaceLineEndings("\n");

    private const string WellFormedText = """
        # Refinement note

        ## Deliberate abstraction gaps

        Intro prose before the list is allowed.

        - **A fold onto a sealed survivor.** The fold's reclaim
          (`ShardRootGrain.ReclaimSlotsAsync`) is the companion module's, and
          `TreeShardSplitGrainTests.Swap_enters_reject_phase_before_setting_shard_map`
          pins it. `ShardOwnershipFold.tla` is not code.
          Production symbols: `ShardRootGrain.ReclaimSlotsAsync`, `TreeShardConsolidationGrain`,
          `ShardRootGrain.TxTerminal`.
        - **Time.** No timers or deadlines.
          No production symbol: the gap is the model's absence of time, not a production mechanism.

        ## Territory owned by other open issues
        """;

    [Test]
    public void Positive_control_a_well_formed_register_has_no_problem()
    {
        var problems = RefinementGapRegister.Check(WellFormed, "note.md", Resolver.Value);
        var entries = RefinementGapRegister.Parse(WellFormed, "note.md", out _)!;

        Assert.Multiple(() =>
        {
            Assert.That(problems, Is.Empty, string.Join(Environment.NewLine, problems));
            Assert.That(entries.Select(e => e.Title), Is.EqualTo(new[] { "A fold onto a sealed survivor", "Time" }));
            Assert.That(entries[0].Symbols, Is.EqualTo(new[]
            {
                "ShardRootGrain.ReclaimSlotsAsync", "TreeShardConsolidationGrain", "ShardRootGrain.TxTerminal",
            }));
            Assert.That(entries[1].NoSymbolReason, Does.StartWith("the gap is the model's absence of time"));
        });
    }

    [Test]
    public void A_note_without_a_gap_section_has_no_register()
    {
        Assert.Multiple(() =>
        {
            Assert.That(RefinementGapRegister.Parse("# Note\n\n## Variable mapping\n", "note.md", out var problems), Is.Null);
            Assert.That(problems, Is.Empty);
        });
    }

    [Test]
    public void The_parser_is_CRLF_safe()
    {
        var problems = RefinementGapRegister.Check(WellFormed.ReplaceLineEndings("\r\n"), "note.md", Resolver.Value);

        Assert.That(problems, Is.Empty, string.Join(Environment.NewLine, problems));
    }

    [TestCase(
        "- **Time.** No timers or deadlines.",
        "- **Time** No timers or deadlines.",
        "bold title ending in a full stop",
        TestName = "Negative_control_a_title_without_its_full_stop_fails")]
    [TestCase(
        "- **Time.** No timers or deadlines.",
        "- Time. No timers or deadlines.",
        "bold title ending in a full stop",
        TestName = "Negative_control_a_bullet_without_a_bold_title_fails")]
    [TestCase(
        "- **Time.** No timers or deadlines.",
        "- **A fold onto a sealed survivor.** No timers or deadlines.",
        "titles are unique",
        TestName = "Negative_control_a_repeated_title_fails")]
    [TestCase(
        "  No production symbol: the gap is the model's absence of time, not a production mechanism.\n",
        "",
        "has no symbol clause",
        TestName = "Negative_control_a_gap_without_a_symbol_clause_fails")]
    [TestCase(
        "No production symbol: the gap is the model's absence of time, not a production mechanism.",
        "No production symbol: none.",
        "without a reason",
        TestName = "Negative_control_a_no_symbol_clause_without_a_reason_fails")]
    [TestCase(
        "- **Time.** No timers or deadlines.",
        "- **Time.** No timers. No production symbol: there is none to name here.",
        "on the title line",
        TestName = "Negative_control_a_clause_on_the_title_line_fails")]
    [TestCase(
        "  No production symbol: the gap is the model's absence of time, not a production mechanism.\n",
        "  No production symbol: the gap is the model's absence of time, not a production mechanism.\n"
        + "  Production symbols: `TreeShardConsolidationGrain`.\n",
        "second or misplaced",
        TestName = "Negative_control_a_second_clause_fails")]
    [TestCase(
        "`ShardRootGrain.TxTerminal`.",
        "ShardRootGrain.TxTerminal.",
        "malformed",
        TestName = "Negative_control_an_unbackticked_symbol_list_fails")]
    [TestCase(
        "`ShardRootGrain.TxTerminal`.",
        "`ShardRootGrain.TxTerminal`, `ShardRootGrain.TxTerminal`.",
        "more than once",
        TestName = "Negative_control_a_repeated_symbol_fails")]
    [TestCase(
        "`ShardRootGrain.TxTerminal`.",
        "`ShardRootGrain.TxTerminal`, `NoSuchProductionTypeAnywhere`.",
        "no type of that name",
        TestName = "Negative_control_a_symbol_whose_type_is_gone_fails")]
    [TestCase(
        "`ShardRootGrain.TxTerminal`.",
        "`ShardRootGrain.TxTerminal`, `ShardRootGrain.NoSuchMemberAnywhere`.",
        "declares nothing called",
        TestName = "Negative_control_a_symbol_whose_member_is_gone_fails")]
    [TestCase(
        "`ShardRootGrain.TxTerminal`.",
        "`ShardRootGrain.TxTerminal`, `NoSuchType.Member`.",
        "no type `NoSuchType`",
        TestName = "Negative_control_a_dotted_symbol_whose_type_is_gone_fails")]
    [TestCase(
        "Production symbols: `ShardRootGrain.ReclaimSlotsAsync`, `TreeShardConsolidationGrain`,",
        "Production symbols: `TreeShardConsolidationGrain`,",
        "in its prose but not in its",
        TestName = "Negative_control_a_prose_symbol_missing_from_the_clause_fails")]
    [TestCase(
        "- **Time.** No timers or deadlines.",
        "- **Time.** No timers, and no `TombstoneCompactionGrain` grace period.",
        "list it in a",
        TestName = "Negative_control_a_no_symbol_gap_that_names_a_symbol_fails")]
    [TestCase(
        "\n## Territory owned",
        "\nTrailing prose that belongs to no gap.\n\n## Territory owned",
        "belongs to no bullet",
        TestName = "Negative_control_prose_after_the_list_fails")]
    [TestCase(
        "  No production symbol: the gap",
        "\n  No production symbol: the gap",
        "one paragraph",
        TestName = "Negative_control_a_blank_line_inside_a_gap_fails")]
    [TestCase(
        "## Deliberate abstraction gaps",
        "## Deliberate abstraction gaps (informal)",
        "not the canonical",
        TestName = "Negative_control_a_renamed_heading_fails")]
    [TestCase(
        "- **Time.** No timers or deadlines.",
        "* **Time.** No timers or deadlines.",
        "belongs to no bullet",
        TestName = "Negative_control_a_star_bullet_after_the_list_fails")]
    public void Negative_control(string find, string replace, string expected)
    {
        Assert.That(WellFormed, Does.Contain(find), "the control's anchor must exist in the well-formed register");
        var mutated = WellFormed.Replace(find, replace, StringComparison.Ordinal);

        var problems = RefinementGapRegister.Check(mutated, "note.md", Resolver.Value);

        Assert.That(problems, Has.Some.Contains(expected), string.Join(Environment.NewLine, problems));
    }

    [Test]
    public void Negative_control_a_section_with_no_gap_bullet_fails()
    {
        const string empty = "# Note\n\n## Deliberate abstraction gaps\n\nOnly prose, no gaps.\n";

        var problems = RefinementGapRegister.Check(empty, "note.md", Resolver.Value);

        Assert.That(problems, Has.Some.Contains("holds no"));
    }

    [Test]
    public void Negative_control_a_star_bullet_before_the_list_fails()
    {
        const string star = "## Deliberate abstraction gaps\n\n* **Time.** No timers.\n";

        var problems = RefinementGapRegister.Check(star, "note.md", Resolver.Value);

        Assert.That(problems, Has.Some.Contains("must start with '- '"));
    }

    [Test]
    public void Prose_symbols_are_production_types_only()
    {
        var symbols = RefinementGapRegister.ProseSymbols(
            "`TreeShardConsolidationGrain`, `ShardRootGrain.ReclaimSlotsAsync`, "
            + "`TreeShardSplitGrainTests.Swap_enters_reject_phase_before_setting_shard_map`, "
            + "`WalMove.tla`, `NoKeyLost`, `rmap`, `#5125`",
            Resolver.Value);

        Assert.That(symbols, Is.EqualTo(new[] { "TreeShardConsolidationGrain", "ShardRootGrain.ReclaimSlotsAsync" }));
    }
}
