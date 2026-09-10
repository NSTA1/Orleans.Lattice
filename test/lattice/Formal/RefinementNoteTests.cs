namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Tests of the shared <see cref="RefinementNote"/> table reader and of
/// <see cref="RefinementCodeSymbols"/>, driven by hand-written markdown rather
/// than by the real note.
/// <para>
/// The reader is shared on purpose: more than one gate over
/// <c>spec/Refinement.md</c> is planned, and two independently-drifting parsers
/// of the same markdown would end with two gates disagreeing about what the
/// note says. Sharing it makes its failure modes everybody's problem, which is
/// why they are pinned here rather than left to the one caller that exists
/// today.
/// </para>
/// </summary>
[TestFixture]
public sealed class RefinementNoteTests
{
    private const string MinimalNote = """
        # Refinement note

        Intro prose naming [`AtomicCommit.tla`](AtomicCommit.tla).

        ## Variable mapping

        | Spec variable | Protocol role | Code counterpart |
        |---------------|---------------|------------------|
        | `revision` | Monotonic registry revision | `TxRegistryState.DecisionsRevision`, bumped on every write. |
        | `pend[t][k]` | Hidden pending bucket | The leaf `_pendingTx[txid]` bucket (`BPlusLeafGrain.PendingTx`). |

        ## Action mapping

        | Spec action | Protocol step | Code counterpart |
        |-------------|---------------|------------------|
        | `DecideTx(t)` | Record the decision | `AtomicWriteGrain.RecordTerminalDecisionAsync`. |

        ## Property mapping

        | Spec property | Code-level property it abstracts |
        |---------------|----------------------------------|
        | `StrictIsolation` | `AtomicVisibilityGate.ResolveKey` never surfaces uncommitted state (#1584). |

        ## Deliberate abstraction gaps

        - Nothing here is a table.
        """;

    [Test]
    public void Reads_all_three_mapping_sections()
    {
        var tables = RefinementNote.ParseTables(MinimalNote);

        Assert.Multiple(() =>
        {
            Assert.That(tables[RefinementNote.VariableSection].Rows, Has.Count.EqualTo(2));
            Assert.That(tables[RefinementNote.ActionSection].Rows, Has.Count.EqualTo(1));
            Assert.That(tables[RefinementNote.PropertySection].Rows, Has.Count.EqualTo(1));
            Assert.That(
                tables[RefinementNote.VariableSection].Headers,
                Is.EqualTo(new[] { "Spec variable", "Protocol role", "Code counterpart" }));
            Assert.That(tables[RefinementNote.VariableSection].Rows[0].Label, Is.EqualTo("`revision`"));
        });
    }

    /// <summary>
    /// The alignment row is markdown syntax, not data. Counting it as a row
    /// would inflate every row count and give the extractor a line of dashes
    /// to search.
    /// </summary>
    [Test]
    public void Skips_the_alignment_separator_row()
    {
        var rows = RefinementNote.ParseTables(MinimalNote)[RefinementNote.ActionSection].Rows;

        Assert.That(rows.Select(r => r.Label), Is.EqualTo(new[] { "`DecideTx(t)`" }));
    }

    /// <summary>
    /// CRLF safety, which is not decoration on a repository that is developed
    /// on Windows. An un-normalised parse leaves a trailing carriage return on
    /// every last cell, so a comparison against expected text fails silently
    /// and a symbol regex can stop matching at a line end.
    /// </summary>
    [Test]
    public void Parses_identically_from_crlf_and_lf_input()
    {
        var lf = MinimalNote.ReplaceLineEndings("\n");
        var crlf = MinimalNote.ReplaceLineEndings("\r\n");

        var fromLf = RefinementNote.ParseTables(lf);
        var fromCrlf = RefinementNote.ParseTables(crlf);

        Assert.Multiple(() =>
        {
            foreach (var section in RefinementNote.MappingSections)
            {
                Assert.That(
                    fromCrlf[section].Rows.Select(r => string.Join("|", r.Cells)),
                    Is.EqualTo(fromLf[section].Rows.Select(r => string.Join("|", r.Cells))),
                    $"the '{section}' table parsed differently from CRLF than from LF input.");
            }
        });
    }

    /// <summary>
    /// The loud-empty-parse guard. A reader that returned an empty list here
    /// would hand every gate built on it a green run over nothing, which is the
    /// exact failure mode this area exists to eliminate.
    /// </summary>
    [Test]
    public void Throws_when_a_mapping_section_has_no_rows()
    {
        var emptied = MinimalNote.Replace(
            "| `DecideTx(t)` | Record the decision | `AtomicWriteGrain.RecordTerminalDecisionAsync`. |",
            string.Empty,
            StringComparison.Ordinal);

        var error = Assert.Throws<InvalidOperationException>(() => RefinementNote.ParseTables(emptied));

        Assert.That(error!.Message, Does.Contain(RefinementNote.ActionSection));
    }

    [Test]
    public void Throws_when_a_mapping_section_is_missing_entirely()
    {
        var renamed = MinimalNote.Replace(
            "## Property mapping",
            "## Properties",
            StringComparison.Ordinal);

        var error = Assert.Throws<InvalidOperationException>(() => RefinementNote.ParseTables(renamed));

        Assert.That(error!.Message, Does.Contain(RefinementNote.PropertySection));
    }

    [Test]
    public void Extracts_the_dotted_code_symbols_from_every_table()
    {
        var tables = RefinementNote.ParseTables(MinimalNote);
        var symbols = RefinementCodeSymbols
            .Extract(RefinementNote.MappingSections.Select(s => tables[s]))
            .Select(s => s.Text)
            .ToArray();

        Assert.That(
            symbols,
            Is.EquivalentTo(new[]
            {
                "TxRegistryState.DecisionsRevision",
                "BPlusLeafGrain.PendingTx",
                "AtomicWriteGrain.RecordTerminalDecisionAsync",
                "AtomicVisibilityGate.ResolveKey",
            }));
    }

    /// <summary>
    /// The false-positive guard on extraction. The tables are full of
    /// backticked text that is not a code symbol, and manufacturing references
    /// out of it would make the gate fail on a note that is entirely correct -
    /// which is how a gate gets suppressed and stops catching the drift it was
    /// built for.
    /// </summary>
    [TestCase("`phase[t]`", TestName = "Extracts_nothing_from_a_spec_variable")]
    [TestCase("`AtomicCommit.tla`", TestName = "Extracts_nothing_from_a_file_name")]
    [TestCase("`spec/mutations/README.md`", TestName = "Extracts_nothing_from_a_path")]
    [TestCase("`terminal # \"none\"`", TestName = "Extracts_nothing_from_a_spec_expression")]
    [TestCase("`InFlight` / `Committed`", TestName = "Extracts_nothing_from_bare_enum_members")]
    [TestCase("`_recentlyTerminal`", TestName = "Extracts_nothing_from_a_bare_field")]
    [TestCase("plain AtomicWriteGrain.PrepareAsync prose", TestName = "Extracts_nothing_outside_backticks")]
    public void Extracts_no_symbol_from(string cellText)
    {
        var note = $"""
            ## Variable mapping

            | Spec variable | Code counterpart |
            |---------------|------------------|
            | `revision` | {cellText} |

            ## Action mapping

            | Spec action | Code counterpart |
            |-------------|------------------|
            | `DecideTx(t)` | `AtomicWriteGrain.RecordTerminalDecisionAsync`. |

            ## Property mapping

            | Spec property | Code-level property it abstracts |
            |---------------|----------------------------------|
            | `StrictIsolation` | `AtomicVisibilityGate.ResolveKey`. |
            """;

        var tables = RefinementNote.ParseTables(note);
        var fromVariables = RefinementCodeSymbols
            .Extract(new[] { tables[RefinementNote.VariableSection] })
            .Select(s => s.Text);

        Assert.That(fromVariables, Is.Empty);
    }

    /// <summary>
    /// A reference is attributed to the row that made the claim, because a
    /// failure message that only names the symbol leaves the reader to find
    /// which sentence is now wrong.
    /// </summary>
    [Test]
    public void Attributes_each_symbol_to_its_section_row_and_line()
    {
        var tables = RefinementNote.ParseTables(MinimalNote);
        var symbol = RefinementCodeSymbols
            .Extract(new[] { tables[RefinementNote.ActionSection] })
            .Single();

        Assert.Multiple(() =>
        {
            Assert.That(symbol.Section, Is.EqualTo(RefinementNote.ActionSection));
            Assert.That(symbol.Row, Is.EqualTo("`DecideTx(t)`"));
            Assert.That(symbol.TypeName, Is.EqualTo("AtomicWriteGrain"));
            Assert.That(symbol.MemberName, Is.EqualTo("RecordTerminalDecisionAsync"));
            Assert.That(symbol.LineNumber, Is.GreaterThan(0));
        });
    }

    /// <summary>
    /// The real note must still parse, and must still yield references. This
    /// is the on-disk half of the vacuity guard: the tests above all run on
    /// synthetic markdown, so without this one a rewrite of the note into a
    /// form the reader does not recognise would leave them all green.
    /// </summary>
    [Test]
    public void The_real_note_parses_into_three_populated_tables()
    {
        var tables = RefinementNote.ReadTables();

        Assert.Multiple(() =>
        {
            foreach (var section in RefinementNote.MappingSections)
            {
                Assert.That(tables[section].Rows, Is.Not.Empty, $"'{section}' parsed to zero rows.");
            }
        });

        Assert.That(
            RefinementCodeSymbols.Extract(RefinementNote.MappingSections.Select(s => tables[s])),
            Is.Not.Empty,
            "the real spec/Refinement.md yielded no code symbols.");
    }
}
