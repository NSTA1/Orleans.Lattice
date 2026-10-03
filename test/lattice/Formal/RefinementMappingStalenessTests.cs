namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// The staleness gate over every module's refinement note: every production
/// C# symbol the mapping tables name in backticks must still exist in
/// <c>src/</c>.
/// <para>
/// WHAT THIS GATE CHECKS, AND WHAT IT EMPHATICALLY DOES NOT. It checks
/// EXISTENCE, not TRUTH. A green run here means every symbol the note names
/// can still be found; it says nothing whatever about whether the sentence
/// around that symbol is correct. A row is free to name eleven perfectly
/// resolvable symbols and assert something false about how they behave, and
/// this fixture will pass. Pairing each behaviour-asserting row with a
/// mutation that demonstrates the claim is actually detected is separate work
/// and deliberately out of scope here. Read a green run as exactly this much
/// and no more.
/// </para>
/// <para>
/// WHY IT EXISTS. Before it, nothing at all connected the note to the code it
/// describes. A rename, a move, or a deletion in <c>src/</c> left the mapping
/// pointing at a symbol that was gone, and the note went on reading as an
/// authoritative statement about the implementation. The failure was silent and
/// permanent: no build step, test, or review gate would have noticed. That is
/// the same defect class the atomicity audit was convened over, which is an
/// artefact that asserts a correspondence it is structurally unable to check.
/// </para>
/// <para>
/// It is toolchain-free by design, in the same fixture family and for the same
/// reason as <see cref="SpecMutationCatalogueTests"/>: string work over
/// <c>spec/</c> and <c>src/</c>, no JVM, no category, milliseconds, so it runs
/// for every contributor in the deterministic tier rather than only where a
/// toolchain happens to be installed.
/// </para>
/// </summary>
[TestFixture]
public sealed class RefinementMappingStalenessTests
{
    private static readonly RefinementSymbolResolver Resolver = RefinementSymbolResolver.ForRepository();

    private static IReadOnlyList<RefinementCodeSymbol> Symbols(SpecModule module)
    {
        var tables = RefinementNote.ReadTables(module);
        return RefinementCodeSymbols.Extract(RefinementNote.MappingSections.Select(s => tables[s]));
    }

    /// <summary>
    /// The vacuity guard, and it is not optional. Every other test in this
    /// fixture iterates the extracted symbols, so a parser regression that
    /// extracted nothing would turn the whole fixture green while checking
    /// nothing at all - which is precisely the failure this area exists to
    /// eliminate, reintroduced by the fix for it.
    /// <para>
    /// <see cref="RefinementNote"/> already throws when a mapping section
    /// yields zero rows. This adds the second half: rows can parse fine and
    /// still yield no code references if the backtick or symbol pattern
    /// regresses.
    /// </para>
    /// </summary>
    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Modules))]
    public void The_refinement_note_yields_rows_and_symbols_to_check(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        var tables = RefinementNote.ReadTables(module);
        var note = module.Describe(module.RefinementNotePath);

        Assert.Multiple(() =>
        {
            foreach (var section in RefinementNote.MappingSections)
            {
                Assert.That(
                    tables[section].Rows,
                    Is.Not.Empty,
                    $"{note}'s '{section}' table parsed to zero rows, so every gate over it "
                    + "would be vacuous.");
            }
        });

        Assert.That(
            Symbols(module),
            Is.Not.Empty,
            $"extracted no code symbols from {note}'s mapping tables, so this gate would "
            + "pass while checking nothing. Either the note stopped naming code in backticks or "
            + "RefinementCodeSymbols no longer recognises the form it uses.");
    }

    /// <summary>
    /// The gate. Each reference must resolve as a member of the named type, as
    /// a partial-class file <c>Type.Suffix.cs</c>, or as a nested type.
    /// </summary>
    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Modules))]
    public void Every_code_symbol_named_in_the_refinement_mapping_still_exists(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        var symbols = Symbols(module);

        Assert.That(symbols, Is.Not.Empty, "no symbols to check; see the vacuity guard in this fixture.");

        Assert.Multiple(() =>
        {
            foreach (var symbol in symbols)
            {
                var resolution = Resolver.Resolve(symbol);

                Assert.That(
                    RefinementSymbolResolver.IsResolved(resolution),
                    Is.True,
                    Diagnose(module, symbol, resolution));
            }
        });
    }

    /// <summary>
    /// Keeps the partial-class file branch honest. Several of the atomic-commit
    /// note's references are file suffixes rather than members, and that branch
    /// is the one a naive rewrite of this gate would drop - producing confident
    /// false positives on a note that is entirely correct. Taken across every
    /// module, since whether a given module's note names one is a fact about
    /// that module rather than about the resolver.
    /// </summary>
    [Test]
    public void The_partial_class_file_form_is_exercised_by_the_mapping()
    {
        var partials = SpecModuleCatalogue.Repository()
            .SelectMany(Symbols)
            .Where(s => Resolver.Resolve(s) == RefinementSymbolResolution.PartialClassFile)
            .Select(s => s.Text)
            .ToArray();

        Assert.That(
            partials,
            Is.Not.Empty,
            "no symbol in any module's refinement note resolves as a partial-class file suffix, so that branch of "
            + "the resolver is now unexercised by the real mapping. If the note legitimately stopped "
            + "naming one (ShardRootGrain.TxTerminal, ShardRootGrain.Split and BPlusLeafGrain.PendingTx "
            + "are the ones it names today), delete this test together with the branch rather than "
            + "leaving an untested path behind.");
    }

    /// <summary>
    /// The standing negative control. A gate that has never been shown to go
    /// red is not evidence, and this one runs against a tree where every symbol
    /// currently resolves, so its green is otherwise indistinguishable from a
    /// resolver that says yes to everything.
    /// <para>
    /// Both failure directions are checked, because they take different code
    /// paths: a missing type is answered from the type index, a missing member
    /// from the type's own files.
    /// </para>
    /// </summary>
    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Modules))]
    public void The_resolver_reports_symbols_that_do_not_exist(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        var realType = Symbols(module).First().TypeName;

        Assert.Multiple(() =>
        {
            Assert.That(
                Resolver.Resolve("NoSuchTypeInOrleansLattice", "NoSuchMember"),
                Is.EqualTo(RefinementSymbolResolution.UnknownType),
                "the resolver accepted a type that does not exist, so a green run of this fixture "
                + "would mean nothing.");

            Assert.That(
                Resolver.Resolve(realType, "NoSuchMemberOnThisType"),
                Is.EqualTo(RefinementSymbolResolution.UnknownMember),
                $"the resolver accepted a member that '{realType}' does not have, so a green run of "
                + "this fixture would mean nothing.");

            Assert.That(
                Resolver.TypeExists(realType),
                Is.True,
                $"the resolver could not find '{realType}', which the mapping names and which exists; "
                + "the negative control above would then be passing for the wrong reason.");
        });
    }

    private static string Diagnose(SpecModule module, RefinementCodeSymbol symbol, RefinementSymbolResolution resolution)
    {
        var cause = resolution switch
        {
            RefinementSymbolResolution.UnknownType =>
                $"no type named '{symbol.TypeName}' is declared under src/.",
            RefinementSymbolResolution.UnknownMember =>
                $"type '{symbol.TypeName}' exists, but nothing named '{symbol.MemberName}' belongs to it: "
                + "not a member, not a nested type, and no partial-class file "
                + $"'{symbol.TypeName}.{symbol.MemberName}.cs'.",
            _ => "unresolved.",
        };

        return $"{module.Describe(module.RefinementNotePath)} line {symbol.LineNumber} ('{symbol.Section}' table, row "
            + $"'{symbol.Row}') names '{symbol.Text}', which no longer resolves: {cause}"
            + Environment.NewLine
            + "The mapping is now describing code that does not exist. Update the row to name the "
            + "current symbol, or remove the claim. Do not silence this by deleting the reference "
            + "from the note while leaving the surrounding sentence, which would leave the same "
            + "unchecked assertion behind in prose.";
    }
}
