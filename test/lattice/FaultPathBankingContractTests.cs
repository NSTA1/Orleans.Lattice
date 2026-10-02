using System.IO;
using System.Text.RegularExpressions;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Guards the fault-path banking contract described by issue #2545: a
/// component that advances durable progress in memory, defers the write behind
/// a coalescing window, and can fault part-way, must bank that progress on the
/// fault path itself.
/// </summary>
/// <remarks>
/// <para>
/// The load-bearing fact is that Orleans does not run
/// <c>OnDeactivateAsync</c> when <c>OnActivateAsync</c> throws. A component
/// whose durability work lives only in its graceful teardown hook therefore has
/// no durability at all on the activation-throw path: the work is performed,
/// discarded, and performed again by the next activation, which is
/// self-reinforcing wherever the discarded progress is what would have let the
/// next attempt start further along.
/// </para>
/// <para>
/// Four instances of that idiom have been found and fixed, each by a separate
/// investigation: the leaf cold WAL replay (#2280), the leaf warm WAL replay
/// (#2541), the ANN index-build ingest slice (#2538), and the replication ship
/// cursor (#2544, fixed by #3420). All four are banked on current HEAD. This
/// guard exists so that the fifth fails CI rather than being found the same
/// expensive way, and so that the four already paid for cannot be silently
/// unwired.
/// </para>
/// <para>
/// The detection is deliberately narrow. It recognises the convention the fixed
/// sites already share - a <c>Bank*Async</c> helper awaited from inside a
/// <c>catch</c> - rather than attempting to infer, from arbitrary source, which
/// in-memory field is durable progress and whether some path persists it. A
/// broad predicate here would produce false positives on the many legitimate
/// non-fault bankers (the permit-free pin bank step and the WAL GC scheduler's
/// are both timer-driven), and a noisy gate gets suppressed, which returns it
/// to the vacuous case this fixture is built to avoid.
/// </para>
/// </remarks>
[TestFixture]
public class FaultPathBankingContractTests
{
    /// <summary>
    /// The banking-helper naming convention every known fault-path banker
    /// follows. Matched against the method's own identifier.
    /// </summary>
    private static readonly Regex BankingHelperName = new(
        @"^(?:Try)?Bank\w*Async$",
        RegexOptions.Compiled);

    /// <summary>
    /// The banking helpers that exist specifically to bank on a fault path, and
    /// the issue that paid for each. Every one of these must remain invoked
    /// from inside a <c>catch</c>; unwiring one reopens the defect its issue
    /// closed, which is exactly what happened to the warm arm of the leaf
    /// replay while the fix for its cold sibling sat a few lines away.
    /// </summary>
    /// <remarks>
    /// Keyed by method name rather than by file path so that moving or
    /// re-partitioning a partial class does not read as a regression. Renaming
    /// a helper does fail this list, which is intended: the rename should
    /// update the list in the same commit, and that is a decision somebody
    /// makes rather than a silent loss of coverage.
    /// </remarks>
    private static readonly (string Helper, string Issue, string Site)[] KnownFaultPathBankers =
    {
        ("TryBankColdReplayProgressAsync", "#2280", "BPlusLeafGrain cold WAL replay"),
        ("BankCancelledWarmReplayProgressAsync", "#2541", "BPlusLeafGrain warm WAL replay"),
        ("BankFaultedSliceAsync", "#2538", "DurableVectorIndex ANN ingest slice"),
        ("BankAppliedPrefixAfterApplyFailureAsync", "#2541", "BPlusLeafGrain apply fault"),
        ("BankSliceProgressAsync", "#4123", "LatticeSchemaRemediationGrain remediation slice"),
        ("BankProgressAsync", "#4122", "LatticeOperationRunner coordinated-operation fault and cancel paths"),
        ("BankRelayedProgressAsync", "#4124", "Tracked grain calls: view rebuild/reconcile, tag-index sweep, WAL move"),
    };

    /// <summary>A banking helper declared somewhere under <c>src/</c>.</summary>
    private sealed record BankingDeclaration(string Name, string File, int Line, bool IsPrivate);

    /// <summary>An invocation of a banking helper, and the fault context it sits in.</summary>
    private sealed record BankingInvocation(
        string Name,
        string File,
        int Line,
        CatchClauseSyntax? Catch,
        InvocationExpressionSyntax Node);

    /// <summary>The whole-repository scan result, computed once per fixture run.</summary>
    private sealed record Scan(
        List<BankingDeclaration> Declarations,
        List<BankingInvocation> Invocations,
        int FilesParsed);

    private static readonly Lazy<Scan> Scanned = new(ScanSource);

    /// <summary>
    /// Parses every C# file under <c>src/</c> and collects banking-helper
    /// declarations and invocations, recording for each invocation the nearest
    /// enclosing <c>catch</c> clause, if any.
    /// </summary>
    private static Scan ScanSource()
    {
        var root = HygieneRepository.FindRepoRoot();
        var src = Path.Combine(root, "src");

        var declarations = new List<BankingDeclaration>();
        var invocations = new List<BankingInvocation>();
        var filesParsed = 0;

        foreach (var file in HygieneRepository.EnumerateFiles(src, "*.cs"))
        {
            var text = File.ReadAllText(file);

            // Cheap pre-filter: the overwhelming majority of files mention no
            // banking helper at all, and parsing them would dominate the gate's
            // runtime for nothing.
            if (!text.Contains("Bank", StringComparison.Ordinal)) continue;

            var tree = CSharpSyntaxTree.ParseText(text, path: file);
            var syntaxRoot = tree.GetRoot();
            filesParsed++;

            var relative = Path.GetRelativePath(root, file);

            foreach (var method in syntaxRoot.DescendantNodes().OfType<MethodDeclarationSyntax>())
            {
                if (!BankingHelperName.IsMatch(method.Identifier.ValueText)) continue;

                declarations.Add(new BankingDeclaration(
                    method.Identifier.ValueText,
                    relative,
                    LineOf(tree, method.Identifier),
                    method.Modifiers.Any(SyntaxKind.PrivateKeyword)));
            }

            foreach (var invocation in syntaxRoot.DescendantNodes().OfType<InvocationExpressionSyntax>())
            {
                var name = InvokedName(invocation);
                if (name is null || !BankingHelperName.IsMatch(name)) continue;

                invocations.Add(new BankingInvocation(
                    name,
                    relative,
                    LineOf(tree, invocation),
                    invocation.FirstAncestorOrSelf<CatchClauseSyntax>(),
                    invocation));
            }
        }

        return new Scan(declarations, invocations, filesParsed);
    }

    /// <summary>
    /// The simple identifier a call expression targets, for both
    /// <c>Foo()</c> and <c>receiver.Foo()</c>, or null when the callee is not a
    /// simple name (an invoked delegate expression, say).
    /// </summary>
    private static string? InvokedName(InvocationExpressionSyntax invocation) => invocation.Expression switch
    {
        IdentifierNameSyntax identifier => identifier.Identifier.ValueText,
        MemberAccessExpressionSyntax member => member.Name.Identifier.ValueText,
        MemberBindingExpressionSyntax binding => binding.Name.Identifier.ValueText,
        GenericNameSyntax generic => generic.Identifier.ValueText,
        _ => null,
    };

    private static int LineOf(SyntaxTree tree, SyntaxNode node) =>
        tree.GetLineSpan(node.Span).StartLinePosition.Line + 1;

    private static int LineOf(SyntaxTree tree, SyntaxToken token) =>
        tree.GetLineSpan(token.Span).StartLinePosition.Line + 1;

    /// <summary>
    /// Whether the fault escapes after <paramref name="invocation"/> has
    /// banked, that is, whether some statement following the banking call on
    /// its way out of <paramref name="catchClause"/> throws.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Propagation is not the same as a bare <c>throw;</c>. The leaf's
    /// unaffordable-load arm banks a larger hydration hint and then throws a
    /// <c>LeafSnapshotUnaffordableException</c> wrapping the original fault,
    /// which propagates with a better diagnosis rather than swallowing. An
    /// earlier draft of this guard accepted only <c>throw;</c> and flagged that
    /// site, which is the false positive this method exists to avoid: a gate
    /// that cries wolf on correct code gets suppressed, and a suppressed gate
    /// is vacuous.
    /// </para>
    /// <para>
    /// The walk starts at the banking statement and moves outward to the catch
    /// block, considering at each level only the statements that come STRICTLY
    /// AFTER the one it came from. That ordering is what keeps a throw on a
    /// disjoint branch from counting: in
    /// <c>if (x) { Bank(); } else { throw; }</c> the throw never runs on the
    /// path that banked, and scanning only forward from the <c>if</c> correctly
    /// reports no propagation.
    /// </para>
    /// <para>
    /// A throw nested inside a further <c>catch</c> is not counted as this
    /// catch's propagation, which is what lets the leaf cold-replay site read
    /// correctly: its banking call is wrapped in its own try/catch so a failure
    /// to bank cannot mask the original cancellation, and it is the outer
    /// <c>throw;</c> after that wrapper which propagates.
    /// </para>
    /// </remarks>
    private static bool PropagatesAfterBanking(
        InvocationExpressionSyntax invocation, CatchClauseSyntax catchClause)
    {
        SyntaxNode? node = invocation;

        while (node is not null && node != catchClause.Block)
        {
            if (node is StatementSyntax statement && node.Parent is BlockSyntax block)
            {
                var index = block.Statements.IndexOf(statement);
                if (index >= 0)
                {
                    for (var i = index + 1; i < block.Statements.Count; i++)
                    {
                        if (ThrowsOwnFault(block.Statements[i], catchClause)) return true;
                    }
                }
            }

            node = node.Parent;
        }

        return false;
    }

    /// <summary>
    /// Whether <paramref name="statement"/> throws on behalf of
    /// <paramref name="catchClause"/> rather than on behalf of some nested
    /// catch of its own. Both <c>throw;</c> and <c>throw new T(..., ex)</c>
    /// count: each propagates the failure to the caller.
    /// </summary>
    private static bool ThrowsOwnFault(SyntaxNode statement, CatchClauseSyntax catchClause) =>
        statement is ThrowStatementSyntax
        || statement.DescendantNodes()
            .OfType<ThrowStatementSyntax>()
            .Any(t => t.FirstAncestorOrSelf<CatchClauseSyntax>() == catchClause);

    [Test]
    public void Every_fault_path_banking_site_propagates_the_fault_it_banked_under()
    {
        var scan = Scanned.Value;
        var faultPathSites = scan.Invocations.Where(i => i.Catch is not null).ToList();

        Assert.That(faultPathSites, Is.Not.Empty,
            "Found no banking helper invoked from inside a catch clause anywhere under src/. "
            + "Seven such sites are known to exist (see KnownFaultPathBankers), so the scan has "
            + "drifted from the source and this guard is silently vacuous.");

        var violations = faultPathSites
            .Where(site => !PropagatesAfterBanking(site.Node, site.Catch!))
            .Select(site =>
                $"{site.File}:{site.Line}: {site.Name} is awaited from a catch clause that then "
                + "returns normally.")
            .ToList();

        Assert.That(violations, Is.Empty,
            "A fault-path banking site swallows the fault it banked under. Banking exists to make a "
            + "failure survivable, not to hide it: swallowing converts a failed activation into a "
            + "silently successful one, so the caller proceeds on state the bank may only have "
            + "partially written. Bank, then let the fault out - either with a bare throw or by "
            + "throwing a more specific exception wrapping it. See issue #2545.\n"
            + string.Join("\n", violations));
    }

    [Test]
    public void Every_private_banking_helper_is_still_invoked()
    {
        var scan = Scanned.Value;
        var privateHelpers = scan.Declarations.Where(d => d.IsPrivate).ToList();

        Assert.That(privateHelpers, Is.Not.Empty,
            "Found no private banking helper anywhere under src/. The declaration pattern has "
            + "drifted from the source, so this guard is silently vacuous.");

        var invoked = scan.Invocations.Select(i => i.Name).ToHashSet(StringComparer.Ordinal);

        var violations = privateHelpers
            .Where(d => !invoked.Contains(d.Name))
            .Select(d => $"{d.File}:{d.Line}: {d.Name} is declared but never invoked.")
            .ToList();

        Assert.That(violations, Is.Empty,
            "A private banking helper is declared but never called. C# raises no warning for an "
            + "uncalled private method, so this is the shape a removed banking call leaves behind: "
            + "the helper that was going to bank the coalesced progress survives, and nothing "
            + "reaches it. Either restore the call on the fault path or delete the helper "
            + "deliberately. See issue #2545.\n"
            + string.Join("\n", violations));
    }

    [Test]
    public void Known_fault_path_banking_sites_remain_wired_to_a_fault_path()
    {
        var scan = Scanned.Value;

        var bankedFromCatch = scan.Invocations
            .Where(i => i.Catch is not null)
            .Select(i => i.Name)
            .ToHashSet(StringComparer.Ordinal);

        var violations = KnownFaultPathBankers
            .Where(known => !bankedFromCatch.Contains(known.Helper))
            .Select(known =>
                $"{known.Helper} ({known.Site}, fixed by {known.Issue}) is no longer invoked from "
                + "any catch clause.")
            .ToList();

        Assert.That(violations, Is.Empty,
            "A banking helper that an earlier investigation added to close a real defect is no "
            + "longer wired to a fault path. Orleans does not run OnDeactivateAsync when "
            + "OnActivateAsync throws, so a graceful-teardown flush does not substitute for this: "
            + "removing the call reopens the defect its issue closed. If the helper was renamed, "
            + "update KnownFaultPathBankers in the same commit. See issue #2545.\n"
            + string.Join("\n", violations));
    }

    [Test]
    public void Scan_parses_the_source_it_claims_to_cover()
    {
        var scan = Scanned.Value;

        Assert.Multiple(() =>
        {
            Assert.That(scan.FilesParsed, Is.GreaterThan(0),
                "Parsed no file under src/ mentioning a banking helper. The enumeration or the "
                + "pre-filter has drifted, so every other arm of this fixture passes over an "
                + "empty corpus.");

            Assert.That(scan.Declarations, Is.Not.Empty,
                "Found no banking helper declaration under src/. The declaration pattern has "
                + "drifted from the source.");

            Assert.That(
                scan.Declarations.Select(d => d.Name).ToHashSet(StringComparer.Ordinal),
                Is.SupersetOf(KnownFaultPathBankers.Select(k => k.Helper)),
                "The scan did not find every banking helper this fixture names as known. Either "
                + "a helper was renamed without updating KnownFaultPathBankers, or the scan no "
                + "longer recognises declarations it used to.");
        });
    }

    /// <summary>
    /// Proves the propagation detection actually separates a swallowing catch
    /// from the several shapes that correctly let the fault out, on a synthetic
    /// body. No site under <c>src/</c> violates the contract, so without this
    /// arm the detection would be unfalsifiable by the real corpus and would
    /// pass whether or not it worked.
    /// </summary>
    [Test]
    public void Propagation_detection_separates_a_swallowing_catch_from_the_shapes_that_propagate()
    {
        const string Source = """
            internal sealed class Probe
            {
                private async Task SwallowingAsync()
                {
                    try { await WorkAsync(); }
                    catch (OperationCanceledException) { await BankSwallowedAsync(); }
                }

                private async Task RethrowingAsync()
                {
                    try { await WorkAsync(); }
                    catch (OperationCanceledException) { await BankRethrownAsync(); throw; }
                }

                private async Task WrappingAsync()
                {
                    try { await WorkAsync(); }
                    catch (Exception ex)
                    {
                        await BankWrappedAsync();
                        throw new InvalidOperationException("unaffordable", ex);
                    }
                }

                private async Task NestedAsync()
                {
                    try { await WorkAsync(); }
                    catch (OperationCanceledException)
                    {
                        try { await BankNestedAsync(); }
                        catch (Exception) { }
                        throw;
                    }
                }

                private async Task DisjointAsync()
                {
                    try { await WorkAsync(); }
                    catch (OperationCanceledException)
                    {
                        if (ShouldBank) { await BankDisjointAsync(); }
                        else { throw; }
                    }
                }
            }
            """;

        var root = CSharpSyntaxTree.ParseText(Source).GetRoot();

        var sites = root.DescendantNodes()
            .OfType<InvocationExpressionSyntax>()
            .Select(i => (Name: InvokedName(i), Node: i, Catch: i.FirstAncestorOrSelf<CatchClauseSyntax>()))
            .Where(x => x.Name is not null && BankingHelperName.IsMatch(x.Name))
            .ToDictionary(x => x.Name!, x => (x.Node, x.Catch), StringComparer.Ordinal);

        Assert.That(sites.Keys, Is.EquivalentTo(new[]
            {
                "BankSwallowedAsync", "BankRethrownAsync", "BankWrappedAsync",
                "BankNestedAsync", "BankDisjointAsync",
            }),
            "The synthetic probe did not yield the five banking invocations it declares, so the "
            + "rest of this test proves nothing about the detection.");

        bool Propagates(string helper) =>
            PropagatesAfterBanking(sites[helper].Node, sites[helper].Catch!);

        Assert.Multiple(() =>
        {
            Assert.That(Propagates("BankSwallowedAsync"), Is.False,
                "A catch that banks and then returns normally must register as swallowing. This "
                + "is the violation the guard exists to catch.");

            Assert.That(Propagates("BankRethrownAsync"), Is.True,
                "A catch that banks and then rethrows must register as compliant.");

            Assert.That(Propagates("BankWrappedAsync"), Is.True,
                "A catch that banks and then throws a more specific exception wrapping the "
                + "original must register as compliant. This is the leaf unaffordable-load arm, "
                + "which an earlier draft of this guard wrongly flagged.");

            Assert.That(Propagates("BankNestedAsync"), Is.True,
                "A catch whose banking call is wrapped in its own try/catch, so a failure to bank "
                + "cannot mask the original fault, must still register as compliant on the outer "
                + "rethrow. This is the shape of the leaf cold-replay site; counting the inner "
                + "swallow as the verdict would flag the real source.");

            Assert.That(Propagates("BankDisjointAsync"), Is.False,
                "A throw on the branch NOT taken after banking must not count as propagation. "
                + "Document order alone would wrongly accept this, because the throw appears "
                + "after the banking call in the text while never running on its path.");
        });
    }

    /// <summary>
    /// Proves the orphan detection actually flags an uninvoked helper, on a
    /// synthetic body, for the same reason as above.
    /// </summary>
    [Test]
    public void Orphan_detection_separates_an_uninvoked_helper_from_an_invoked_one()
    {
        const string Source = """
            internal sealed class Probe
            {
                private async Task CallerAsync()
                {
                    try { await WorkAsync(); }
                    catch (Exception) { await BankWiredAsync(); throw; }
                }

                private async Task BankWiredAsync() => await Task.CompletedTask;

                private async Task BankOrphanedAsync() => await Task.CompletedTask;
            }
            """;

        var root = CSharpSyntaxTree.ParseText(Source).GetRoot();

        var declared = root.DescendantNodes()
            .OfType<MethodDeclarationSyntax>()
            .Where(m => BankingHelperName.IsMatch(m.Identifier.ValueText)
                && m.Modifiers.Any(SyntaxKind.PrivateKeyword))
            .Select(m => m.Identifier.ValueText)
            .ToList();

        var invoked = root.DescendantNodes()
            .OfType<InvocationExpressionSyntax>()
            .Select(InvokedName)
            .Where(n => n is not null && BankingHelperName.IsMatch(n))
            .ToHashSet(StringComparer.Ordinal);

        Assert.Multiple(() =>
        {
            Assert.That(declared, Is.EquivalentTo(new[] { "BankWiredAsync", "BankOrphanedAsync" }),
                "The synthetic probe did not yield the two helper declarations it declares, so "
                + "the rest of this test proves nothing about the detection.");

            Assert.That(invoked, Does.Contain("BankWiredAsync"),
                "A helper awaited from a catch must register as invoked.");

            Assert.That(invoked, Does.Not.Contain("BankOrphanedAsync"),
                "A helper nothing calls must register as orphaned. This is the shape a removed "
                + "banking call leaves behind.");
        });
    }
}
