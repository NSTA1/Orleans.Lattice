using System.IO;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// #5876 (a5e2e2ef's V3): a descent checks every internal node's declared bound on its
/// path (V1), and shard-wide only the root's identity: it is undecided while a
/// root-identity mutation is in flight or completed during it. That is sound only if every
/// assignment of root-identity state in the shard root is bracketed; this fixture scans the
/// source and fails on any outside a structural-routing scope. Internal child lists and
/// separators are V1's, not this scan's.
/// </summary>
[TestFixture]
public sealed partial class ShardRootGrainDescentVerdictTests
{
    /// <summary>The shard-root state the descent reads to route, assigned only inside a scope or right after a version bump.</summary>
    private static readonly HashSet<string> StructuralFields = new(StringComparer.Ordinal)
    {
        "RootNodeId", "IsRetired", "PendingPromotion", "PendingBulkGraft", "SplitInProgress", "MovedAwaySlots",
    };

    [Test]
    public void Every_routing_structure_mutation_site_is_bracketed()
    {
        var root = HygieneRepository.FindRepoRoot();
        var files = Directory.GetFiles(Path.Combine(root, "src", "lattice", "BPlusTree", "Grains"), "ShardRootGrain*.cs");
        var sites = 0;
        var violations = new List<string>();
        foreach (var file in files)
        {
            var tree = CSharpSyntaxTree.ParseText(File.ReadAllText(file), path: file);
            foreach (var node in tree.GetRoot().DescendantNodes())
            {
                string? what = node switch
                {
                    AssignmentExpressionSyntax { Left: MemberAccessExpressionSyntax target } a
                        when a.Parent is ExpressionStatementSyntax
                        && StructuralFields.Contains(target.Name.Identifier.Text)
                        && target.Expression.ToString() == "state.State" => $"state.State.{target.Name.Identifier.Text} =",
                    _ => null,
                };
                if (what is null)
                {
                    continue;
                }

                sites++;
                if (!IsBracketed(node))
                {
                    var line = node.GetLocation().GetLineSpan().StartLinePosition.Line + 1;
                    violations.Add($"{Path.GetFileName(file)}:{line} {what} in {Enclosing(node)}");
                }
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(sites, Is.GreaterThan(15), "the scan matched no root-identity mutation sites; it has gone vacuous");
            Assert.That(violations, Is.Empty, "routing-structure mutation outside a structural-routing scope (#5876): " + string.Join("; ", violations));
        });
    }

    /// <summary>
    /// Whether <paramref name="site"/> runs inside a structural-routing scope: under a
    /// <c>using (EnterStructuralRouting())</c> statement, after a <c>using var ... =
    /// EnterStructuralRouting()</c> declaration in an enclosing block, or - for a flag set
    /// in one synchronous step - right after <c>NoteStructuralRoutingChange()</c>.
    /// </summary>
    private static bool IsBracketed(SyntaxNode site)
    {
        if (site is AssignmentExpressionSyntax { Parent: ExpressionStatementSyntax statement }
            && statement.Parent is BlockSyntax flagBlock)
        {
            var index = flagBlock.Statements.IndexOf(statement);
            if (index > 0 && flagBlock.Statements[index - 1].ToString().StartsWith("NoteStructuralRoutingChange()", StringComparison.Ordinal))
            {
                return true;
            }
        }

        for (var node = site.Parent; node is not null and not MemberDeclarationSyntax; node = node.Parent)
        {
            if (node is UsingStatementSyntax { Expression: InvocationExpressionSyntax entered }
                && entered.Expression.ToString() == "EnterStructuralRouting")
            {
                return true;
            }

            if (node is BlockSyntax block)
            {
                foreach (var prior in block.Statements)
                {
                    if (prior.SpanStart >= site.SpanStart)
                    {
                        break;
                    }

                    if (prior is LocalDeclarationStatementSyntax { UsingKeyword.RawKind: > 0 } declaration
                        && declaration.Declaration.Variables.Any(v => v.Initializer?.Value.ToString() == "EnterStructuralRouting()"))
                    {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    private static string Enclosing(SyntaxNode node) =>
        node.Ancestors().OfType<MethodDeclarationSyntax>().FirstOrDefault()?.Identifier.Text
        ?? node.Ancestors().OfType<LocalFunctionStatementSyntax>().FirstOrDefault()?.Identifier.Text
        ?? "(no method)";
}
