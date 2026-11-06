using System.IO;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// #5876 (a5e2e2ef's D1 and its site list): a descent is decided only when no
/// routing-structure mutation was in flight before or after it and the structural version
/// did not move, which is sound only if every such mutation is bracketed. This fixture
/// scans the shard root's source and fails on any routing-structure mutation site outside
/// a structural-routing scope.
/// </summary>
[TestFixture]
public sealed partial class ShardRootGrainDescentVerdictTests
{
    /// <summary>The shard-root state the descent reads to route, assigned only inside a scope or right after a version bump.</summary>
    private static readonly HashSet<string> StructuralFields = new(StringComparer.Ordinal)
    {
        "RootNodeId", "IsRetired", "PendingPromotion", "PendingBulkGraft", "SplitInProgress", "MovedAwaySlots",
    };

    /// <summary>The node-grain calls that change a child list, a separator, or a node's existence.</summary>
    private static bool IsStructuralCall(string name) =>
        name is "AcceptSplitAsync" or "RemoveChildAsync" or "ClearGrainStateAsync"
        || (name.StartsWith("Initialize", StringComparison.Ordinal) && name.EndsWith("Async", StringComparison.Ordinal));

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
                    InvocationExpressionSyntax { Expression: MemberAccessExpressionSyntax call }
                        when IsStructuralCall(call.Name.Identifier.Text) => call.Name.Identifier.Text,
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
            Assert.That(sites, Is.GreaterThan(20), "the scan matched no routing-structure mutation sites; it has gone vacuous");
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
