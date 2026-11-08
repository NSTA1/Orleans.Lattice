using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins every acquire of the per-silo replay gate (issue #6006, #5992's G1).
/// A1's work-available re-check reads a positive <see cref="SemaphoreSlim.CurrentCount"/>
/// as a permit nobody is queued for. That holds because the semaphore hands a
/// released permit straight to the earliest parked waiter, and only while every
/// acquire goes through the semaphore: a queued <c>WaitAsync</c> for a replay,
/// or a zero-timeout <c>Wait(0)</c> probe for a background drive, which the
/// model's <c>DriveAdmission</c> requires never to queue. Capacity taken any
/// other way would overtake a parked replay. This test fails on any new
/// acquire site, so each one is reviewed against that premise.
/// </summary>
[TestFixture]
public sealed class ReplayGateAcquireSiteTests
{
    private const string GrainDirectory = "src/lattice/BPlusTree/Grains";

    /// <summary>Names that hold the replay gate in the leaf grain's partial files.</summary>
    private static readonly HashSet<string> ReplayGateNames = new(StringComparer.Ordinal)
    {
        "gate", "permit", "replayPermit", "existing", "Gate", "_replayConcurrencyGate", "ReplayConcurrencyGateForTest",
    };

    /// <summary>Per-activation semaphores that are not the replay gate.</summary>
    private static readonly HashSet<string> OtherSemaphoreNames = new(StringComparer.Ordinal)
    {
        "_splitGate", "_persistWriteGate",
    };

    /// <summary>The reviewed acquire sites, as file | method | call.</summary>
    private static readonly string[] ExpectedAcquireSites =
    [
        "BPlusLeafGrain.Activation.cs | AcquireReplayPermitAsync | gate.WaitAsync(cancellationToken)",
        "BPlusLeafGrain.Activation.cs | TryAcquireStarvationReplayPermit | gate.Wait(0)",
        "BPlusLeafGrain.ReplayPermitNesting.cs | ResumeReplayPermitAsync | lease.Gate.WaitAsync()",
    ];

    [Test]
    public void The_replay_gate_is_acquired_only_at_its_reviewed_queued_waits_and_zero_timeout_probe()
    {
        var files = LeafGrainSources();
        var undeclared = new List<string>();
        var sites = new List<string>();

        foreach (var (name, root) in files)
        {
            foreach (var semaphoreName in DeclaredSemaphoreNames(root))
            {
                if (!ReplayGateNames.Contains(semaphoreName) && !OtherSemaphoreNames.Contains(semaphoreName))
                    undeclared.Add($"{name}: SemaphoreSlim '{semaphoreName}'");
            }

            foreach (var local in root.DescendantNodes().OfType<VariableDeclaratorSyntax>())
            {
                if (local.Initializer is { } initializer
                    && ReadsTheReplayGate(initializer.Value)
                    && !ReplayGateNames.Contains(local.Identifier.ValueText)
                    && local.Parent is VariableDeclarationSyntax { Type.IsVar: true })
                {
                    undeclared.Add($"{name}: 'var {local.Identifier.ValueText}' holds the replay gate");
                }
            }

            foreach (var invocation in root.DescendantNodes().OfType<InvocationExpressionSyntax>())
            {
                if (invocation.Expression is not MemberAccessExpressionSyntax access
                    || access.Name.Identifier.ValueText is not ("Wait" or "WaitAsync")
                    || !IsReplayGateReceiver(access.Expression))
                {
                    continue;
                }

                var method = invocation.Ancestors().OfType<MethodDeclarationSyntax>().FirstOrDefault()?.Identifier.ValueText
                    ?? "<no method>";
                sites.Add($"{name} | {method} | {Normalise(invocation)}");
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(files, Has.Count.GreaterThan(1), "the scan must find the leaf grain's partial files");
            Assert.That(undeclared, Is.Empty,
                "a new name holds a SemaphoreSlim in the leaf grain. Classify it here as the replay gate or as "
                + "another semaphore, so every acquire of the replay gate stays visible to this pin.");
            Assert.That(sites, Is.EquivalentTo(ExpectedAcquireSites),
                "the replay gate's acquire sites changed. Each must take its permit through the semaphore, as a "
                + "queued WaitAsync or a zero-timeout Wait(0) drive probe, so no acquire overtakes a parked replay "
                + "(#5992's G1, the premise of A1's free-count re-check IsReplayPermitFreeWithNoWaiterAhead). "
                + "Review the new site against it, then add it to ExpectedAcquireSites.");
            Assert.That(
                sites.Where(site => site.Contains(".Wait(", StringComparison.Ordinal)),
                Has.All.EndsWith(".Wait(0)"),
                "a synchronous acquire of the replay gate is a drive probe, which never queues or blocks");
        });
    }

    private static List<(string Name, SyntaxNode Root)> LeafGrainSources()
    {
        var directory = Path.Combine(HygieneRepository.FindRepoRoot(), GrainDirectory);
        return Directory.GetFiles(directory, "BPlusLeafGrain*.cs")
            .OrderBy(path => path, StringComparer.Ordinal)
            .Select(path => (Path.GetFileName(path), CSharpSyntaxTree.ParseText(File.ReadAllText(path), path: path).GetRoot()))
            .ToList();
    }

    private static IEnumerable<string> DeclaredSemaphoreNames(SyntaxNode root)
    {
        foreach (var parameter in root.DescendantNodes().OfType<ParameterSyntax>())
        {
            if (IsSemaphore(parameter.Type))
                yield return parameter.Identifier.ValueText;
        }

        foreach (var declaration in root.DescendantNodes().OfType<VariableDeclarationSyntax>())
        {
            if (!IsSemaphore(declaration.Type))
                continue;
            foreach (var variable in declaration.Variables)
                yield return variable.Identifier.ValueText;
        }

        foreach (var property in root.DescendantNodes().OfType<PropertyDeclarationSyntax>())
        {
            if (IsSemaphore(property.Type))
                yield return property.Identifier.ValueText;
        }
    }

    private static bool IsSemaphore(TypeSyntax? type)
    {
        var text = type switch
        {
            NullableTypeSyntax nullable => nullable.ElementType.ToString(),
            null => string.Empty,
            _ => type.ToString(),
        };
        return text is "SemaphoreSlim" or "System.Threading.SemaphoreSlim";
    }

    private static bool ReadsTheReplayGate(ExpressionSyntax value)
        => value.DescendantNodesAndSelf().OfType<IdentifierNameSyntax>().Any(identifier =>
            identifier.Identifier.ValueText is "ResolveReplayConcurrencyGate" or "_replayConcurrencyGate"
                or "ReplayConcurrencyGateForTest");

    private static bool IsReplayGateReceiver(ExpressionSyntax receiver)
    {
        var rightmost = receiver switch
        {
            IdentifierNameSyntax identifier => identifier.Identifier.ValueText,
            MemberAccessExpressionSyntax member => member.Name.Identifier.ValueText,
            PostfixUnaryExpressionSyntax { Operand: IdentifierNameSyntax identifier } => identifier.Identifier.ValueText,
            _ => null,
        };
        return (rightmost is not null && ReplayGateNames.Contains(rightmost)) || ReadsTheReplayGate(receiver);
    }

    private static string Normalise(InvocationExpressionSyntax invocation)
        => string.Concat(invocation.ToString().Where(character => !char.IsWhiteSpace(character)));
}
