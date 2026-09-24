using System.IO;
using System.Reflection;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Orleans.Concurrency;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Pins <see cref="AlwaysInterleaveAttribute"/> on every <see cref="ILattice"/>
/// method whose <c>LatticeGrain</c> implementation awaits an atomic-write saga.
/// </summary>
/// <remarks>
/// <c>LatticeGrain</c> is a bounded stateless worker, and the saga it awaits
/// calls back into <see cref="ILattice"/> on the same tree. A saga-awaiting
/// entry point that did not interleave would hold a worker for the saga's whole
/// duration, and a full pool of such calls self-deadlocks because the saga's
/// callbacks have no worker to run on. The attribute looks removable, so this
/// contract makes removing it a test failure rather than a production hang.
/// <see cref="AtomicWriteWorkerPoolLivenessIntegrationTests"/> is the
/// behavioural counterpart.
/// </remarks>
[TestFixture]
public class LatticeAtomicEntryPointInterleaveContractTests
{
    private const string SagaGrainInterface = "IAtomicWriteGrain";

    [Test]
    public void Every_saga_awaiting_entry_point_is_always_interleave()
    {
        var sagaAwaitingNames = FindSagaAwaitingMethodNames();
        Assert.That(sagaAwaitingNames, Is.Not.Empty,
            $"No LatticeGrain method obtains an {SagaGrainInterface}; the scan is broken or the saga moved.");

        var interfaceMethods = typeof(ILattice).GetMethods();
        var missingFromInterface = sagaAwaitingNames
            .Where(name => interfaceMethods.All(m => m.Name != name))
            .ToList();
        Assert.That(missingFromInterface, Is.Empty,
            "A LatticeGrain method that awaits a saga is not an ILattice method, so the saga is reached "
            + "through a helper. Extend this contract to follow the helper to its ILattice callers.");

        var notInterleaved = interfaceMethods
            .Where(m => sagaAwaitingNames.Contains(m.Name))
            .Where(m => m.GetCustomAttribute<AlwaysInterleaveAttribute>() is null)
            .Select(Describe)
            .ToList();
        Assert.That(notInterleaved, Is.Empty,
            "These ILattice methods await an atomic-write saga but are not [AlwaysInterleave]; "
            + "concurrent calls self-deadlock the LatticeGrain worker pool.");
    }

    [Test]
    public void Every_atomic_bulk_write_overload_is_always_interleave()
    {
        var atomic = typeof(ILattice).GetMethods()
            .Where(m => m.Name is nameof(ILattice.SetManyAtomicAsync) or nameof(ILattice.SetManyAtomicWhereAsync))
            .ToList();

        Assert.That(atomic, Has.Count.EqualTo(5), "Expected three SetManyAtomicAsync and two SetManyAtomicWhereAsync overloads.");
        Assert.That(atomic.Where(m => m.GetCustomAttribute<AlwaysInterleaveAttribute>() is null).Select(Describe),
            Is.Empty);
    }

    private static HashSet<string> FindSagaAwaitingMethodNames()
    {
        var grainsDir = Path.Combine(HygieneRepository.FindRepoRoot(), "src", "lattice", "BPlusTree", "Grains");
        var files = Directory.GetFiles(grainsDir, "LatticeGrain*.cs");
        Assert.That(files, Is.Not.Empty, $"No LatticeGrain source files under {grainsDir}.");

        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var file in files)
        {
            var root = CSharpSyntaxTree.ParseText(File.ReadAllText(file)).GetRoot();
            foreach (var generic in root.DescendantNodes().OfType<GenericNameSyntax>())
            {
                if (generic.Identifier.ValueText != "GetGrain"
                    || generic.TypeArgumentList.Arguments.Count != 1
                    || generic.TypeArgumentList.Arguments[0].ToString() != SagaGrainInterface)
                {
                    continue;
                }

                var method = generic.Ancestors().OfType<MethodDeclarationSyntax>().FirstOrDefault();
                Assert.That(method, Is.Not.Null, $"GetGrain<{SagaGrainInterface}> outside a method in {file}.");
                names.Add(method!.Identifier.ValueText);
            }
        }

        return names;
    }

    private static string Describe(MethodInfo method) =>
        $"{method.Name}({string.Join(", ", method.GetParameters().Select(p => p.ParameterType.Name))})";
}
