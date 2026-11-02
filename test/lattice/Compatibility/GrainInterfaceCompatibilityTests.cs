using System.Reflection;
using Orleans.Lattice.Testing.Hygiene;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.Compatibility;

/// <summary>
/// Wire compatibility with <c>lattice</c> 10.2.0 (epic #5605): every grain
/// interface method that release shipped still exists here with the same name,
/// parameter types and return type, so a 10.2.0 silo in a mixed-version cluster
/// can call every method it knows. New behaviour arrives only through new methods.
/// </summary>
/// <remarks>
/// The baseline, <c>GrainInterfaces.lattice-v10.2.0.txt</c> beside this file, was
/// generated from the tagged build. The only methods absent today are the ones
/// #5217 removed deliberately because 10.2.0 never calls them (their calls were
/// elided before that release); they are listed in <see cref="RemovedByIssue5217"/>,
/// and the test fails if that list stops matching what is actually absent.
/// </remarks>
[TestFixture]
public sealed class GrainInterfaceCompatibilityTests
{
    private static readonly string[] RemovedByIssue5217 =
    {
        "Orleans.Lattice.BPlusTree.Grains.IWalShardGrain::GetLiveEntryCountAsync(System.Threading.CancellationToken)->System.Threading.Tasks.Task`1[System.Int64]",
        "Orleans.Lattice.BPlusTree.IBPlusInternalGrain::GetRightmostChildAsync()->System.Threading.Tasks.Task`1[Orleans.Runtime.GrainId]",
        "Orleans.Lattice.BPlusTree.IBPlusLeafGrain::FreezeProjectionAsync(System.Threading.CancellationToken)->System.Threading.Tasks.Task`1[Orleans.Lattice.BPlusTree.State.LeafBaselineFreeze]",
        "Orleans.Lattice.BPlusTree.IBPlusLeafGrain::GetDeltaSinceForSlotsAsync(Orleans.Lattice.VersionVector,System.Int32[],System.Int32)->System.Threading.Tasks.Task`1[Orleans.Lattice.Primitives.StateDelta]",
        "Orleans.Lattice.BPlusTree.IBPlusLeafGrain::GetKeyRangeAsync()->System.Threading.Tasks.Task`1[Orleans.Lattice.LeafKeyRange]",
        "Orleans.Lattice.BPlusTree.IBPlusLeafGrain::GetProjectionCheckpointOffsetAsync()->System.Threading.Tasks.Task`1[System.Int64]",
        "Orleans.Lattice.BPlusTree.IBPlusLeafGrain::RebuildProjectionFromWalAsync()->System.Threading.Tasks.Task",
        "Orleans.Lattice.BPlusTree.IShardRootGrain::GetHotnessAsync()->System.Threading.Tasks.Task`1[Orleans.Lattice.BPlusTree.ShardHotness]",
        "Orleans.Lattice.BPlusTree.IShardRootGrain::HasPendingBulkOperationAsync()->System.Threading.Tasks.Task`1[System.Boolean]",
        "Orleans.Lattice.BPlusTree.ITxRegistryGrain::HasUndecidedParticipantsAsync(System.Guid)->System.Threading.Tasks.Task`1[System.Boolean]",
    };

    private static IReadOnlyList<string> Baseline()
    {
        var path = Path.Combine(HygieneRepository.FindRepoRoot(), "test", "lattice", "Compatibility", "GrainInterfaces.lattice-v10.2.0.txt");
        return File.ReadAllLines(path)
            .Where(static l => l.Length > 0 && !l.StartsWith('#'))
            .ToList();
    }

    private static HashSet<string> Current()
    {
        var signatures = new HashSet<string>(StringComparer.Ordinal);
        foreach (var type in typeof(ILattice).Assembly.GetTypes())
        {
            if (!type.IsInterface || !typeof(IAddressable).IsAssignableFrom(type))
            {
                continue;
            }

            foreach (var method in type.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly))
            {
                var generic = method.IsGenericMethod ? "``" + method.GetGenericArguments().Length : string.Empty;
                var parameters = string.Join(",", method.GetParameters().Select(static p => Format(p.ParameterType)));
                signatures.Add($"{type.FullName}::{method.Name}{generic}({parameters})->{Format(method.ReturnType)}");
            }
        }

        return signatures;
    }

    private static string Format(Type type)
    {
        if (type.IsByRef)
        {
            return Format(type.GetElementType()!) + "&";
        }

        if (type.IsArray)
        {
            return Format(type.GetElementType()!) + "[" + new string(',', type.GetArrayRank() - 1) + "]";
        }

        if (type.IsGenericParameter)
        {
            return type.Name;
        }

        if (type.IsGenericType && !type.IsGenericTypeDefinition)
        {
            return type.GetGenericTypeDefinition().FullName + "[" + string.Join(",", type.GetGenericArguments().Select(Format)) + "]";
        }

        return type.FullName ?? type.Name;
    }

    [Test]
    public void The_baseline_is_the_whole_shipped_surface()
    {
        Assert.That(Baseline(), Has.Count.GreaterThan(600),
            "vacuousness guard: the 10.2.0 baseline must list the shipped grain interfaces");
    }

    [Test]
    public void Every_lattice_10_2_0_grain_interface_method_still_exists_with_its_signature()
    {
        var current = Current();
        var missing = Baseline().Where(s => !current.Contains(s)).Except(RemovedByIssue5217, StringComparer.Ordinal).ToList();

        Assert.That(missing, Is.Empty,
            "A grain interface method shipped in lattice 10.2.0 is gone or changed signature; a 10.2.0 silo in a "
            + "mixed-version cluster would call it and fail. Add new behaviour through new methods instead:"
            + Environment.NewLine + string.Join(Environment.NewLine, missing));
    }

    [Test]
    public void The_issue_5217_removals_are_exactly_the_methods_absent_today()
    {
        var current = Current();
        var baseline = Baseline();

        Assert.Multiple(() =>
        {
            Assert.That(RemovedByIssue5217, Is.SubsetOf(baseline), "every listed removal shipped in 10.2.0");
            Assert.That(RemovedByIssue5217.Where(current.Contains), Is.Empty,
                "a listed removal is present again: drop it from the list");
        });
    }

    [Test]
    public void The_append_fence_adds_only_new_methods()
    {
        var current = Current();

        Assert.That(current, Has.Some.StartsWith("Orleans.Lattice.BPlusTree.Grains.IWalShardGrain::AppendBatchStampedAsync("),
            "vacuousness guard: the scan sees the methods this member added");
    }
}
