using System.Reflection;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Pins each state-name prefix <see cref="RepoContextGrainStorageLockRetryPolicy"/>
/// admits to the state name the core grain actually declares.
/// </summary>
/// <remarks>
/// The host names these stores by string because the core grains and their state
/// types are internal. That makes every prefix a silent coupling: were the core to
/// rename a state, the prefix would simply stop matching, those writes would stop
/// being re-issued, and nothing would fail - the convoy of issue #2419 would return
/// with no test red anywhere. These tests are what converts that into a build
/// failure.
/// </remarks>
[TestFixture]
public sealed class RepoContextGrainStorageLockRetryPolicyStateNameTests
{
    [Test]
    public void The_pin_state_prefix_is_the_core_materialiser_pin_state_name()
    {
        var pinState = typeof(LatticeOptions).Assembly.GetType(
            "Orleans.Lattice.BPlusTree.Grains.WalMaterialiserPinState", throwOnError: true)!;
        var stateName = pinState.GetField("StateName", BindingFlags.Public | BindingFlags.Static)!.GetRawConstantValue();

        Assert.That(RepoContextGrainStorageLockRetryPolicy.PinStateNamePrefix, Is.EqualTo(stateName),
            "The host names the pin store by prefix because the core type is internal. If the core "
            + "renames its state, pin writes would silently stop being re-issued.");
    }

    [Test]
    public void The_leaf_prefix_is_the_core_leaf_grain_state_name()
    {
        Assert.That(
            DeclaredStateName("Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain"),
            Is.EqualTo(RepoContextGrainStorageLockRetryPolicy.LeafStateNamePrefix),
            "The leaf's write is the durable checkpoint advance issue #2419 traced the replay loop "
            + "to. If the core renames it, that write silently stops being re-issued and the loop "
            + "returns.");
    }

    [Test]
    public void The_shard_root_prefix_is_the_core_shard_root_grain_state_name()
    {
        Assert.That(
            DeclaredStateName("Orleans.Lattice.BPlusTree.Grains.ShardRootGrain"),
            Is.EqualTo(RepoContextGrainStorageLockRetryPolicy.ShardRootStateNamePrefix),
            "The shard root is the state the issue #2419 attribution line names and the convoy's "
            + "largest single victim by volume.");
    }

    [Test]
    public void Every_admitted_prefix_matches_a_state_name_some_core_grain_declares()
    {
        var declared = typeof(LatticeOptions).Assembly.GetTypes()
            .SelectMany(t => t.GetConstructors(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
            .SelectMany(c => c.GetParameters())
            .Select(p => p.GetCustomAttribute<PersistentStateAttribute>()?.StateName)
            .Where(n => !string.IsNullOrEmpty(n))
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        Assert.That(declared, Is.Not.Empty,
            "The scan must find the core's persistent states, or this gate is vacuous and every "
            + "prefix below would pass by default.");

        var prefixes = RepoContextGrainStorageLockRetryPolicy.SelfAmplifyingWrites.StateNamePrefixes;
        Assert.That(prefixes, Is.Not.Empty);
        foreach (var prefix in prefixes)
        {
            Assert.That(
                declared.Any(n => n!.StartsWith(prefix, StringComparison.Ordinal)),
                Is.True,
                $"No core grain declares a persistent state starting with '{prefix}', so the host "
                + "is re-issuing writes for a state that no longer exists.");
        }
    }

    private static string DeclaredStateName(string grainTypeName)
    {
        var grain = typeof(LatticeOptions).Assembly.GetType(grainTypeName, throwOnError: true)!;
        var stateName = grain
            .GetConstructors(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
            .SelectMany(c => c.GetParameters())
            .Select(p => p.GetCustomAttribute<PersistentStateAttribute>()?.StateName)
            .FirstOrDefault(n => !string.IsNullOrEmpty(n));

        Assert.That(stateName, Is.Not.Null,
            $"{grainTypeName} declares no [PersistentState], so this test no longer observes what it "
            + "claims to and must be repointed rather than deleted.");
        return stateName!;
    }
}
