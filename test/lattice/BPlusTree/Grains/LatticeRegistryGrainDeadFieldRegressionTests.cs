using System.Linq;
using System.Reflection;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression guard for a dead-code defect in <see cref="LatticeRegistryGrain"/>.
/// The static <c>EmptyEntry</c> field held a pre-serialised empty registry entry
/// that no member ever read, so it did a one-off serialisation of a value nothing
/// consumed. This fixture pins the removal so the unused field cannot creep back.
/// </summary>
[TestFixture]
public class LatticeRegistryGrainDeadFieldRegressionTests
{
    [Test]
    public void LatticeRegistryGrain_HasNoEmptyEntryField()
    {
        var fields = typeof(LatticeRegistryGrain)
            .GetFields(BindingFlags.NonPublic | BindingFlags.Public
                | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
            .Where(f => f.Name == "EmptyEntry");

        Assert.That(fields, Is.Empty);
    }
}
