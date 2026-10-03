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
        var declared = typeof(LatticeRegistryGrain)
            .GetFields(BindingFlags.NonPublic | BindingFlags.Public
                | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly);

        // The claim is an absence, so the lookup needs a positive control: a
        // BindingFlags drift that returned no field at all would satisfy the
        // absence assertion while proving nothing about the removed field.
        Assert.Multiple(() =>
        {
            Assert.That(declared, Is.Not.Empty,
                "the field lookup must see this grain's own fields, or the absence check below "
                + "passes for the wrong reason.");
            Assert.That(declared.Where(f => f.Name == "EmptyEntry"), Is.Empty);
        });
    }
}
