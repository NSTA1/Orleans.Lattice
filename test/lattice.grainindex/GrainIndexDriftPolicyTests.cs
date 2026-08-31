namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers <see cref="GrainIndexDriftPolicy"/> and the
/// <see cref="GrainIndexOptions.DriftPolicy"/> knob that selects it.
/// </summary>
[TestFixture]
public sealed class GrainIndexDriftPolicyTests
{
    [Test]
    public void Reject_is_the_default_policy_on_a_fresh_options_instance()
    {
        Assert.That(new GrainIndexOptions().DriftPolicy, Is.EqualTo(GrainIndexDriftPolicy.Reject),
            "Defaulting to Reject is what makes a breaking configuration change fail loudly "
            + "instead of serving quietly wrong query results.");
    }

    [Test]
    public void Reject_is_the_zero_value_so_a_default_struct_field_also_rejects()
    {
        Assert.That((int)GrainIndexDriftPolicy.Reject, Is.EqualTo(0),
            "Any code path that leaves the policy at its zero value must land on the safe branch.");
    }

    [Test]
    public void The_policy_set_is_exactly_reject_and_rebuild()
    {
        Assert.That(
            Enum.GetValues<GrainIndexDriftPolicy>(),
            Is.EquivalentTo(new[] { GrainIndexDriftPolicy.Reject, GrainIndexDriftPolicy.Rebuild }));
    }

    [Test]
    public void The_policy_is_settable_per_index()
    {
        var options = new GrainIndexOptions { DriftPolicy = GrainIndexDriftPolicy.Rebuild };

        Assert.That(options.DriftPolicy, Is.EqualTo(GrainIndexDriftPolicy.Rebuild));
    }
}
