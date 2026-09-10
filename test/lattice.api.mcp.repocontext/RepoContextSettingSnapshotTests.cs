namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// The snapshot pairs a resolved value with the default it may have departed from, which
/// is what lets a report answer "did anything set this?" without knowing what did.
/// </summary>
[TestFixture]
public sealed class RepoContextSettingSnapshotTests
{
    [Test]
    public void A_snapshot_carries_the_three_values_it_was_given()
    {
        var snapshot = new RepoContextSettingSnapshot("LATTICE_X", "60s", "900s");

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Name, Is.EqualTo("LATTICE_X"));
            Assert.That(snapshot.Resolved, Is.EqualTo("60s"));
            Assert.That(snapshot.Default, Is.EqualTo("900s"));
        });
    }

    [Test]
    public void Two_snapshots_of_the_same_setting_are_equal()
        => Assert.That(
            new RepoContextSettingSnapshot("LATTICE_X", "60s", "900s"),
            Is.EqualTo(new RepoContextSettingSnapshot("LATTICE_X", "60s", "900s")),
            "value equality is what lets a test assert on an expected set of snapshots "
            + "rather than on the order a host happened to build them in");

    [Test]
    public void A_snapshot_that_differs_only_in_the_default_is_not_equal()
        => Assert.That(
            new RepoContextSettingSnapshot("LATTICE_X", "60s", "900s"),
            Is.Not.EqualTo(new RepoContextSettingSnapshot("LATTICE_X", "60s", "15s")),
            "the default is part of the value, not commentary on it: two snapshots agreeing "
            + "on the resolved value while disagreeing on the default describe different "
            + "deployments and must not compare equal");
}
