using Orleans.Lattice.Samples.Explorer.TaskBoard;

namespace Orleans.Lattice.Samples.Explorer.Tests;

[TestFixture]
public sealed class SampleSeederTests
{
    [TestCase(0, "machine-000")]
    [TestCase(11, "machine-011")]
    public void Machine_keys_are_zero_padded(int index, string expected) =>
        Assert.That(SampleSeeder.MachineKey(index), Is.EqualTo(expected));

    [Test]
    public void Tenant_trees_are_composed_into_the_tenant_namespace()
    {
        Assert.That(SampleSeeder.OrdersTree(SampleIdentities.AcmeTenant), Is.EqualTo("t/acme/orders"));
        Assert.That(SampleSeeder.TaskBoardTree(SampleIdentities.GlobexTenant), Is.EqualTo($"t/globex/a/{TaskBoardApp.Slug}/tasks"));
        Assert.That(SampleSeeder.TaskKey("t-001"), Is.EqualTo("tasks/t-001"));
    }

    [Test]
    public async Task A_basic_token_signs_in_as_its_subject()
    {
        var principal = await new DemoBasicAuthenticator().AuthenticateAsync(
            new LatticeCredential(SampleSeeder.BasicToken(SampleIdentities.AcmeAdmin), DemoBasicAuthenticator.Scheme));

        Assert.That(principal?.SubjectId, Is.EqualTo(SampleIdentities.AcmeAdmin));
    }

    [Test]
    public void The_seeded_task_cards_cover_every_column() =>
        Assert.That(SampleSeeder.AcmeTasks.Select(task => task.Column), Is.EquivalentTo(new[] { "todo", "doing", "done" }));

    [Test]
    public void Seeding_rejects_null_arguments()
    {
        Assert.That(() => SampleSeeder.SeedRegionAsync(null!, staticDirectory: true, _ => { }), Throws.ArgumentNullException);
        Assert.That(() => SampleSeeder.SeedPrimaryAsync(null!, null, _ => { }), Throws.ArgumentNullException);
        Assert.That(() => SampleSeeder.EnrolAsync(null!, null!, _ => { }), Throws.ArgumentNullException);
        Assert.That(() => SampleSeeder.WaitForEnrolmentAsync(null!, "tree", TimeSpan.FromSeconds(1)), Throws.ArgumentNullException);
    }
}
