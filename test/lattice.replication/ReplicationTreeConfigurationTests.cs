using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage for <see cref="ReplicationTreeConfiguration"/> (#5676): a tree
/// with a merge mode or a static enrolment is configured, and the mode is
/// resolved for the tree id itself rather than through an alias.
/// </summary>
[TestFixture]
public class ReplicationTreeConfigurationTests
{
    [Test]
    public async Task A_tree_with_a_merge_mode_is_configured()
    {
        var context = Substitute.For<ILatticeReplicationContext>();
        context.ResolveMergeMode("alpha").Returns(LatticeMergeMode.LwwRegister);

        Assert.That(
            await ReplicationTreeConfiguration.IsUnconfiguredAsync(context, Options(new()), null, "alpha"),
            Is.False);
    }

    [Test]
    public async Task A_statically_enrolled_tree_without_a_mode_is_configured()
    {
        var context = Substitute.For<ILatticeReplicationContext>();
        context.ResolveMergeMode("alpha").Returns((LatticeMergeMode?)null);
        var options = new LatticeReplicationOptions
        {
            ReplicatedTrees = new Dictionary<string, LatticeMergeMode> { ["alpha"] = LatticeMergeMode.LwwRegister },
        };

        Assert.That(
            await ReplicationTreeConfiguration.IsUnconfiguredAsync(context, Options(options), null, "alpha"),
            Is.False);
    }

    [Test]
    public async Task An_alias_name_of_an_enrolled_tree_is_unconfigured()
    {
        var context = Substitute.For<ILatticeReplicationContext>();
        context.ResolveMergeMode("factory-orders").Returns(LatticeMergeMode.LwwRegister);
        context.ResolveMergeMode("orders").Returns((LatticeMergeMode?)null);
        var options = new LatticeReplicationOptions
        {
            ReplicatedTrees = new Dictionary<string, LatticeMergeMode> { ["factory-orders"] = LatticeMergeMode.LwwRegister },
        };

        Assert.That(
            await ReplicationTreeConfiguration.IsUnconfiguredAsync(context, Options(options), null, "orders"),
            Is.True);
    }

    [Test]
    public void Null_arguments_are_rejected()
    {
        var context = Substitute.For<ILatticeReplicationContext>();
        var options = Options(new());
        Assert.Multiple(() =>
        {
            Assert.That(async () => await ReplicationTreeConfiguration.IsUnconfiguredAsync(null!, options, null, "t"),
                Throws.ArgumentNullException);
            Assert.That(async () => await ReplicationTreeConfiguration.IsUnconfiguredAsync(context, null!, null, "t"),
                Throws.ArgumentNullException);
            Assert.That(async () => await ReplicationTreeConfiguration.IsUnconfiguredAsync(context, options, null, null!),
                Throws.ArgumentNullException);
        });
    }

    private static IOptionsMonitor<LatticeReplicationOptions> Options(LatticeReplicationOptions value)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(value);
        monitor.Get(Arg.Any<string>()).Returns(value);
        return monitor;
    }
}
