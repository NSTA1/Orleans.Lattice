using Orleans.Lattice;
using Orleans.Lattice.Explorer.UI.Access;

namespace Orleans.Lattice.Explorer.Tests.Access;

/// <summary>
/// Unit coverage for <see cref="AccessRuleFormat.Operations"/>, the canonical
/// assignable-operation list that drives every Access-area picker and label, and
/// for <see cref="AccessRuleFormat.OperationsLabel"/>, the compact label built by
/// iterating that list. The consistency guard asserts every non-<see
/// cref="LatticeOperation.None"/> flag is represented, so a future capability can
/// never again silently drop out of the pickers or labels.
/// </summary>
[TestFixture]
public sealed class AccessRuleFormatOperationsTests
{
    [Test]
    public void Operations_contains_replication()
    {
        Assert.That(
            AccessRuleFormat.Operations.Select(o => o.Flag),
            Does.Contain(LatticeOperation.Replication));
    }

    [Test]
    public void Operations_labels_replication_as_replication()
    {
        var option = AccessRuleFormat.Operations.Single(o => o.Flag == LatticeOperation.Replication);

        Assert.That(option.Label, Is.EqualTo("Replication"));
    }

    [Test]
    public void Operations_covers_every_non_none_lattice_operation_flag()
    {
        var represented = AccessRuleFormat.Operations.Select(o => o.Flag).ToHashSet();
        var expected = Enum.GetValues<LatticeOperation>()
            .Where(flag => flag != LatticeOperation.None)
            .ToArray();

        Assert.That(expected, Is.Not.Empty);
        Assert.That(represented, Is.SupersetOf(expected));
    }

    [Test]
    public void OperationsLabel_none_returns_none()
    {
        Assert.That(AccessRuleFormat.OperationsLabel(LatticeOperation.None), Is.EqualTo("none"));
    }

    [Test]
    public void OperationsLabel_replication_alone_renders_friendly_label()
    {
        Assert.That(AccessRuleFormat.OperationsLabel(LatticeOperation.Replication), Is.EqualTo("Replication"));
    }

    [Test]
    public void OperationsLabel_read_and_replication_renders_both_labels()
    {
        var label = AccessRuleFormat.OperationsLabel(LatticeOperation.Read | LatticeOperation.Replication);

        Assert.That(label, Is.EqualTo("Read, Replication"));
    }
}
