using Orleans.Lattice.Explorer.Plugins.MyTenant;
using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.MyTenant;

/// <summary>
/// The grant display vocabulary: every state gets a name, a sentence saying what
/// it authorizes, and a badge class, and only the active one reads as live.
/// </summary>
[TestFixture]
public sealed class TenantGrantLabelsTests
{
    [Test]
    public void Every_state_has_a_distinct_label_and_description()
    {
        var states = Enum.GetValues<ExplorerTenantGrantState>();

        Assert.Multiple(() =>
        {
            Assert.That(
                states.Select(TenantGrantLabels.Label).ToArray(),
                Is.Unique,
                "two states that read identically would be indistinguishable in the inbox");

            foreach (var state in states)
            {
                Assert.That(TenantGrantLabels.Description(state), Is.Not.Empty, state.ToString());
            }
        });
    }

    [Test]
    public void Every_non_active_state_says_it_authorizes_nothing()
    {
        Assert.Multiple(() =>
        {
            foreach (var state in Enum.GetValues<ExplorerTenantGrantState>())
            {
                if (state == ExplorerTenantGrantState.Active)
                {
                    continue;
                }

                Assert.That(
                    TenantGrantLabels.Description(state),
                    Does.Contain("authorizes nothing"),
                    state.ToString());
            }
        });
    }

    [Test]
    public void Only_the_active_badge_reads_as_live()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                TenantGrantLabels.BadgeClass(ExplorerTenantGrantState.Active),
                Does.Contain("is-active"));
            Assert.That(
                TenantGrantLabels.BadgeClass(ExplorerTenantGrantState.Pending),
                Does.Contain("is-pending"));
            Assert.That(
                TenantGrantLabels.BadgeClass(ExplorerTenantGrantState.Rejected),
                Does.Contain("is-closed"));
            Assert.That(
                TenantGrantLabels.BadgeClass(ExplorerTenantGrantState.Revoked),
                Does.Contain("is-closed"));
        });
    }

    [Test]
    public void An_undeclared_state_is_rejected_rather_than_rendered()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => TenantGrantLabels.Label((ExplorerTenantGrantState)99),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
            Assert.That(
                () => TenantGrantLabels.Description((ExplorerTenantGrantState)99),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
            Assert.That(
                () => TenantGrantLabels.BadgeClass((ExplorerTenantGrantState)99),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }

    [Test]
    public void Every_operation_combination_has_a_label()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantGrantLabels.Operations(ExplorerTenantGrantAccess.None), Is.EqualTo("None"));
            Assert.That(TenantGrantLabels.Operations(ExplorerTenantGrantAccess.Read), Is.EqualTo("Read"));
            Assert.That(TenantGrantLabels.Operations(ExplorerTenantGrantAccess.Write), Is.EqualTo("Write"));
            Assert.That(
                TenantGrantLabels.Operations(ExplorerTenantGrantAccess.ReadWrite),
                Is.EqualTo("Read and write"));
        });
    }

    [Test]
    public void An_unrecognised_operation_flag_falls_back_to_the_widest_description() =>
        // Fails loud rather than quiet: a flag combination this build does not
        // know must not be described as narrower than it might be.
        Assert.That(
            TenantGrantLabels.Operations((ExplorerTenantGrantAccess)64),
            Is.EqualTo("Read and write"));

    [Test]
    public void The_mid_sentence_form_matches_the_label_and_allocates_nothing()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantGrantLabels.OperationsInSentence(ExplorerTenantGrantAccess.None), Is.EqualTo("no"));
            Assert.That(TenantGrantLabels.OperationsInSentence(ExplorerTenantGrantAccess.Read), Is.EqualTo("read"));
            Assert.That(TenantGrantLabels.OperationsInSentence(ExplorerTenantGrantAccess.Write), Is.EqualTo("write"));
            Assert.That(
                TenantGrantLabels.OperationsInSentence(ExplorerTenantGrantAccess.ReadWrite),
                Is.EqualTo("read and write"));
            Assert.That(
                TenantGrantLabels.OperationsInSentence((ExplorerTenantGrantAccess)64),
                Is.EqualTo("read and write"),
                "an unknown combination is never described as narrower than it might be");

            // Interned literals, so a grant list may call this per row per render.
            Assert.That(
                TenantGrantLabels.OperationsInSentence(ExplorerTenantGrantAccess.ReadWrite),
                Is.SameAs(TenantGrantLabels.OperationsInSentence(ExplorerTenantGrantAccess.ReadWrite)));
        });
    }

    [Test]
    public void Each_direction_labels_its_counterparty_column()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantGrantLabels.Label(TenantGrantDirection.Outbound), Is.EqualTo("Offered to"));
            Assert.That(TenantGrantLabels.Label(TenantGrantDirection.Inbound), Is.EqualTo("Offered by"));
            Assert.That(TenantGrantLabels.Label(TenantGrantDirection.Unrelated), Is.Not.Empty);
            Assert.That(
                () => TenantGrantLabels.Label((TenantGrantDirection)99),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }
}
