using Orleans.Lattice.Explorer.Tenancy;
using Orleans.Lattice.Explorer.Plugins.Tenants;

namespace Orleans.Lattice.Explorer.Tests.Tenants;

/// <summary>
/// The tenant list row: its lifecycle and default markers, and the headline
/// usage figures, which obey the same rule as the quota surface - an unmeasured
/// figure says so rather than reading as zero.
/// </summary>
[TestFixture]
public sealed class TenantRowTests
{
    [Test]
    public void A_row_carries_its_tenant_identity_and_state()
    {
        var row = TenantRow.From(SampleTenants.Summary(), SampleTenants.Usage());

        Assert.Multiple(() =>
        {
            Assert.That(row.TenantId, Is.EqualTo(SampleTenants.Acme));
            Assert.That(row.Status, Is.EqualTo(ExplorerTenantLifecycle.Active));
            Assert.That(row.StatusLabel, Is.EqualTo("Active"));
            Assert.That(row.IsSuspended, Is.False);
        });
    }

    [Test]
    public void A_suspended_tenant_says_so()
    {
        var row = TenantRow.From(
            SampleTenants.Summary(status: ExplorerTenantLifecycle.Suspended),
            SampleTenants.Usage());

        Assert.Multiple(() =>
        {
            Assert.That(row.IsSuspended, Is.True);
            Assert.That(row.StatusLabel, Is.EqualTo("Suspended"));
        });
    }

    [Test]
    public void The_reserved_default_tenant_is_marked_and_others_are_not()
    {
        var reserved = TenantRow.From(SampleTenants.Summary(isDefault: true), SampleTenants.Usage());
        var ordinary = TenantRow.From(SampleTenants.Summary(), SampleTenants.Usage());

        Assert.Multiple(() =>
        {
            Assert.That(reserved.IsDefault, Is.True);
            Assert.That(reserved.DefaultLabel, Is.EqualTo("Default"));
            Assert.That(ordinary.IsDefault, Is.False);
            Assert.That(ordinary.DefaultLabel, Is.Empty);
        });
    }

    [Test]
    public void Headline_figures_come_from_the_reading()
    {
        var row = TenantRow.From(SampleTenants.Summary(), SampleTenants.Usage());

        Assert.Multiple(() =>
        {
            Assert.That(row.StoredText, Is.EqualTo("250 B"));
            Assert.That(row.KeysText, Is.EqualTo("0"));
            Assert.That(row.TreesText, Is.EqualTo("3"));
        });
    }

    [Test]
    public void An_unmeasured_headline_figure_says_so_rather_than_reading_as_zero()
    {
        var row = TenantRow.From(SampleTenants.Summary(), SampleTenants.Usage(trees: null));

        Assert.Multiple(() =>
        {
            Assert.That(row.TreesText, Is.EqualTo(TenantQuotaFormat.NotMeasuredText));
            Assert.That(row.TreesText, Is.Not.EqualTo("0"));
        });
    }

    [Test]
    public void A_row_with_no_reading_at_all_says_the_usage_was_not_read()
    {
        // A refusal on one tenant's reading must leave the row honest rather than
        // fabricating zeros for it.
        var row = TenantRow.From(SampleTenants.Summary(), usage: null);

        Assert.Multiple(() =>
        {
            Assert.That(row.StoredText, Is.EqualTo(TenantRow.UsageUnavailableText));
            Assert.That(row.KeysText, Is.EqualTo(TenantRow.UsageUnavailableText));
            Assert.That(row.TreesText, Is.EqualTo(TenantRow.UsageUnavailableText));
            Assert.That(row.IsOverQuota, Is.False);

            // The tenant's own identity and state are still known.
            Assert.That(row.TenantId, Is.EqualTo(SampleTenants.Acme));
            Assert.That(row.StatusLabel, Is.EqualTo("Active"));
        });
    }

    [Test]
    public void A_reading_with_a_breach_on_any_dimension_flags_the_row()
    {
        // The sample reading caps resident memory at zero with 64 bytes resident,
        // which is a real breach.
        var row = TenantRow.From(SampleTenants.Summary(), SampleTenants.Usage());

        Assert.That(row.IsOverQuota, Is.True);
    }

    [Test]
    public void A_reading_within_every_ceiling_does_not_flag_the_row()
    {
        var usage = SampleTenants.Usage() with
        {
            MemoryBytes = new ExplorerTenantQuotaDimension { Usage = 0, Limit = 1_000 },
        };

        var row = TenantRow.From(SampleTenants.Summary(), usage);

        Assert.That(row.IsOverQuota, Is.False);
    }

    [Test]
    public void An_unbounded_dimension_never_counts_as_a_breach()
    {
        var usage = SampleTenants.Usage() with
        {
            Bytes = new ExplorerTenantQuotaDimension { Usage = long.MaxValue, Limit = null },
            Keys = default,
            MemoryBytes = default,
            TreeCount = default,
            OpsPerSecond = default,
        };

        var row = TenantRow.From(SampleTenants.Summary(), usage);

        Assert.That(row.IsOverQuota, Is.False);
    }
}
