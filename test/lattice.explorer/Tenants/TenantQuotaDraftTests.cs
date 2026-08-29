using Orleans.Lattice.Explorer.Tenancy;
using Orleans.Lattice.Explorer.Tenants;

namespace Orleans.Lattice.Explorer.Tests.Tenants;

/// <summary>
/// The quota editor: a blank field is unbounded and a typed zero is a real
/// ceiling, in both directions, so authoring cannot silently cap a dimension at
/// nothing or silently uncap one.
/// </summary>
[TestFixture]
public sealed class TenantQuotaDraftTests
{
    [Test]
    public void An_empty_draft_authors_every_dimension_as_unbounded()
    {
        var draft = new TenantQuotaDraft();

        Assert.That(draft.TryBuild(out var limits, out var error), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(error, Is.Null);
            Assert.That(limits.IsUnbounded, Is.True);
            Assert.That(limits.MaxBytes, Is.Null);
            Assert.That(limits.MaxKeys, Is.Null);
            Assert.That(limits.MaxMemoryBytes, Is.Null);
            Assert.That(limits.MaxTreeCount, Is.Null);
            Assert.That(limits.MaxOpsPerSecond, Is.Null);
            Assert.That(limits.BurstPercent, Is.Zero);
        });
    }

    [Test]
    public void Load_renders_an_unbounded_ceiling_as_a_blank_field()
    {
        var draft = new TenantQuotaDraft();
        draft.Load(SampleTenants.Limits());

        Assert.Multiple(() =>
        {
            Assert.That(draft.MaxBytes, Is.EqualTo("1000"));
            Assert.That(draft.MaxKeys, Is.EqualTo("500"));

            // A real cap of nothing renders as the number, not as blank.
            Assert.That(draft.MaxMemoryBytes, Is.EqualTo("0"));

            // Unbounded renders as blank, not as zero.
            Assert.That(draft.MaxTreeCount, Is.Empty);
            Assert.That(draft.MaxOpsPerSecond, Is.Empty);
            Assert.That(draft.BurstPercent, Is.EqualTo("10"));
        });
    }

    [Test]
    public void Ceilings_survive_a_full_round_trip_through_the_editor()
    {
        var original = SampleTenants.Limits();
        var draft = new TenantQuotaDraft();
        draft.Load(original);

        Assert.That(draft.TryBuild(out var rebuilt, out _), Is.True);
        Assert.That(rebuilt, Is.EqualTo(original));
    }

    [Test]
    public void A_typed_zero_is_a_real_ceiling_and_not_unbounded()
    {
        var draft = new TenantQuotaDraft { MaxKeys = "0" };

        Assert.That(draft.TryBuild(out var limits, out _), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(limits.MaxKeys, Is.EqualTo(0L));
            Assert.That(limits.IsUnbounded, Is.False);
        });
    }

    [Test]
    public void A_cleared_field_authors_unbounded_rather_than_zero()
    {
        var draft = new TenantQuotaDraft();
        draft.Load(SampleTenants.Limits());
        draft.MaxBytes = string.Empty;

        Assert.That(draft.TryBuild(out var limits, out _), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(limits.MaxBytes, Is.Null);
            Assert.That(limits.MaxBytes, Is.Not.EqualTo(0L));
        });
    }

    [Test]
    public void Whitespace_is_trimmed_rather_than_rejected()
    {
        var draft = new TenantQuotaDraft { MaxKeys = "  42  " };

        Assert.That(draft.TryBuild(out var limits, out _), Is.True);
        Assert.That(limits.MaxKeys, Is.EqualTo(42L));
    }

    [TestCase("-1")]
    [TestCase("many")]
    [TestCase("1.5")]
    public void A_ceiling_that_is_not_a_non_negative_whole_number_is_refused(string text)
    {
        var draft = new TenantQuotaDraft { MaxBytes = text };

        Assert.That(draft.TryBuild(out var limits, out var error), Is.False);
        Assert.Multiple(() =>
        {
            Assert.That(error, Is.EqualTo(TenantQuotaDraft.InvalidLimitMessage));
            Assert.That(limits, Is.EqualTo(default(ExplorerTenantQuotaLimits)));
        });
    }

    [Test]
    public void A_negative_burst_percent_is_refused_with_its_own_message()
    {
        var draft = new TenantQuotaDraft { BurstPercent = "-5" };

        Assert.That(draft.TryBuild(out _, out var error), Is.False);
        Assert.That(error, Is.EqualTo(TenantQuotaDraft.InvalidBurstMessage));
    }

    [Test]
    public void A_burst_percent_beyond_the_representable_range_is_refused()
    {
        var draft = new TenantQuotaDraft { BurstPercent = "99999999999" };

        Assert.That(draft.TryBuild(out _, out var error), Is.False);
        Assert.That(error, Is.EqualTo(TenantQuotaDraft.InvalidBurstMessage));
    }

    [Test]
    public void A_blank_burst_percent_is_no_headroom_rather_than_an_absence()
    {
        // Unlike a ceiling, the burst percent is not nullable on the control API,
        // so blank means zero headroom rather than "unbounded".
        var draft = new TenantQuotaDraft { BurstPercent = string.Empty };

        Assert.That(draft.TryBuild(out var limits, out _), Is.True);
        Assert.That(limits.BurstPercent, Is.Zero);
    }

    [Test]
    public void Every_dimension_can_be_authored_independently()
    {
        var draft = new TenantQuotaDraft
        {
            MaxBytes = "1",
            MaxKeys = "2",
            MaxMemoryBytes = "3",
            MaxTreeCount = "4",
            MaxOpsPerSecond = "5",
            BurstPercent = "6",
        };

        Assert.That(draft.TryBuild(out var limits, out _), Is.True);
        Assert.That(limits, Is.EqualTo(new ExplorerTenantQuotaLimits
        {
            MaxBytes = 1,
            MaxKeys = 2,
            MaxMemoryBytes = 3,
            MaxTreeCount = 4,
            MaxOpsPerSecond = 5,
            BurstPercent = 6,
        }));
    }
}
