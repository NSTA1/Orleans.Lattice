using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for the quota validation guard on <see cref="TenantRecord.Create"/>
/// and <see cref="TenantRecord.SetQuotas"/>. A tenant's <see cref="TenantQuotas.BurstPercent"/>
/// is authored data stored per record, so it is rejected at the authoring seam
/// (rather than as startup options) when it is negative.
/// </summary>
[TestFixture]
public sealed class TenantRecordQuotaValidationTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

    private static TenantRecord Record() =>
        TenantRecord.Create(Acme, TenantStatus.Active, new TenantQuotas { MaxKeys = 10 }, TenantPlacement.Shared, Clock(10), "w1");

    [Test]
    public void Create_with_negative_burst_percent_throws()
    {
        Assert.That(
            () => TenantRecord.Create(
                Acme,
                TenantStatus.Active,
                new TenantQuotas { BurstPercent = -1 },
                TenantPlacement.Shared,
                Clock(10),
                "w1"),
            Throws.ArgumentException);
    }

    [Test]
    public void Create_with_zero_burst_percent_succeeds()
    {
        var record = TenantRecord.Create(
            Acme,
            TenantStatus.Active,
            new TenantQuotas { BurstPercent = 0 },
            TenantPlacement.Shared,
            Clock(10),
            "w1");

        Assert.That(record.Quotas.BurstPercent, Is.EqualTo(0));
    }

    [Test]
    public void Create_with_positive_burst_percent_succeeds()
    {
        var record = TenantRecord.Create(
            Acme,
            TenantStatus.Active,
            new TenantQuotas { BurstPercent = 50 },
            TenantPlacement.Shared,
            Clock(10),
            "w1");

        Assert.That(record.Quotas.BurstPercent, Is.EqualTo(50));
    }

    [Test]
    public void SetQuotas_with_negative_burst_percent_throws()
    {
        var record = Record();

        Assert.That(
            () => record.SetQuotas(new TenantQuotas { BurstPercent = -5 }, Clock(20), "w1"),
            Throws.ArgumentException);
    }

    [Test]
    public void SetQuotas_with_non_negative_burst_percent_succeeds()
    {
        var record = Record();

        record.SetQuotas(new TenantQuotas { MaxKeys = 20, BurstPercent = 10 }, Clock(20), "w1");

        Assert.Multiple(() =>
        {
            Assert.That(record.Quotas.MaxKeys, Is.EqualTo(20));
            Assert.That(record.Quotas.BurstPercent, Is.EqualTo(10));
        });
    }
}
