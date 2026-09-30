using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for the quota validation guard on <see cref="TenantRecord.Create"/>
/// and <see cref="TenantRecord.SetQuotas"/>. A tenant's <see cref="TenantQuotas.BurstPercent"/>
/// and its bounded ceilings are authored data stored per record, so they are rejected
/// at the authoring seam (rather than as startup options) when negative.
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

    // Regression for #4096: a negative ceiling was persisted, after which the
    // storage evaluator refused every write against it while the rate provider
    // treated a negative MaxOpsPerSecond as unbounded.
    private static IEnumerable<TestCaseData> NegativeCeilings()
    {
        yield return new TestCaseData(new TenantQuotas { MaxBytes = -1 }, nameof(TenantQuotas.MaxBytes)).SetName("{m}(MaxBytes)");
        yield return new TestCaseData(new TenantQuotas { MaxKeys = -1 }, nameof(TenantQuotas.MaxKeys)).SetName("{m}(MaxKeys)");
        yield return new TestCaseData(new TenantQuotas { MaxMemoryBytes = -1 }, nameof(TenantQuotas.MaxMemoryBytes)).SetName("{m}(MaxMemoryBytes)");
        yield return new TestCaseData(new TenantQuotas { MaxTreeCount = -1 }, nameof(TenantQuotas.MaxTreeCount)).SetName("{m}(MaxTreeCount)");
        yield return new TestCaseData(new TenantQuotas { MaxOpsPerSecond = long.MinValue }, nameof(TenantQuotas.MaxOpsPerSecond)).SetName("{m}(MaxOpsPerSecond)");
    }

    [TestCaseSource(nameof(NegativeCeilings))]
    public void Create_with_a_negative_ceiling_throws_naming_the_dimension(TenantQuotas quotas, string dimension)
    {
        Assert.That(
            () => TenantRecord.Create(Acme, TenantStatus.Active, quotas, TenantPlacement.Shared, Clock(10), "w1"),
            Throws.ArgumentException.With.Message.Contains(dimension));
    }

    [TestCaseSource(nameof(NegativeCeilings))]
    public void SetQuotas_with_a_negative_ceiling_throws_and_keeps_the_current_quotas(TenantQuotas quotas, string dimension)
    {
        var record = Record();

        Assert.That(
            () => record.SetQuotas(quotas, Clock(20), "w1"),
            Throws.ArgumentException.With.Message.Contains(dimension));
        Assert.That(record.Quotas, Is.EqualTo(new TenantQuotas { MaxKeys = 10 }));
    }

    [Test]
    public void SetQuotas_with_zero_ceilings_succeeds()
    {
        var record = Record();
        var zero = new TenantQuotas
        {
            MaxBytes = 0,
            MaxKeys = 0,
            MaxMemoryBytes = 0,
            MaxTreeCount = 0,
            MaxOpsPerSecond = 0,
        };

        record.SetQuotas(zero, Clock(20), "w1");

        Assert.That(record.Quotas, Is.EqualTo(zero));
    }
}
