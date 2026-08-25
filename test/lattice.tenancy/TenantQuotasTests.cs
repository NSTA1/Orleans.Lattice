namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>Unit tests for <see cref="TenantQuotas"/>.</summary>
public sealed class TenantQuotasTests
{
    [Test]
    public void Unbounded_has_every_dimension_null_and_no_burst()
    {
        var quotas = TenantQuotas.Unbounded;

        Assert.Multiple(() =>
        {
            Assert.That(quotas.MaxBytes, Is.Null);
            Assert.That(quotas.MaxKeys, Is.Null);
            Assert.That(quotas.MaxMemoryBytes, Is.Null);
            Assert.That(quotas.MaxTreeCount, Is.Null);
            Assert.That(quotas.MaxOpsPerSecond, Is.Null);
            Assert.That(quotas.BurstPercent, Is.EqualTo(0));
        });
    }

    [Test]
    public void IsUnbounded_is_true_for_the_unbounded_quota()
    {
        Assert.That(TenantQuotas.Unbounded.IsUnbounded, Is.True);
    }

    [Test]
    public void IsUnbounded_ignores_burst_percent()
    {
        var quotas = TenantQuotas.Unbounded with { BurstPercent = 25 };

        Assert.That(quotas.IsUnbounded, Is.True);
    }

    [Test]
    public void IsUnbounded_is_false_when_any_dimension_is_bounded()
    {
        var quotas = new TenantQuotas { MaxKeys = 1000 };

        Assert.That(quotas.IsUnbounded, Is.False);
    }

    [Test]
    public void Value_equality_holds_for_identical_dimensions()
    {
        var a = new TenantQuotas { MaxBytes = 1, MaxKeys = 2, BurstPercent = 10 };
        var b = new TenantQuotas { MaxBytes = 1, MaxKeys = 2, BurstPercent = 10 };

        Assert.That(a, Is.EqualTo(b));
    }
}
