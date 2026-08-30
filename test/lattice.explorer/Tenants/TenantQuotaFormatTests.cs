using Orleans.Lattice.Explorer.Plugins.Tenancy;
using Orleans.Lattice.Explorer.Plugins.Tenants;

namespace Orleans.Lattice.Explorer.Tests.Tenants;

/// <summary>
/// The quota display vocabulary: the labels, the culture-invariant formatters,
/// the scope captions, and the round trip an operator-typed ceiling makes
/// through the editor.
/// </summary>
[TestFixture]
public sealed class TenantQuotaFormatTests
{
    [Test]
    public void Label_names_every_dimension()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantQuotaFormat.Label(ExplorerTenantQuotaDimensionKind.Bytes), Is.EqualTo("Stored bytes"));
            Assert.That(TenantQuotaFormat.Label(ExplorerTenantQuotaDimensionKind.Keys), Is.EqualTo("Live keys"));
            Assert.That(
                TenantQuotaFormat.Label(ExplorerTenantQuotaDimensionKind.MemoryBytes),
                Is.EqualTo("Resident memory"));
            Assert.That(
                TenantQuotaFormat.Label(ExplorerTenantQuotaDimensionKind.TreeCount),
                Is.EqualTo("Owned trees"));
            Assert.That(
                TenantQuotaFormat.Label(ExplorerTenantQuotaDimensionKind.OpsPerSecond),
                Is.EqualTo("Operations per second"));
        });
    }

    [Test]
    public void Label_undefined_dimension_throws()
    {
        Assert.That(
            () => TenantQuotaFormat.Label((ExplorerTenantQuotaDimensionKind)42),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void IsByteValued_is_true_only_for_the_byte_dimensions()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantQuotaFormat.IsByteValued(ExplorerTenantQuotaDimensionKind.Bytes), Is.True);
            Assert.That(TenantQuotaFormat.IsByteValued(ExplorerTenantQuotaDimensionKind.MemoryBytes), Is.True);
            Assert.That(TenantQuotaFormat.IsByteValued(ExplorerTenantQuotaDimensionKind.Keys), Is.False);
            Assert.That(TenantQuotaFormat.IsByteValued(ExplorerTenantQuotaDimensionKind.TreeCount), Is.False);
            Assert.That(TenantQuotaFormat.IsByteValued(ExplorerTenantQuotaDimensionKind.OpsPerSecond), Is.False);
        });
    }

    [TestCase(0L, "0 B")]
    [TestCase(512L, "512 B")]
    [TestCase(1024L, "1 KB")]
    [TestCase(1536L, "1.5 KB")]
    [TestCase(1048576L, "1 MB")]
    [TestCase(1073741824L, "1 GB")]
    public void Bytes_formats_in_binary_units(long value, string expected)
    {
        Assert.That(TenantQuotaFormat.Bytes(value), Is.EqualTo(expected));
    }

    [Test]
    public void Bytes_saturates_at_the_largest_declared_unit()
    {
        Assert.That(TenantQuotaFormat.Bytes(long.MaxValue), Does.EndWith(" PB"));
    }

    [TestCase(0L, "0")]
    [TestCase(1000L, "1,000")]
    [TestCase(1234567L, "1,234,567")]
    public void Count_groups_invariantly(long value, string expected)
    {
        Assert.That(TenantQuotaFormat.Count(value), Is.EqualTo(expected));
    }

    [Test]
    public void Value_uses_binary_units_for_a_byte_dimension_and_a_count_otherwise()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                TenantQuotaFormat.Value(ExplorerTenantQuotaDimensionKind.Bytes, 2048),
                Is.EqualTo("2 KB"));
            Assert.That(
                TenantQuotaFormat.Value(ExplorerTenantQuotaDimensionKind.Keys, 2048),
                Is.EqualTo("2,048"));
        });
    }

    [Test]
    public void ScopeCaption_distinguishes_a_per_cluster_reading_from_a_global_total()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                TenantQuotaFormat.ScopeCaption(ExplorerTenantQuotaEnforcement.GlobalConverged),
                Is.EqualTo(TenantQuotaFormat.GlobalScopeCaption));
            Assert.That(
                TenantQuotaFormat.ScopeCaption(ExplorerTenantQuotaEnforcement.PerCluster),
                Is.EqualTo(TenantQuotaFormat.PerClusterScopeCaption));

            // The two must never read the same: a per-cluster figure presented as
            // a global total is a lie an operator would act on.
            Assert.That(
                TenantQuotaFormat.PerClusterScopeCaption,
                Is.Not.EqualTo(TenantQuotaFormat.GlobalScopeCaption));
        });
    }

    [Test]
    public void TryParseLimit_blank_is_unbounded_and_not_zero()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantQuotaFormat.TryParseLimit(null, out var fromNull), Is.True);
            Assert.That(fromNull, Is.Null);

            Assert.That(TenantQuotaFormat.TryParseLimit("   ", out var fromBlank), Is.True);
            Assert.That(fromBlank, Is.Null);
        });
    }

    [Test]
    public void TryParseLimit_zero_is_a_real_ceiling_and_not_unbounded()
    {
        Assert.That(TenantQuotaFormat.TryParseLimit("0", out var limit), Is.True);
        Assert.That(limit, Is.EqualTo(0L));
    }

    [TestCase("-1")]
    [TestCase("abc")]
    [TestCase("1.5")]
    public void TryParseLimit_rejects_anything_that_is_not_a_non_negative_whole_number(string text)
    {
        Assert.That(TenantQuotaFormat.TryParseLimit(text, out _), Is.False);
    }

    [Test]
    public void ToEditorText_renders_unbounded_as_blank_so_a_round_trip_cannot_cap_at_zero()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantQuotaFormat.ToEditorText(null), Is.Empty);
            Assert.That(TenantQuotaFormat.ToEditorText(0), Is.EqualTo("0"));
            Assert.That(TenantQuotaFormat.ToEditorText(1024), Is.EqualTo("1024"));
        });
    }

    [Test]
    public void An_unbounded_ceiling_survives_a_round_trip_through_the_editor()
    {
        Assert.That(
            TenantQuotaFormat.TryParseLimit(TenantQuotaFormat.ToEditorText(null), out var reparsed),
            Is.True);

        Assert.That(reparsed, Is.Null);
    }

    [Test]
    public void A_zero_ceiling_survives_a_round_trip_through_the_editor()
    {
        Assert.That(
            TenantQuotaFormat.TryParseLimit(TenantQuotaFormat.ToEditorText(0), out var reparsed),
            Is.True);

        Assert.That(reparsed, Is.EqualTo(0L));
    }
}
