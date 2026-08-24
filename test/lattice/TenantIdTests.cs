using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="TenantId"/>: the grammar accepted by
/// <see cref="TenantId.Parse"/> / <see cref="TenantId.TryParse"/>, every
/// invalid-grammar rejection, the reserved <c>default</c> tenant, equality,
/// <see cref="TenantId.ToString"/>, and the Orleans serialization round-trip.
/// </summary>
[TestFixture]
public sealed class TenantIdTests
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp() => _services = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private T RoundTrip<T>(T value)
    {
        var serializer = _services.GetRequiredService<Serializer<T>>();
        return serializer.Deserialize(serializer.SerializeToArray(value));
    }

    [TestCase("a")]
    [TestCase("0")]
    [TestCase("tenant")]
    [TestCase("tenant-1")]
    [TestCase("a-b-c")]
    [TestCase("t0")]
    [TestCase("contoso-prod")]
    [TestCase("default")]
    public void TryParse_valid_id_returns_true_and_preserves_value(string value)
    {
        var ok = TenantId.TryParse(value, out var tenant);

        Assert.That(ok, Is.True);
        Assert.That(tenant.Value, Is.EqualTo(value));
    }

    [Test]
    public void TryParse_id_at_max_length_is_valid()
    {
        var value = new string('a', TenantId.MaxLength);

        Assert.That(TenantId.TryParse(value, out var tenant), Is.True);
        Assert.That(tenant.Value.Length, Is.EqualTo(TenantId.MaxLength));
    }

    [Test]
    public void TryParse_null_returns_false_and_default()
    {
        var ok = TenantId.TryParse(null, out var tenant);

        Assert.That(ok, Is.False);
        Assert.That(tenant, Is.EqualTo(default(TenantId)));
    }

    [TestCase("", TestName = "empty")]
    [TestCase("-lead", TestName = "leading_hyphen")]
    [TestCase("trail-", TestName = "trailing_hyphen")]
    [TestCase("-", TestName = "single_hyphen")]
    [TestCase("Tenant", TestName = "uppercase")]
    [TestCase("TENANT", TestName = "all_uppercase")]
    [TestCase("_tenant", TestName = "leading_underscore")]
    [TestCase("ten_ant", TestName = "embedded_underscore")]
    [TestCase("ten/ant", TestName = "slash")]
    [TestCase("ten.ant", TestName = "dot")]
    [TestCase("ten ant", TestName = "space")]
    [TestCase("t\u00e9nant", TestName = "non_ascii_letter")]
    public void TryParse_invalid_id_returns_false_and_default(string value)
    {
        var ok = TenantId.TryParse(value, out var tenant);

        Assert.That(ok, Is.False);
        Assert.That(tenant, Is.EqualTo(default(TenantId)));
    }

    [Test]
    public void TryParse_id_over_max_length_returns_false()
    {
        var value = new string('a', TenantId.MaxLength + 1);

        Assert.That(TenantId.TryParse(value, out _), Is.False);
    }

    [Test]
    public void Parse_valid_id_returns_tenant()
    {
        var tenant = TenantId.Parse("contoso");

        Assert.That(tenant.Value, Is.EqualTo("contoso"));
    }

    [Test]
    public void Parse_null_throws_argument_null()
    {
        Assert.That(() => TenantId.Parse(null!), Throws.ArgumentNullException);
    }

    [TestCase("")]
    [TestCase("-bad")]
    [TestCase("BAD")]
    [TestCase("sys/tenant")]
    public void Parse_invalid_id_throws_format(string value)
    {
        Assert.That(() => TenantId.Parse(value), Throws.TypeOf<FormatException>());
    }

    [Test]
    public void Default_is_the_reserved_default_tenant()
    {
        Assert.That(TenantId.Default.Value, Is.EqualTo(TenantId.DefaultId));
        Assert.That(TenantId.Default.IsDefault, Is.True);
    }

    [Test]
    public void IsDefault_is_true_only_for_the_default_id()
    {
        Assert.That(TenantId.Parse("default").IsDefault, Is.True);
        Assert.That(TenantId.Parse("contoso").IsDefault, Is.False);
    }

    [Test]
    public void Default_struct_value_is_not_the_reserved_default_tenant()
    {
        var none = default(TenantId);

        Assert.That(none.Value, Is.Null);
        Assert.That(none.IsDefault, Is.False);
        Assert.That(none, Is.Not.EqualTo(TenantId.Default));
    }

    [Test]
    public void Equality_holds_for_equal_ids()
    {
        var a = TenantId.Parse("contoso");
        var b = TenantId.Parse("contoso");

        Assert.That(a, Is.EqualTo(b));
        Assert.That(a == b, Is.True);
        Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
    }

    [Test]
    public void Inequality_holds_for_different_ids()
    {
        var a = TenantId.Parse("contoso");
        var b = TenantId.Parse("fabrikam");

        Assert.That(a, Is.Not.EqualTo(b));
        Assert.That(a != b, Is.True);
    }

    [Test]
    public void ToString_returns_the_id()
    {
        Assert.That(TenantId.Parse("contoso").ToString(), Is.EqualTo("contoso"));
    }

    [Test]
    public void ToString_of_the_no_tenant_value_is_empty()
    {
        Assert.That(default(TenantId).ToString(), Is.EqualTo(string.Empty));
    }

    [Test]
    public void Serializer_round_trips_a_parsed_tenant()
    {
        var original = TenantId.Parse("contoso-prod");

        var copy = RoundTrip(original);

        Assert.That(copy, Is.EqualTo(original));
        Assert.That(copy.Value, Is.EqualTo("contoso-prod"));
    }

    [Test]
    public void Serializer_round_trips_the_default_tenant()
    {
        var copy = RoundTrip(TenantId.Default);

        Assert.That(copy, Is.EqualTo(TenantId.Default));
        Assert.That(copy.IsDefault, Is.True);
    }
}
