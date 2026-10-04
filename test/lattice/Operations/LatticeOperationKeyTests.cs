using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Tests.Operations;

/// <summary>
/// Unit tests for <see cref="LatticeOperationKey"/>: operation-id validation, the
/// tenant-qualified operation and index keys, and their round-trip parsing.
/// Storage safety of the composers is audited separately by the package's
/// grain-key storage-safety contract test.
/// </summary>
[TestFixture]
public sealed class LatticeOperationKeyTests
{
    [TestCase("a")]
    [TestCase("nightly-2026.10.01_full")]
    public void Valid_ids_are_accepted(string id)
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeOperationKey.IsValid(id), Is.True);
            Assert.That(() => LatticeOperationKey.ThrowIfInvalid(id, "id"), Throws.Nothing);
        });
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("has space")]
    [TestCase("slash/id")]
    [TestCase("pipe|id")]
    [TestCase("percent%id")]
    public void Invalid_ids_are_refused(string? id)
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeOperationKey.IsValid(id), Is.False);
            Assert.That(() => LatticeOperationKey.ThrowIfInvalid(id, "id"), Throws.ArgumentException);
        });
    }

    [Test]
    public void An_id_longer_than_the_limit_is_refused()
    {
        Assert.That(LatticeOperationKey.IsValid(new string('a', LatticeOperationKey.MaxOperationIdLength + 1)), Is.False);
        Assert.That(LatticeOperationKey.IsValid(new string('a', LatticeOperationKey.MaxOperationIdLength)), Is.True);
    }

    [Test]
    public void ValidateOrGenerate_returns_a_supplied_valid_id_unchanged()
    {
        Assert.That(LatticeOperationKey.ValidateOrGenerate("nightly-1"), Is.EqualTo("nightly-1"));
    }

    [Test]
    public void ValidateOrGenerate_generates_a_valid_id_when_none_is_supplied()
    {
        var id = LatticeOperationKey.ValidateOrGenerate(null);

        Assert.Multiple(() =>
        {
            Assert.That(LatticeOperationKey.IsValid(id), Is.True);
            Assert.That(id, Has.Length.EqualTo(32));
            Assert.That(LatticeOperationKey.ValidateOrGenerate(null), Is.Not.EqualTo(id));
        });
    }

    [TestCase("")]
    [TestCase("has space")]
    public void ValidateOrGenerate_refuses_a_supplied_invalid_id_naming_the_operation_id_parameter(string id)
    {
        Assert.That(
            () => LatticeOperationKey.ValidateOrGenerate(id),
            Throws.ArgumentException.With.Property(nameof(ArgumentException.ParamName)).EqualTo("operationId"));
    }

    [Test]
    public void New_ids_are_valid_and_distinct()
    {
        var a = LatticeOperationKey.NewId();
        var b = LatticeOperationKey.NewId();

        Assert.Multiple(() =>
        {
            Assert.That(LatticeOperationKey.IsValid(a), Is.True);
            Assert.That(a, Is.Not.EqualTo(b));
        });
    }

    [TestCase("default")]
    [TestCase("acme|corp/eu#1")]
    public void Operation_keys_round_trip_and_never_carry_a_storage_unsafe_character(string tenant)
    {
        var key = LatticeOperationKey.For(tenant, "op-1");

        Assert.Multiple(() =>
        {
            Assert.That(LatticeOperationKey.Parse(key), Is.EqualTo((tenant, "op-1")));
            Assert.That(key, Does.Not.Contain("/").And.Not.Contain("#"));
        });
    }

    [Test]
    public void The_same_id_in_two_tenants_yields_two_keys()
    {
        Assert.That(LatticeOperationKey.For("acme", "op-1"), Is.Not.EqualTo(LatticeOperationKey.For("globex", "op-1")));
    }

    [TestCase("default")]
    [TestCase("acme|corp")]
    public void Index_keys_round_trip(string tenant)
    {
        Assert.That(LatticeOperationKey.ParseIndex(LatticeOperationKey.ForIndex(tenant)), Is.EqualTo(tenant));
    }

    [TestCase("no-separator")]
    [TestCase("trailing|")]
    public void A_malformed_operation_key_is_refused(string key)
    {
        Assert.That(() => LatticeOperationKey.Parse(key), Throws.InstanceOf<FormatException>());
    }

    [TestCase("x|acme")]
    [TestCase("idx")]
    public void A_malformed_index_key_is_refused(string key)
    {
        Assert.That(() => LatticeOperationKey.ParseIndex(key), Throws.InstanceOf<FormatException>());
    }
}
