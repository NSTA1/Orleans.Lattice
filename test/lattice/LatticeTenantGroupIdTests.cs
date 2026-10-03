using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeTenantGroupId"/>: the reserved
/// <c>t/{tenant}/{name}</c> tenant-group grammar accepted by
/// <see cref="LatticeTenantGroupId.Parse"/>, <see cref="LatticeTenantGroupId.TryParse"/>,
/// <see cref="LatticeTenantGroupId.Compose"/> and
/// <see cref="LatticeTenantGroupId.IsTenantGroupId"/>; refusal of the reserved
/// <c>default</c> tenant; the length and ASCII-only bounds of the name; equality;
/// the Orleans serialization round-trip; and the allocation-free shape test.
/// </summary>
[TestFixture]
public sealed class LatticeTenantGroupIdTests
{
    private static readonly TenantId Contoso = TenantId.Parse("contoso");

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

    // ----- Valid grammar -----

    [TestCase("t/contoso/admins", "contoso", "admins")]
    [TestCase("t/a/b", "a", "b")]
    [TestCase("t/contoso-eu/finance.readers", "contoso-eu", "finance.readers")]
    [TestCase("t/c0/team_1-x.y", "c0", "team_1-x.y")]
    [TestCase("t/contoso/.", "contoso", ".")]
    [TestCase("t/contoso/-", "contoso", "-")]
    [TestCase("t/contoso/0", "contoso", "0")]
    public void TryParse_accepts_a_well_formed_id(string value, string tenant, string name)
    {
        var parsed = LatticeTenantGroupId.TryParse(value, out var groupId);

        Assert.Multiple(() =>
        {
            Assert.That(parsed, Is.True);
            Assert.That(groupId.Tenant, Is.EqualTo(TenantId.Parse(tenant)));
            Assert.That(groupId.Name, Is.EqualTo(name));
            Assert.That(groupId.Value, Is.SameAs(value));
            Assert.That(LatticeTenantGroupId.IsTenantGroupId(value), Is.True);
        });
    }

    [Test]
    public void Parse_returns_the_parsed_id()
    {
        var groupId = LatticeTenantGroupId.Parse("t/contoso/admins");

        Assert.That(groupId.Tenant, Is.EqualTo(Contoso));
        Assert.That(groupId.Name, Is.EqualTo("admins"));
        Assert.That(groupId.Value, Is.EqualTo("t/contoso/admins"));
    }

    // ----- Invalid grammar -----

    [TestCase("")]
    [TestCase("admins")]
    [TestCase("t/")]
    [TestCase("t/contoso")]
    [TestCase("t/contoso/")]
    [TestCase("t//admins")]
    [TestCase("T/contoso/admins")]
    [TestCase(" t/contoso/admins")]
    [TestCase("t/contoso/admins ")]
    [TestCase("t/Contoso/admins")]
    [TestCase("t/-contoso/admins")]
    [TestCase("t/contoso-/admins")]
    [TestCase("t/con_toso/admins")]
    [TestCase("t/contoso/Admins")]
    [TestCase("t/contoso/a b")]
    [TestCase("t/contoso/a/b")]
    [TestCase("t/contoso/a:b")]
    [TestCase("t/contoso/a*")]
    [TestCase("t/contoso/*")]
    [TestCase("t/contoso/caf\u00e9")]
    [TestCase("t/contoso/\uff41dmins")]
    [TestCase("t/c\u00f6ntoso/admins")]
    [TestCase("x/contoso/admins")]
    [TestCase("sys-membership-groups")]
    public void TryParse_rejects_a_malformed_id(string value)
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeTenantGroupId.TryParse(value, out var groupId), Is.False);
            Assert.That(groupId, Is.EqualTo(default(LatticeTenantGroupId)));
            Assert.That(LatticeTenantGroupId.IsTenantGroupId(value), Is.False);
            Assert.That(() => LatticeTenantGroupId.Parse(value), Throws.TypeOf<FormatException>());
        });
    }

    [Test]
    public void TryParse_rejects_null()
    {
        Assert.That(LatticeTenantGroupId.TryParse(null, out var groupId), Is.False);
        Assert.That(groupId, Is.EqualTo(default(LatticeTenantGroupId)));
    }

    [Test]
    public void IsTenantGroupId_rejects_null()
    {
        Assert.That(LatticeTenantGroupId.IsTenantGroupId(null), Is.False);
    }

    [Test]
    public void Parse_null_throws()
    {
        Assert.That(() => LatticeTenantGroupId.Parse(null!), Throws.ArgumentNullException);
    }

    // ----- The reserved default tenant (D16) -----

    [Test]
    public void The_default_tenant_has_no_tenant_groups()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeTenantGroupId.TryParse("t/default/admins", out _), Is.False);
            Assert.That(LatticeTenantGroupId.IsTenantGroupId("t/default/admins"), Is.False);
            Assert.That(() => LatticeTenantGroupId.Parse("t/default/admins"), Throws.TypeOf<FormatException>());
            Assert.That(
                () => LatticeTenantGroupId.Compose(TenantId.Default, "admins"),
                Throws.ArgumentException.With.Property(nameof(ArgumentException.ParamName)).EqualTo("tenant"));
        });
    }

    [Test]
    public void A_tenant_merely_prefixed_default_is_an_ordinary_tenant()
    {
        Assert.That(LatticeTenantGroupId.IsTenantGroupId("t/default-eu/admins"), Is.True);
    }

    // ----- Maximum lengths -----

    [Test]
    public void A_name_of_the_maximum_length_is_accepted()
    {
        var name = new string('a', LatticeTenantGroupId.MaxNameLength);

        Assert.That(LatticeTenantGroupId.IsTenantGroupId("t/contoso/" + name), Is.True);
        Assert.That(LatticeTenantGroupId.Compose(Contoso, name).Name, Is.EqualTo(name));
    }

    [Test]
    public void A_name_one_over_the_maximum_length_is_rejected()
    {
        var name = new string('a', LatticeTenantGroupId.MaxNameLength + 1);

        Assert.That(LatticeTenantGroupId.IsTenantGroupId("t/contoso/" + name), Is.False);
        Assert.That(() => LatticeTenantGroupId.Compose(Contoso, name), Throws.ArgumentException);
    }

    [Test]
    public void A_tenant_of_the_maximum_length_is_accepted_and_one_over_is_rejected()
    {
        var tenant = new string('a', TenantId.MaxLength);
        var tooLong = new string('a', TenantId.MaxLength + 1);

        Assert.That(LatticeTenantGroupId.IsTenantGroupId($"t/{tenant}/admins"), Is.True);
        Assert.That(LatticeTenantGroupId.IsTenantGroupId($"t/{tooLong}/admins"), Is.False);
    }

    [Test]
    public void MaxNameLength_is_63()
    {
        Assert.That(LatticeTenantGroupId.MaxNameLength, Is.EqualTo(63));
    }

    // ----- ASCII-only alphabet -----

    [Test]
    public void Every_character_of_the_name_alphabet_is_accepted_and_nothing_else_is()
    {
        for (var c = (char)0; c < 0x250; c++)
        {
            var expected = c is (>= 'a' and <= 'z') or (>= '0' and <= '9') or '-' or '_' or '.';
            var id = "t/contoso/" + c;

            Assert.That(LatticeTenantGroupId.IsTenantGroupId(id), Is.EqualTo(expected), $"U+{(int)c:X4}");
        }
    }

    // ----- Compose -----

    [Test]
    public void Compose_builds_the_reserved_grammar()
    {
        var groupId = LatticeTenantGroupId.Compose(Contoso, "finance.readers");

        Assert.Multiple(() =>
        {
            Assert.That(groupId.Value, Is.EqualTo("t/contoso/finance.readers"));
            Assert.That(groupId.Tenant, Is.EqualTo(Contoso));
            Assert.That(groupId.Name, Is.EqualTo("finance.readers"));
        });
    }

    [Test]
    public void Compose_and_Parse_round_trip_to_an_equal_id()
    {
        var composed = LatticeTenantGroupId.Compose(Contoso, "admins");

        Assert.That(LatticeTenantGroupId.Parse(composed.Value), Is.EqualTo(composed));
    }

    [Test]
    public void Compose_rejects_the_uninitialised_tenant()
    {
        Assert.That(
            () => LatticeTenantGroupId.Compose(default, "admins"),
            Throws.ArgumentException.With.Property(nameof(ArgumentException.ParamName)).EqualTo("tenant"));
    }

    [Test]
    public void Compose_rejects_a_null_name()
    {
        Assert.That(() => LatticeTenantGroupId.Compose(Contoso, null!), Throws.ArgumentNullException);
    }

    [TestCase("")]
    [TestCase("Admins")]
    [TestCase("a/b")]
    [TestCase("*")]
    [TestCase("a b")]
    public void Compose_rejects_an_invalid_name(string name)
    {
        Assert.That(
            () => LatticeTenantGroupId.Compose(Contoso, name),
            Throws.ArgumentException.With.Property(nameof(ArgumentException.ParamName)).EqualTo("name"));
    }

    // ----- Value semantics -----

    [Test]
    public void Ids_with_the_same_text_are_equal()
    {
        var parsed = LatticeTenantGroupId.Parse(new string("t/contoso/admins".AsSpan()));
        var composed = LatticeTenantGroupId.Compose(Contoso, "admins");

        Assert.That(parsed, Is.EqualTo(composed));
        Assert.That(parsed.GetHashCode(), Is.EqualTo(composed.GetHashCode()));
    }

    [Test]
    public void Ids_of_different_tenants_are_not_equal()
    {
        Assert.That(
            LatticeTenantGroupId.Parse("t/contoso/admins"),
            Is.Not.EqualTo(LatticeTenantGroupId.Parse("t/fabrikam/admins")));
    }

    [Test]
    public void ToString_returns_the_value_and_empty_for_the_uninitialised_id()
    {
        Assert.That(LatticeTenantGroupId.Parse("t/contoso/admins").ToString(), Is.EqualTo("t/contoso/admins"));
        Assert.That(default(LatticeTenantGroupId).ToString(), Is.Empty);
    }

    [Test]
    public void The_uninitialised_id_carries_no_value()
    {
        var none = default(LatticeTenantGroupId);

        Assert.That(none.Value, Is.Null);
        Assert.That(none.Name, Is.Null);
        Assert.That(none.Tenant.Value, Is.Null);
    }

    [Test]
    public void Serializer_round_trips_a_tenant_group_id()
    {
        var groupId = LatticeTenantGroupId.Compose(Contoso, "admins");

        var copy = RoundTrip(groupId);

        Assert.That(copy, Is.EqualTo(groupId));
        Assert.That(copy.Tenant, Is.EqualTo(Contoso));
        Assert.That(copy.Name, Is.EqualTo("admins"));
    }

    // ----- Allocation -----

    [Test]
    public void IsTenantGroupId_allocates_nothing()
    {
        string[] ids =
        [
            "t/contoso/admins",
            "t/default/admins",
            "t/contoso",
            "cluster-admins",
            "t/contoso/Admins",
        ];

        var growth = AllocationProbe.Growth(
            prepare: _ => ids,
            measure: static (state, size) =>
            {
                long hits = 0;
                for (var i = 0; i < size; i++)
                {
                    foreach (var id in state)
                    {
                        if (LatticeTenantGroupId.IsTenantGroupId(id))
                        {
                            hits++;
                        }
                    }
                }

                AllocationProbe.ScalarSink += hits;
            },
            smallSize: 100,
            largeSize: 10_000);

        Assert.That(growth, Is.Zero);
    }
}
