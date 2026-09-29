using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.Tests.UI.Navigation;

/// <summary>
/// The address grammar, example by example: what parses, what it parses to, the
/// canonical text it formats back to, and what is rejected.
/// </summary>
[TestFixture]
public sealed class ExplorerAddressTests
{
    [Test]
    [TestCase("/", null, null)]
    [TestCase("", null, null)]
    [TestCase("/data", null, "data")]
    [TestCase("data", null, "data")]
    [TestCase("/data/", null, "data")]
    [TestCase("/t/acme", "acme", null)]
    [TestCase("/t/acme/data", "acme", "data")]
    [TestCase("/T/acme/Data", "acme", "data")]
    public void Parse_reads_the_tenant_and_area(string text, string? tenant, string? area)
    {
        var address = ExplorerAddress.Parse(text);

        Assert.Multiple(() =>
        {
            Assert.That(address.Tenant, Is.EqualTo(tenant));
            Assert.That(address.Area, Is.EqualTo(area));
            Assert.That(address.IsHome, Is.EqualTo(area is null));
        });
    }

    [Test]
    public void A_data_address_carries_the_logical_tree_id_as_segments()
    {
        var address = ExplorerAddress.Parse("/t/acme/data/a/crm/orders?key=order%2F2026-09%2F10233&at=Rev-7");

        Assert.Multiple(() =>
        {
            Assert.That(address.Path, Is.EqualTo(new[] { "a", "crm", "orders" }));
            Assert.That(address.TreeId, Is.EqualTo("a/crm/orders"));
            Assert.That(address.GetQuery(ExplorerAddress.KeyQuery), Is.EqualTo("order/2026-09/10233"));
            Assert.That(address.GetQuery(ExplorerAddress.AtQuery), Is.EqualTo("Rev-7"));
            Assert.That(address.GetQuery(ExplorerAddress.PrefixQuery), Is.Null);
            Assert.That(address.Format(), Is.EqualTo("/t/acme/data/a/crm/orders?key=order%2F2026-09%2F10233&at=Rev-7"));
        });
    }

    [Test]
    [TestCase("Orders", "%4Frders")]
    [TestCase("a b", "a%20b")]
    [TestCase("x/y", "x%2Fy")]
    [TestCase(".", "%2E")]
    [TestCase("..", "%2E%2E")]
    [TestCase("caf\u00e9", "caf%C3%A9")]
    [TestCase("q?#&=%", "q%3F%23%26%3D%25")]
    [TestCase("lower-case_1.2~", "lower-case_1.2~")]
    public void A_segment_is_lower_case_with_everything_else_percent_encoded(string segment, string encoded)
    {
        var address = ExplorerAddress.ForArea("data", segment);

        Assert.Multiple(() =>
        {
            Assert.That(address.Format(), Is.EqualTo("/data/" + encoded));
            Assert.That(ExplorerAddress.Parse(address.Format()).Path.Single(), Is.EqualTo(segment));
        });
    }

    [Test]
    public void A_query_value_keeps_its_case_and_encodes_the_rest()
    {
        var address = ExplorerAddress.ForArea("data", "orders").WithQuery("prefix", "Order/2026 09");

        Assert.That(address.Format(), Is.EqualTo("/data/orders?prefix=Order%2F2026%2009"));
    }

    [Test]
    public void A_percent_escape_is_read_in_either_case_and_written_in_upper_case()
    {
        var address = ExplorerAddress.Parse("/data/%4frders");

        Assert.Multiple(() =>
        {
            Assert.That(address.Path.Single(), Is.EqualTo("Orders"));
            Assert.That(address.Format(), Is.EqualTo("/data/%4Frders"));
        });
    }

    [Test]
    public void A_fragment_is_ignored()
    {
        Assert.That(ExplorerAddress.Parse("/data/orders#lt-shell-content").Format(), Is.EqualTo("/data/orders"));
    }

    [Test]
    [TestCase("/t")]
    [TestCase("/t/")]
    [TestCase("//data")]
    [TestCase("/data//orders")]
    [TestCase("/1data")]
    [TestCase("/da_ta")]
    [TestCase("/data/%ZZ")]
    [TestCase("/data/%C3")]
    [TestCase("/data/%")]
    [TestCase("/data?key")]
    [TestCase("/data?=v")]
    [TestCase("/data?Bad_Key=v")]
    [TestCase("/data?key=a&key=b")]
    [TestCase("/t/acme/t")]
    public void Invalid_text_is_not_an_address(string text)
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerAddress.TryParse(text, out var address), Is.False);
            Assert.That(address, Is.Null);
            Assert.That(() => ExplorerAddress.Parse(text), Throws.TypeOf<FormatException>());
        });
    }

    [Test]
    public void Null_is_not_an_address()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerAddress.TryParse(null, out _), Is.False);
            Assert.That(() => ExplorerAddress.Parse(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void An_empty_query_parameter_is_skipped_and_an_upper_case_key_is_lowered()
    {
        var address = ExplorerAddress.Parse("/data?&PREFIX=a&&");

        Assert.That(address.Query, Is.EqualTo(new[] { new KeyValuePair<string, string>("prefix", "a") }));
    }

    [Test]
    [TestCase("/", "./")]
    [TestCase("/?x=1", "./?x=1")]
    [TestCase("/data/orders", "data/orders")]
    [TestCase("/t/acme", "t/acme")]
    public void ToHref_is_relative_to_the_base_path(string canonical, string href)
    {
        Assert.That(ExplorerAddress.Parse(canonical).ToHref(), Is.EqualTo(href));
    }

    [Test]
    public void The_parent_chain_drops_the_query_then_segments_then_the_area_then_the_tenant()
    {
        var chain = new List<string>();
        for (var address = ExplorerAddress.Parse("/t/acme/data/a/crm?key=k"); address is not null; address = address.Parent)
        {
            chain.Add(address.Format());
        }

        Assert.That(chain, Is.EqualTo(new[]
        {
            "/t/acme/data/a/crm?key=k",
            "/t/acme/data/a/crm",
            "/t/acme/data/a",
            "/t/acme/data",
            "/t/acme",
            "/",
        }));
    }

    [Test]
    public void WithQuery_adds_replaces_and_removes_in_place()
    {
        var address = ExplorerAddress.ForArea("data", "orders")
            .WithQuery("prefix", "a")
            .WithQuery("key", "k")
            .WithQuery("prefix", "b");

        Assert.Multiple(() =>
        {
            Assert.That(address.Format(), Is.EqualTo("/data/orders?prefix=b&key=k"));
            Assert.That(address.WithQuery("prefix", null).Format(), Is.EqualTo("/data/orders?key=k"));
            Assert.That(address.WithQuery("absent", null), Is.SameAs(address));
            Assert.That(() => address.WithQuery("Bad", "v"), Throws.ArgumentException);
            Assert.That(() => address.WithQuery("ok", "\ud800"), Throws.ArgumentException);
        });
    }

    [Test]
    public void WithTenant_and_WithPath_rebuild_the_address()
    {
        var address = ExplorerAddress.ForArea("data", "orders").WithQuery("key", "k");

        Assert.Multiple(() =>
        {
            Assert.That(address.WithTenant("acme").Format(), Is.EqualTo("/t/acme/data/orders?key=k"));
            Assert.That(address.WithTenant(null), Is.SameAs(address));
            Assert.That(address.WithPath("a", "crm").Format(), Is.EqualTo("/data/a/crm"));
            Assert.That(() => address.WithTenant(string.Empty), Throws.ArgumentException);
            Assert.That(() => ExplorerAddress.Home.WithPath("x"), Throws.ArgumentException);
        });
    }

    [Test]
    public void ForTree_splits_the_logical_tree_id()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerAddress.ForTree("data", "a/crm/orders").Format(), Is.EqualTo("/data/a/crm/orders"));
            Assert.That(ExplorerAddress.ForTree("data", "orders").TreeId, Is.EqualTo("orders"));
            Assert.That(() => ExplorerAddress.ForTree("data", "a//b"), Throws.ArgumentException);
            Assert.That(() => ExplorerAddress.ForTree("data", string.Empty), Throws.ArgumentException);
        });
    }

    [Test]
    public void Create_validates_every_part()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => ExplorerAddress.Create(null, "Data"), Throws.ArgumentException);
            Assert.That(() => ExplorerAddress.Create(null, "t"), Throws.ArgumentException);
            Assert.That(() => ExplorerAddress.Create(null, null, ["x"]), Throws.ArgumentException);
            Assert.That(() => ExplorerAddress.Create(null, "data", [string.Empty]), Throws.ArgumentException);
            Assert.That(() => ExplorerAddress.Create(null, "data", ["\udc00"]), Throws.ArgumentException);
            Assert.That(() => ExplorerAddress.Create(string.Empty, "data"), Throws.ArgumentException);
            Assert.That(
                () => ExplorerAddress.Create(null, "data", null, [new("key", "a"), new("key", "b")]),
                Throws.ArgumentException);
            Assert.That(() => ExplorerAddress.Create(null, "data", null, [new("1k", "a")]), Throws.ArgumentException);
            Assert.That(() => ExplorerAddress.ForArea(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void TryFromUri_reads_an_absolute_uri_under_the_base()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                ExplorerAddress.TryFromUri("https://host/explorer/data/orders?key=k", "https://host/explorer/", out var address),
                Is.True);
            Assert.That(address!.Format(), Is.EqualTo("/data/orders?key=k"));
            Assert.That(ExplorerAddress.TryFromUri("https://host/explorer", "https://host/explorer/", out var home), Is.True);
            Assert.That(home, Is.EqualTo(ExplorerAddress.Home));
            Assert.That(ExplorerAddress.TryFromUri("https://other/data", "https://host/explorer/", out _), Is.False);
        });
    }

    [Test]
    public void Equality_is_by_value()
    {
        var one = ExplorerAddress.Parse("/t/acme/data/orders?key=k");
        var two = ExplorerAddress.Create("acme", "data", ["orders"], [new("key", "k")]);

        Assert.Multiple(() =>
        {
            Assert.That(one, Is.EqualTo(two));
            Assert.That(one.GetHashCode(), Is.EqualTo(two.GetHashCode()));
            Assert.That(one.Equals((object)two), Is.True);
            Assert.That(one, Is.Not.EqualTo(two.WithQuery("key", "K")));
            Assert.That(one.ToString(), Is.EqualTo(one.Format()));
            Assert.That(two.Equals(null), Is.False);
        });
    }

    [Test]
    public void Route_segments_are_the_url_path_segments_decoded()
    {
        var rooted = ExplorerAddress.Parse("/t/acme/data/a%2Fb/c?key=k");
        var plain = ExplorerAddress.Parse("/access");
        var tenantHome = ExplorerAddress.Parse("/t/acme");

        Assert.Multiple(() =>
        {
            Assert.That(rooted.RouteSegmentCount, Is.EqualTo(5));
            Assert.That(Enumerable.Range(0, rooted.RouteSegmentCount).Select(rooted.RouteSegmentAt), Is.EqualTo(new[] { "t", "acme", "data", "a/b", "c" }));
            Assert.That(plain.RouteSegmentCount, Is.EqualTo(1));
            Assert.That(plain.RouteSegmentAt(0), Is.EqualTo("access"));
            Assert.That(tenantHome.RouteSegmentCount, Is.EqualTo(2));
            Assert.That(tenantHome.RouteSegmentAt(1), Is.EqualTo("acme"));
            Assert.That(ExplorerAddress.Home.RouteSegmentCount, Is.Zero);
            Assert.That(() => plain.RouteSegmentAt(1), Throws.InstanceOf<ArgumentOutOfRangeException>());
            Assert.That(() => plain.RouteSegmentAt(-1), Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }

    [Test]
    public void HasPath_is_whether_a_segment_follows_the_area()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerAddress.Parse("/t/acme/tenancy").HasPath, Is.False);
            Assert.That(ExplorerAddress.Parse("/tenancy?new=true").HasPath, Is.False);
            Assert.That(ExplorerAddress.Parse("/tenancy/acme").HasPath, Is.True);
            Assert.That(ExplorerAddress.Home.HasPath, Is.False);
        });
    }
}
