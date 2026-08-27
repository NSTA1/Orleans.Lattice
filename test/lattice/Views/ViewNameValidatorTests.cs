using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Unit tests for <see cref="ViewNameValidator"/>. A view name becomes the view
/// maintainer's grain key and is interpolated into the view tree id, which is
/// carried into <c>ShardRootGrain</c>'s composite key - a persistent grain - so a
/// name is held to the same storage-safety and unambiguity contract as any other
/// grain-key part.
/// </summary>
[TestFixture]
public sealed class ViewNameValidatorTests
{
    [TestCase("orders")]
    [TestCase("orders-by-region")]
    [TestCase("orders_v2")]
    [TestCase("Orders.V2")]
    [TestCase("a")]
    [TestCase("history:sys-auth-policy")]
    public void A_legal_name_is_accepted(string viewName)
    {
        Assert.Multiple(() =>
        {
            Assert.DoesNotThrow(() => ViewNameValidator.ThrowIfInvalid(viewName));
            Assert.That(ViewNameValidator.TryValidate(viewName, out var reason), Is.True);
            Assert.That(reason, Is.Null);
        });
    }

    // ----- Storage-unsafe characters -----

    [TestCase("a/b", '/')]
    [TestCase("a\\b", '\\')]
    [TestCase("a#b", '#')]
    [TestCase("a?b", '?')]
    public void A_storage_unsafe_character_is_rejected(string viewName, char offending)
    {
        var ex = Assert.Throws<ArgumentException>(() => ViewNameValidator.ThrowIfInvalid(viewName));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.ParamName, Is.EqualTo("viewName"));
            Assert.That(ex.Message, Does.Contain(offending.ToString()));
        });
    }

    [Test]
    public void A_control_character_is_rejected()
    {
        // The historical production defect (#1529) was an ASCII Unit Separator
        // joined into a grain key, which no in-memory test storage reproduces.
        var ex = Assert.Throws<ArgumentException>(
            () => ViewNameValidator.ThrowIfInvalid("orders\u001feu"));

        Assert.That(ex!.Message, Does.Contain("U+001F"));
    }

    [Test]
    public void A_control_character_is_escaped_in_the_diagnostic()
    {
        ViewNameValidator.TryValidate("orders\u0001eu", out var reason);

        Assert.Multiple(() =>
        {
            Assert.That(reason, Does.Contain("\\u0001"), "the message must not smuggle a raw control byte into a log");
            Assert.That(reason, Does.Not.Contain('\u0001'));
        });
    }

    // ----- The reserved generation separator -----

    [Test]
    public void The_generation_separator_is_rejected()
    {
        // A name carrying the separator would make two different views resolve to
        // one tree id, hence one grain identity and one persistent state row.
        // Asserted on the separator character rather than on which rule fires:
        // while the separator is itself storage-unsafe the storage rule catches it
        // first, and once the separator moves to a storage-safe character the
        // dedicated separator rule is what rejects it.
        var name = $"orders{LatticeViewTrees.GenerationSeparator}g2";

        var ex = Assert.Throws<ArgumentException>(() => ViewNameValidator.ThrowIfInvalid(name));

        Assert.That(ex!.Message, Does.Contain(LatticeViewTrees.GenerationSeparator.ToString()));
    }

    [Test]
    public void The_separator_rule_tracks_the_constant()
    {
        // Guards the rule against drifting from the separator it protects: if the
        // separator changes, this still rejects the new one.
        Assert.That(
            ViewNameValidator.TryValidate(
                $"a{LatticeViewTrees.GenerationSeparator}b", out _),
            Is.False);
    }

    // ----- Tenant-namespace containment falls out of banning the slash -----

    [Test]
    public void A_name_naming_a_tenant_namespace_is_rejected()
    {
        // Without this a caller could name a view 't/globex/orders', and
        // tenant-aware composition of the view tree id would place the tree inside
        // another tenant's reserved namespace. Banning '/' closes it with no
        // separate tenant-prefix rule.
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentException>(
                () => ViewNameValidator.ThrowIfInvalid("t/globex/orders"));
            Assert.Throws<ArgumentException>(
                () => ViewNameValidator.ThrowIfInvalid("t/acme/orders"));
        });
    }

    // ----- Empty and null -----

    [Test]
    public void Null_or_empty_is_rejected()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => ViewNameValidator.ThrowIfInvalid(null!));
            Assert.Throws<ArgumentException>(() => ViewNameValidator.ThrowIfInvalid(""));
        });
    }

    [Test]
    public void The_reported_parameter_name_follows_the_caller()
        => Assert.That(
            Assert.Throws<ArgumentException>(
                () => ViewNameValidator.ThrowIfInvalid("a/b", "sourceViewName"))!.ParamName,
            Is.EqualTo("sourceViewName"));

    // ----- Every rejected name would have produced an unusable id -----

    [TestCase("a/b")]
    [TestCase("a#b")]
    [TestCase("t/globex/orders")]
    public void A_rejected_name_would_have_produced_an_unsafe_or_ambiguous_tree_id(string viewName)
    {
        // Documents why the rule exists: the composed id carries the offending
        // character straight into the grain key.
        var wouldHaveBeen = LatticeViewTrees.ComposeTreeId(viewName);

        Assert.That(
            wouldHaveBeen.IndexOfAny(['/', '\\', '#', '?']),
            Is.GreaterThanOrEqualTo(0));
    }
}
