using NSubstitute;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="AtomicActionRegistrationBuilder"/>: the fail-closed
/// registration surface for custom atomic-action handlers. Registration is the only
/// way a handler enters the catalog, so the allow-list rules enforced here - the
/// reserved <c>ol.</c> prefix that keeps library built-ins un-shadowable, and the
/// duplicate-id rejection that makes resolution unambiguous - are what stop a saga
/// step resolving a handler the host never sanctioned.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class AtomicActionRegistrationBuilderTests
{
    private static IAtomicActionHandler Handler(string handlerId, string versionTag = "v1")
    {
        var handler = Substitute.For<IAtomicActionHandler>();
        handler.HandlerId.Returns(handlerId);
        handler.VersionTag.Returns(versionTag);
        return handler;
    }

    private static Task Noop(IAtomicActionContext context) => Task.CompletedTask;

    [Test]
    public void AddHandler_registers_a_delegate_pair_under_its_handler_id()
    {
        var builder = new AtomicActionRegistrationBuilder();

        var returned = builder.AddHandler("charge", "v1", Noop, Noop);

        Assert.That(returned, Is.SameAs(builder), "the builder must be chainable");
        Assert.That(builder.Handlers.Keys, Is.EquivalentTo(new[] { "charge" }));
        Assert.That(builder.Handlers["charge"].VersionTag, Is.EqualTo("v1"));
    }

    [Test]
    public void AddHandler_registers_a_handler_instance_under_its_own_id_and_version()
    {
        var builder = new AtomicActionRegistrationBuilder();
        var handler = Handler("ship", "v3");

        builder.AddHandler(handler);

        Assert.That(builder.Handlers["ship"].VersionTag, Is.EqualTo("v3"));
        Assert.That(builder.Handlers["ship"].Handler, Is.SameAs(handler));
    }

    [Test]
    public void AddHandler_registers_distinct_ids_side_by_side()
    {
        var builder = new AtomicActionRegistrationBuilder();

        builder.AddHandler(Handler("charge")).AddHandler(Handler("ship"));

        Assert.That(builder.Handlers.Keys, Is.EquivalentTo(new[] { "charge", "ship" }));
    }

    [Test]
    public void AddHandler_rejects_the_reserved_library_prefix()
    {
        var builder = new AtomicActionRegistrationBuilder();

        // 'ol.' is reserved for library built-ins (the tree-write step); admitting an
        // application handler under it would let a host shadow a built-in.
        var ex = Assert.Throws<ArgumentException>(() => builder.AddHandler(Handler("ol.treeWrite")));
        Assert.That(ex!.ParamName, Is.EqualTo("handler"));
        Assert.That(ex.Message, Does.Contain("reserved"));
        Assert.That(builder.Handlers, Is.Empty);
    }

    [Test]
    public void AddHandler_rejects_the_reserved_prefix_through_the_delegate_overload()
    {
        var builder = new AtomicActionRegistrationBuilder();

        Assert.Throws<ArgumentException>(() => builder.AddHandler("ol.sneaky", "v1", Noop, Noop));
    }

    [Test]
    public void AddHandler_allows_an_id_that_merely_contains_the_reserved_prefix()
    {
        var builder = new AtomicActionRegistrationBuilder();

        // The rule is a prefix rule, not a substring rule.
        Assert.DoesNotThrow(() => builder.AddHandler(Handler("payroll.calc")));
        Assert.That(builder.Handlers.ContainsKey("payroll.calc"), Is.True);
    }

    [Test]
    public void AddHandler_rejects_a_duplicate_handler_id()
    {
        var builder = new AtomicActionRegistrationBuilder();
        builder.AddHandler(Handler("charge", "v1"));

        var ex = Assert.Throws<ArgumentException>(() => builder.AddHandler(Handler("charge", "v2")));
        Assert.That(ex!.ParamName, Is.EqualTo("handler"));
        Assert.That(ex.Message, Does.Contain("already registered"));

        // The first registration must survive the rejected duplicate.
        Assert.That(builder.Handlers["charge"].VersionTag, Is.EqualTo("v1"));
    }

    [Test]
    public void AddHandler_id_comparison_is_ordinal_so_case_distinguishes_handlers()
    {
        var builder = new AtomicActionRegistrationBuilder();

        builder.AddHandler(Handler("charge")).AddHandler(Handler("Charge"));

        Assert.That(builder.Handlers.Keys, Is.EquivalentTo(new[] { "charge", "Charge" }));
    }

    [Test]
    public void AddHandler_rejects_a_null_handler()
    {
        var builder = new AtomicActionRegistrationBuilder();

        Assert.Throws<ArgumentNullException>(() => builder.AddHandler((IAtomicActionHandler)null!));
    }

    [Test]
    public void AddHandler_rejects_a_handler_with_a_null_id()
    {
        var builder = new AtomicActionRegistrationBuilder();

        Assert.Throws<ArgumentNullException>(() => builder.AddHandler(Handler(null!, "v1")));
    }

    [Test]
    public void AddHandler_rejects_a_handler_with_an_empty_id()
    {
        var builder = new AtomicActionRegistrationBuilder();

        Assert.Throws<ArgumentException>(() => builder.AddHandler(Handler(string.Empty, "v1")));
    }

    [Test]
    public void AddHandler_rejects_a_handler_with_a_null_version_tag()
    {
        var builder = new AtomicActionRegistrationBuilder();

        Assert.Throws<ArgumentNullException>(() => builder.AddHandler(Handler("charge", null!)));
    }

    [Test]
    public void AddHandler_rejects_a_handler_with_an_empty_version_tag()
    {
        var builder = new AtomicActionRegistrationBuilder();

        Assert.Throws<ArgumentException>(() => builder.AddHandler(Handler("charge", string.Empty)));
    }

    [Test]
    public void AddHandler_delegate_overload_rejects_null_arguments()
    {
        var builder = new AtomicActionRegistrationBuilder();

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => builder.AddHandler(null!, "v1", Noop, Noop));
            Assert.Throws<ArgumentException>(() => builder.AddHandler(string.Empty, "v1", Noop, Noop));
            Assert.Throws<ArgumentNullException>(() => builder.AddHandler("charge", null!, Noop, Noop));
            Assert.Throws<ArgumentException>(() => builder.AddHandler("charge", string.Empty, Noop, Noop));
            Assert.Throws<ArgumentNullException>(() => builder.AddHandler("charge", "v1", null!, Noop));
            Assert.Throws<ArgumentNullException>(() => builder.AddHandler("charge", "v1", Noop, null!));
        });
    }
}
