namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the public <see cref="LatticeShuttingDownException"/>
/// type itself: construction overloads, inheritance contract, and the
/// stable serialization alias that the Orleans manifest relies on to
/// surface the typed exception across grain boundaries.
/// </summary>
[TestFixture]
public class LatticeShuttingDownExceptionTests
{
    [Test]
    public void Parameterless_constructor_initialises_with_no_message_or_inner()
    {
        var ex = new LatticeShuttingDownException();
        Assert.Multiple(() =>
        {
            // The parameterless ctor exists to satisfy the framework's
            // exception-construction contract; production throw sites
            // use the overloads that carry diagnostic context.
            Assert.That(ex.Message, Is.Not.Null,
                "Exception.Message is non-null per the framework contract; the parameterless ctor inherits the default message");
            Assert.That(ex.InnerException, Is.Null);
        });
    }

    [Test]
    public void Message_constructor_preserves_supplied_message()
    {
        var ex = new LatticeShuttingDownException("the silo is shutting down (WalDrainBudget)");
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("the silo is shutting down (WalDrainBudget)"));
            Assert.That(ex.InnerException, Is.Null);
        });
    }

    [Test]
    public void MessageAndInner_constructor_preserves_both_arguments()
    {
        var inner = new TimeoutException("admission semaphore release-by-drain");
        var ex = new LatticeShuttingDownException("WAL append refused (WalDrainBudget)", inner);
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("WAL append refused (WalDrainBudget)"));
            Assert.That(ex.InnerException, Is.SameAs(inner),
                "the inner exception slot must preserve the original cause for log diagnostics");
        });
    }

    [Test]
    public void Derives_from_InvalidOperationException()
    {
        // Subclass-compatibility contract: catch (InvalidOperationException)
        // handlers that existed before the typed exception was
        // introduced must continue to absorb LatticeShuttingDownException
        // without source changes.
        var ex = new LatticeShuttingDownException("any");
        Assert.That(ex, Is.InstanceOf<InvalidOperationException>(),
            "LatticeShuttingDownException must derive from InvalidOperationException so historical catch handlers continue to work");
    }

    [Test]
    public void Is_sealed_so_consumers_cannot_subclass_the_typed_shutdown_surface()
    {
        // The typed shutdown surface is intentionally sealed: a
        // consumer-defined subclass would defeat the
        // "callers detect the regime via a single `is` check"
        // guarantee by introducing additional types in the same
        // hierarchy that callers would forget to handle.
        Assert.That(typeof(LatticeShuttingDownException).IsSealed, Is.True,
            "LatticeShuttingDownException must be sealed to keep the typed shutdown surface a closed set");
    }

    [Test]
    public void Carries_stable_Orleans_alias_for_cross_grain_serialization()
    {
        // The Alias attribute is part of the Orleans wire format; the
        // typed exception must cross grain boundaries (e.g. when a
        // downstream IShardRootGrain RPC surfaces the drain refusal
        // from its peer-silo writer) so caller-side `is` checks see
        // the typed exception rather than a generic
        // RuntimeException(message). The alias value is checked here
        // to pin the wire format - any future rename would silently
        // break rolling-upgrade peers.
        var aliasAttr = typeof(LatticeShuttingDownException)
            .GetCustomAttributes(typeof(AliasAttribute), inherit: false)
            .Cast<AliasAttribute>()
            .SingleOrDefault();
        Assert.That(aliasAttr, Is.Not.Null,
            "LatticeShuttingDownException must carry an [Alias] attribute so Orleans can serialize it across grain boundaries");
        Assert.That(aliasAttr!.Alias, Is.EqualTo("ol.lsd"),
            "the alias value must not change - it is part of the Orleans wire format and any rename would break rolling-upgrade peers");
    }

    [Test]
    public void Carries_GenerateSerializer_attribute_so_Orleans_codegen_runs()
    {
        // Without [GenerateSerializer] the Orleans code generator
        // does not emit a serializer for the type and a cross-grain
        // throw would surface a generic RuntimeException at the
        // caller rather than the typed shape.
        var attr = typeof(LatticeShuttingDownException)
            .GetCustomAttributes(typeof(GenerateSerializerAttribute), inherit: false);
        Assert.That(attr, Is.Not.Empty,
            "LatticeShuttingDownException must carry [GenerateSerializer] so Orleans codegen emits a serializer");
    }

    [Test]
    public void Is_public_so_callers_can_catch_it_by_type()
    {
        // The whole point of the typed exception is caller-side
        // detection via `is LatticeShuttingDownException`. The type
        // must be public for that to compile against the consumer's
        // assembly.
        Assert.That(typeof(LatticeShuttingDownException).IsPublic, Is.True,
            "LatticeShuttingDownException must be public so consumers can catch it by type");
    }
}