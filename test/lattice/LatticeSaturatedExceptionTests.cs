namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the public <see cref="LatticeSaturatedException"/>
/// type itself: construction overloads, inheritance contract, the
/// <see cref="LatticeSaturatedException.TreeId"/> attribution slot,
/// and the stable serialization alias that the Orleans manifest relies
/// on to surface the typed exception across grain boundaries.
/// </summary>
[TestFixture]
public class LatticeSaturatedExceptionTests
{
    [Test]
    public void Parameterless_constructor_initialises_with_no_message_or_inner_and_empty_treeId()
    {
        var ex = new LatticeSaturatedException();
        Assert.Multiple(() =>
        {
            // The parameterless ctor exists to satisfy the framework's
            // exception-construction contract; production throw sites
            // use the overloads that carry diagnostic context.
            Assert.That(ex.Message, Is.Not.Null,
                "Exception.Message is non-null per the framework contract; the parameterless ctor inherits the default message");
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.TreeId, Is.EqualTo(string.Empty),
                "the parameterless ctor sets TreeId to string.Empty so callers can attribute the exception even on the no-context shape");
        });
    }

    [Test]
    public void Message_constructor_preserves_supplied_message_with_empty_treeId()
    {
        var ex = new LatticeSaturatedException("the per-tree saturation signal stayed Saturated beyond the budget");
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("the per-tree saturation signal stayed Saturated beyond the budget"));
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.TreeId, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void MessageAndInner_constructor_preserves_both_arguments_with_empty_treeId()
    {
        var inner = new TimeoutException("admission semaphore wait expired");
        var ex = new LatticeSaturatedException("WAL append refused (saturation budget)", inner);
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("WAL append refused (saturation budget)"));
            Assert.That(ex.InnerException, Is.SameAs(inner),
                "the inner exception slot must preserve the original cause for log diagnostics");
            Assert.That(ex.TreeId, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void MessageAndTreeId_constructor_preserves_both_arguments()
    {
        var ex = new LatticeSaturatedException(
            "WAL append refused (saturation budget)",
            treeId: "tree-bp");
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("WAL append refused (saturation budget)"));
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.TreeId, Is.EqualTo("tree-bp"),
                "the TreeId slot must preserve the caller-supplied tree id so callers can attribute the exception without parsing the message");
        });
    }

    [Test]
    public void MessageAndTreeIdAndInner_constructor_preserves_all_three_arguments()
    {
        var inner = new TimeoutException("admission semaphore wait expired");
        var ex = new LatticeSaturatedException(
            "WAL append refused (saturation budget)",
            treeId: "tree-bp",
            innerException: inner);
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("WAL append refused (saturation budget)"));
            Assert.That(ex.InnerException, Is.SameAs(inner));
            Assert.That(ex.TreeId, Is.EqualTo("tree-bp"));
        });
    }

    [Test]
    public void MessageAndTreeId_constructor_rejects_null_treeId()
    {
        Assert.Throws<ArgumentNullException>(
            () => new LatticeSaturatedException("any", treeId: null!),
            "treeId is part of the caller-visible attribution surface; the production overload must reject null at construction time");
    }

    [Test]
    public void MessageAndTreeIdAndInner_constructor_rejects_null_treeId()
    {
        Assert.Throws<ArgumentNullException>(
            () => new LatticeSaturatedException("any", treeId: null!, innerException: new InvalidOperationException()),
            "treeId is part of the caller-visible attribution surface; the production overload must reject null at construction time");
    }

    [Test]
    public void Derives_from_InvalidOperationException()
    {
        // Subclass-compatibility contract: catch (InvalidOperationException)
        // handlers that existed before the typed exception was
        // introduced must continue to absorb LatticeSaturatedException
        // without source changes.
        var ex = new LatticeSaturatedException("any");
        Assert.That(ex, Is.InstanceOf<InvalidOperationException>(),
            "LatticeSaturatedException must derive from InvalidOperationException so historical catch handlers continue to work");
    }

    [Test]
    public void Is_distinct_from_LatticeShuttingDownException()
    {
        // The saturation and shutdown surfaces are intentionally
        // distinct typed exceptions because their caller-side
        // recovery shapes are different: shutdown is a one-way
        // transition (retry will fail), saturation is recoverable
        // (retry after back-off can succeed). A catch-by-type
        // distinguishing the two must not absorb either exception
        // into the other.
        var sat = new LatticeSaturatedException("sat");
        var shut = new LatticeShuttingDownException("shut");
        Assert.Multiple(() =>
        {
            Assert.That(sat, Is.Not.InstanceOf<LatticeShuttingDownException>(),
                "LatticeSaturatedException must not derive from LatticeShuttingDownException");
            Assert.That(shut, Is.Not.InstanceOf<LatticeSaturatedException>(),
                "LatticeShuttingDownException must not derive from LatticeSaturatedException");
        });
    }

    [Test]
    public void Is_sealed_so_consumers_cannot_subclass_the_typed_saturation_surface()
    {
        // Same rationale as LatticeShuttingDownException: a
        // consumer-defined subclass would defeat the
        // "callers detect the regime via a single `is` check"
        // guarantee by introducing additional types in the same
        // hierarchy that callers would forget to handle.
        Assert.That(typeof(LatticeSaturatedException).IsSealed, Is.True,
            "LatticeSaturatedException must be sealed to keep the typed saturation surface a closed set");
    }

    [Test]
    public void Carries_stable_Orleans_alias_for_cross_grain_serialization()
    {
        // The Alias attribute is part of the Orleans wire format; the
        // typed exception must cross grain boundaries (e.g. when a
        // downstream WAL writer surfaces the admission-gate refusal
        // from its peer-silo writer) so caller-side `is` checks see
        // the typed exception rather than a generic
        // RuntimeException(message). The alias value is checked here
        // to pin the wire format - any future rename would silently
        // break rolling-upgrade peers.
        var aliasAttr = typeof(LatticeSaturatedException)
            .GetCustomAttributes(typeof(AliasAttribute), inherit: false)
            .Cast<AliasAttribute>()
            .SingleOrDefault();
        Assert.That(aliasAttr, Is.Not.Null,
            "LatticeSaturatedException must carry an [Alias] attribute so Orleans can serialize it across grain boundaries");
        Assert.That(aliasAttr!.Alias, Is.EqualTo("ol.lsa"),
            "the alias value must not change - it is part of the Orleans wire format and any rename would break rolling-upgrade peers");
    }

    [Test]
    public void Carries_GenerateSerializer_attribute_so_Orleans_codegen_runs()
    {
        // Without [GenerateSerializer] the Orleans code generator
        // does not emit a serializer for the type and a cross-grain
        // throw would surface a generic RuntimeException at the
        // caller rather than the typed shape.
        var attr = typeof(LatticeSaturatedException)
            .GetCustomAttributes(typeof(GenerateSerializerAttribute), inherit: false);
        Assert.That(attr, Is.Not.Empty,
            "LatticeSaturatedException must carry [GenerateSerializer] so Orleans codegen emits a serializer");
    }

    [Test]
    public void TreeId_property_carries_Orleans_Id_attribute_for_serialization()
    {
        // The TreeId property must be marked [Id(0)] so Orleans
        // codegen serialises it as a stable wire-format member.
        // Without the attribute the cross-grain surface would still
        // typecheck but the property would be silently zeroed at the
        // remote end.
        var prop = typeof(LatticeSaturatedException).GetProperty(nameof(LatticeSaturatedException.TreeId));
        Assert.That(prop, Is.Not.Null);
        var idAttr = prop!
            .GetCustomAttributes(typeof(IdAttribute), inherit: false)
            .Cast<IdAttribute>()
            .SingleOrDefault();
        Assert.That(idAttr, Is.Not.Null,
            "TreeId must carry [Id(0)] so it survives the cross-grain serialization round-trip");
        Assert.That(idAttr!.Id, Is.EqualTo((uint)0));
    }

    [Test]
    public void Is_public_so_callers_can_catch_it_by_type()
    {
        // The whole point of the typed exception is caller-side
        // detection via `is LatticeSaturatedException`. The type
        // must be public for that to compile against the consumer's
        // assembly.
        Assert.That(typeof(LatticeSaturatedException).IsPublic, Is.True,
            "LatticeSaturatedException must be public so consumers can catch it by type");
    }
}
