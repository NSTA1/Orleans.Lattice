using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the public <see cref="LatticeReplicationModeMismatchException"/>
/// type itself: construction overloads, inheritance contract, the
/// attribution slots (<see cref="LatticeReplicationModeMismatchException.TreeId"/>,
/// <see cref="LatticeReplicationModeMismatchException.DeclaredMode"/>,
/// <see cref="LatticeReplicationModeMismatchException.AttemptedMode"/>), and the
/// stable serialization alias the Orleans manifest relies on to surface the
/// typed exception across grain boundaries.
/// </summary>
[TestFixture]
public class LatticeReplicationModeMismatchExceptionTests
{
    [Test]
    public void Parameterless_constructor_initialises_with_empty_treeId_and_default_modes()
    {
        var ex = new LatticeReplicationModeMismatchException();
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.Not.Null);
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.TreeId, Is.EqualTo(string.Empty));
            Assert.That(ex.DeclaredMode, Is.EqualTo(default(LatticeMergeMode)));
            Assert.That(ex.AttemptedMode, Is.EqualTo(default(LatticeMergeMode)));
        });
    }

    [Test]
    public void Message_constructor_preserves_supplied_message_with_empty_treeId()
    {
        var ex = new LatticeReplicationModeMismatchException("plain write to a CRDT-declared tree");
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("plain write to a CRDT-declared tree"));
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.TreeId, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void MessageAndInner_constructor_preserves_both_arguments_with_empty_treeId()
    {
        var inner = new InvalidOperationException("decode failed");
        var ex = new LatticeReplicationModeMismatchException("shape mismatch", inner);
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("shape mismatch"));
            Assert.That(ex.InnerException, Is.SameAs(inner));
            Assert.That(ex.TreeId, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void Primary_constructor_preserves_all_context_slots()
    {
        var ex = new LatticeReplicationModeMismatchException(
            "shape mismatch",
            treeId: "votes",
            declaredMode: LatticeMergeMode.PnCounter,
            attemptedMode: LatticeMergeMode.LwwRegister);
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("shape mismatch"));
            Assert.That(ex.TreeId, Is.EqualTo("votes"));
            Assert.That(ex.DeclaredMode, Is.EqualTo(LatticeMergeMode.PnCounter));
            Assert.That(ex.AttemptedMode, Is.EqualTo(LatticeMergeMode.LwwRegister));
        });
    }

    [Test]
    public void Primary_constructor_rejects_null_treeId()
    {
        Assert.Throws<ArgumentNullException>(
            () => new LatticeReplicationModeMismatchException(
                "any", treeId: null!, declaredMode: LatticeMergeMode.OrSet, attemptedMode: LatticeMergeMode.LwwRegister));
    }

    [Test]
    public void Derives_from_InvalidOperationException()
    {
        var ex = new LatticeReplicationModeMismatchException("any");
        Assert.That(ex, Is.InstanceOf<InvalidOperationException>(),
            "must derive from InvalidOperationException so historical catch handlers continue to absorb it");
    }

    [Test]
    public void Is_sealed()
    {
        Assert.That(typeof(LatticeReplicationModeMismatchException).IsSealed, Is.True);
    }

    [Test]
    public void Is_public_so_callers_can_catch_it_by_type()
    {
        Assert.That(typeof(LatticeReplicationModeMismatchException).IsPublic, Is.True);
    }

    [Test]
    public void Carries_stable_Orleans_alias_for_cross_grain_serialization()
    {
        var aliasAttr = typeof(LatticeReplicationModeMismatchException)
            .GetCustomAttributes(typeof(AliasAttribute), inherit: false)
            .Cast<AliasAttribute>()
            .SingleOrDefault();
        Assert.That(aliasAttr, Is.Not.Null,
            "must carry an [Alias] attribute so Orleans can serialize it across grain boundaries");
        Assert.That(aliasAttr!.Alias, Is.EqualTo("ol.lrm"),
            "the alias value must not change - it is part of the Orleans wire format");
    }

    [Test]
    public void Carries_GenerateSerializer_attribute_so_Orleans_codegen_runs()
    {
        var attr = typeof(LatticeReplicationModeMismatchException)
            .GetCustomAttributes(typeof(GenerateSerializerAttribute), inherit: false);
        Assert.That(attr, Is.Not.Empty,
            "must carry [GenerateSerializer] so Orleans codegen emits a serializer");
    }

    [Test]
    public void Context_properties_carry_sequential_Orleans_Id_attributes()
    {
        Assert.Multiple(() =>
        {
            AssertHasId(nameof(LatticeReplicationModeMismatchException.TreeId), 0);
            AssertHasId(nameof(LatticeReplicationModeMismatchException.DeclaredMode), 1);
            AssertHasId(nameof(LatticeReplicationModeMismatchException.AttemptedMode), 2);
        });

        static void AssertHasId(string propertyName, uint expectedId)
        {
            var prop = typeof(LatticeReplicationModeMismatchException).GetProperty(propertyName);
            Assert.That(prop, Is.Not.Null);
            var idAttr = prop!
                .GetCustomAttributes(typeof(IdAttribute), inherit: false)
                .Cast<IdAttribute>()
                .SingleOrDefault();
            Assert.That(idAttr, Is.Not.Null, $"{propertyName} must carry an [Id] attribute");
            Assert.That(idAttr!.Id, Is.EqualTo(expectedId));
        }
    }
}
