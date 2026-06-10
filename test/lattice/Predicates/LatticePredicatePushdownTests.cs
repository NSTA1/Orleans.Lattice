namespace Orleans.Lattice.Tests.Predicates;

/// <summary>
/// Unit tests for the client-side capability gate
/// (<see cref="LatticePredicatePushdown"/> and
/// <see cref="ILatticePredicateSerializer"/>): a serializer that cannot expose
/// a navigable document must throw before any RPC.
/// </summary>
[TestFixture]
public class LatticePredicatePushdownTests
{
    private sealed class OpaqueSerializer : ILatticeSerializer<PredicatePerson>
    {
        public byte[] Serialize(PredicatePerson value) => [];
        public PredicatePerson Deserialize(byte[] bytes) => null!;
    }

    [Test]
    public void JsonLatticeSerializer_implements_predicate_capability()
    {
        Assert.That(JsonLatticeSerializer<PredicatePerson>.Default, Is.InstanceOf<ILatticePredicateSerializer>());
    }

    [Test]
    public void Compile_with_json_serializer_returns_ir()
    {
        var ir = LatticePredicatePushdown.Compile<PredicatePerson>(
            p => p.Age >= 18,
            JsonLatticeSerializer<PredicatePerson>.Default);

        Assert.That(ir.Kind, Is.EqualTo(LatticePredicateNodeKind.Compare));
    }

    [Test]
    public void Compile_with_unsupported_serializer_throws_NotSupported()
    {
        Assert.That(
            () => LatticePredicatePushdown.Compile<PredicatePerson>(p => p.Age >= 18, new OpaqueSerializer()),
            Throws.TypeOf<NotSupportedException>());
    }

    [Test]
    public void Compile_null_predicate_throws()
    {
        Assert.That(
            () => LatticePredicatePushdown.Compile<PredicatePerson>(null!, JsonLatticeSerializer<PredicatePerson>.Default),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Compile_null_serializer_throws()
    {
        Assert.That(
            () => LatticePredicatePushdown.Compile<PredicatePerson>(p => p.Age >= 18, null!),
            Throws.ArgumentNullException);
    }
}
