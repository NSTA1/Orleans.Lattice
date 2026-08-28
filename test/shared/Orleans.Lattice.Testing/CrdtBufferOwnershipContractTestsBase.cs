using System.Collections;
using System.Reflection;
using NUnit.Framework;

namespace Orleans.Lattice.Testing;

/// <summary>
/// Reusable, library-agnostic guard proving every CRDT primitive in a package
/// honours the <b>buffer-ownership contract</b>: ingress from a caller is a
/// hand-off, a fold from a peer or a delta copies the winning candidate, and
/// egress to a caller copies.
/// <para>
/// The rule exists because every primitive in the family stores opaque
/// <see cref="byte"/>[] payloads and none of them is defensively immutable. When
/// a fold adopts a peer's array, the receiver's durable state is aliased to an
/// array somebody else still owns; when a projection or clone hands one out, a
/// caller can write straight into stored state without passing any mutation API.
/// Both have drawn blood repeatedly in this repository, each time in a different
/// primitive, and each time the fix was pinned only by a hand-written test for
/// that one type - which is why the next sibling was always free to repeat it.
/// </para>
/// <para>
/// This base replaces that per-type auditing with a structural one. It walks the
/// real object graph of each specimen and compares <see cref="byte"/>[] instances
/// by <em>reference identity</em>, so it asserts the property itself rather than
/// any particular implementation of it. Crucially it also fails when a CRDT type
/// in the package has no specimen at all, or when a type grows a new public
/// <see cref="byte"/>[]-bearing projection that is not covered - so a newly added
/// primitive or read method cannot quietly opt out of the contract.
/// </para>
/// <para>
/// The library stays product-agnostic: it references no Orleans.Lattice type at
/// compile time and drives everything through <see cref="System.Reflection"/> and
/// consumer-supplied factories. The base is <see langword="abstract"/> so it is
/// never discovered on its own; the inherited <c>[Test]</c>s run through the
/// concrete subclass in the consuming assembly.
/// </para>
/// </summary>
public abstract class CrdtBufferOwnershipContractTestsBase
{
    /// <summary>Depth ceiling for the object-graph walk; specimens are small by construction.</summary>
    private const int MaxWalkDepth = 24;

    /// <summary>
    /// The package assembly whose CRDT types are audited. Only types
    /// <em>declared</em> in this assembly are considered, so each package audits
    /// exactly its own primitives.
    /// </summary>
    protected abstract Assembly PackageAssembly { get; }

    /// <summary>
    /// The open generic CRDT interface, i.e. <c>typeof(ICrdt&lt;&gt;)</c>. Supplied
    /// by the consumer so this library needs no compile-time product reference.
    /// </summary>
    protected abstract Type CrdtInterfaceType { get; }

    /// <summary>
    /// One specimen per CRDT type declared in <see cref="PackageAssembly"/>. A type
    /// may register more than one (for example a composite exercised with a single
    /// and with several contributors); every declared type needs at least one.
    /// </summary>
    protected abstract IReadOnlyList<CrdtOwnershipSpecimen> Specimens { get; }

    /// <summary>
    /// Fails when a CRDT type declared in the package has no specimen. This is the
    /// test that makes the audit structural: a newly added primitive cannot ship
    /// uncovered, because it fails <c>build-and-test</c> until it is registered.
    /// </summary>
    [Test]
    public void Every_crdt_type_in_the_package_has_an_ownership_specimen()
    {
        var covered = Specimens.Select(static s => Normalise(s.CrdtType)).ToHashSet();
        var missing = DeclaredCrdtTypes()
            .Where(t => !covered.Contains(Normalise(t)))
            .Select(static t => t.Name)
            .OrderBy(static n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.That(missing, Is.Empty,
            $"every CRDT type must have a buffer-ownership specimen; uncovered: {string.Join(", ", missing)}");
    }

    /// <summary>
    /// Keeps every other leg honest by pinning what each specimen actually holds.
    /// A specimen declared payload-free must expose no reachable
    /// <see cref="byte"/>[] - which is how the set primitives satisfy the contract,
    /// by encoding elements as base64 strings and never retaining a caller array,
    /// so a future change to raw <see cref="byte"/>[] storage fails here. Every
    /// other specimen must expose at least one buffer, or the clone and fold legs
    /// would pass vacuously against an empty instance.
    /// </summary>
    [Test]
    public void Every_specimen_matches_its_declared_payload_shape()
    {
        foreach (var specimen in Specimens)
        {
            var buffers = Buffers(specimen.CreatePopulated());
            if (specimen.PayloadFree)
            {
                Assert.That(buffers, Is.Empty,
                    $"{specimen.Description}: declared payload-free, but retains {buffers.Count} caller byte[]");
            }
            else
            {
                Assert.That(buffers, Is.Not.Empty,
                    $"{specimen.Description}: carries no byte[] payload, so the ownership legs would pass vacuously");
            }
        }
    }

    /// <summary>
    /// The <em>egress</em> leg for <c>Clone</c>: a clone must share no buffer with
    /// its source, or a composite that hands back <c>Clone()</c> (as
    /// <c>OrMap.Get</c> does, precisely so the caller may mutate what it read)
    /// leaks a live handle on stored state.
    /// </summary>
    [Test]
    public void Clone_shares_no_buffer_with_its_source()
    {
        foreach (var specimen in Specimens)
        {
            var source = specimen.CreatePopulated();
            var clone = Invoke(source, "Clone", Type.EmptyTypes);

            AssertDisjoint(
                Buffers(source),
                Buffers(clone),
                $"{specimen.Description}: Clone must deep-copy every byte[] payload");
        }
    }

    /// <summary>
    /// The <em>fold</em> leg for a state merge: after folding a peer, the receiver
    /// must share no buffer with that peer, which keeps using its own arrays.
    /// </summary>
    [Test]
    public void A_state_fold_from_a_peer_shares_no_buffer_with_the_peer()
    {
        foreach (var specimen in Specimens)
        {
            var peer = specimen.CreatePopulated();
            var receiver = specimen.CreateEmpty();
            InvokeVoid(receiver, "MergeFrom", [specimen.CrdtType], peer);

            AssertDisjoint(
                Buffers(receiver),
                Buffers(peer),
                $"{specimen.Description}: a fold from a peer must copy the adopted value");
        }
    }

    /// <summary>
    /// The <em>fold</em> leg for a delta apply: a producer may retry a delta or fan
    /// it out to several receivers, so adopting its buffers would leave every
    /// receiver sharing one array with the producer and with each other.
    /// </summary>
    [Test]
    public void A_delta_fold_shares_no_buffer_with_the_delta()
    {
        foreach (var specimen in Specimens)
        {
            if (specimen.CreateDeltaFrom is null || specimen.ApplyDelta is null) continue;

            var producer = specimen.CreatePopulated();
            var delta = specimen.CreateDeltaFrom(producer);
            var receiver = specimen.CreateEmpty();
            specimen.ApplyDelta(receiver, delta);

            AssertDisjoint(
                Buffers(receiver),
                Buffers(delta),
                $"{specimen.Description}: a fold from a delta must copy the adopted value");
        }
    }

    /// <summary>
    /// Fails when a CRDT type that <em>has</em> a delta fold has no specimen
    /// exercising it, so the delta leg cannot go unguarded for a type that
    /// implements one. A package whose CRDTs genuinely have no <c>MergeDelta</c>
    /// (a composite that folds only whole state, say) is correctly silent here
    /// rather than being forced to invent a specimen.
    /// </summary>
    [Test]
    public void Every_crdt_type_with_a_delta_fold_exercises_it()
    {
        var exercised = Specimens
            .Where(static s => s.CreateDeltaFrom is not null && s.ApplyDelta is not null)
            .Select(static s => Normalise(s.CrdtType))
            .ToHashSet();

        var missing = DeclaredCrdtTypes()
            .Where(static t => t.GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.DeclaredOnly)
                .Any(static m => m.Name == "MergeDelta" && m.GetParameters().Length == 1))
            .Where(t => !exercised.Contains(Normalise(t)))
            .Select(static t => t.Name)
            .OrderBy(static n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.That(missing, Is.Empty,
            $"a CRDT type with a MergeDelta must have a specimen exercising the delta fold leg; unexercised: {string.Join(", ", missing)}");
    }

    /// <summary>
    /// The <em>egress</em> leg for a materialised projection: a value handed back
    /// out must not be one of the arrays the instance retains.
    /// </summary>
    [Test]
    public void A_projection_shares_no_buffer_with_the_stored_state()
    {
        foreach (var specimen in Specimens)
        {
            foreach (var projection in specimen.Projections)
            {
                var state = specimen.CreatePopulated();
                var stored = Buffers(state);
                var projected = projection.Invoke(state).Where(static b => b is { Length: > 0 }).ToArray();

                Assert.That(projected, Is.Not.Empty,
                    $"{specimen.Description}.{projection.Name}: the specimen must project at least one non-empty value, or the leg is not actually exercised");
                AssertDisjoint(
                    stored,
                    new HashSet<byte[]>(projected, ReferenceComparer.Instance),
                    $"{specimen.Description}.{projection.Name}: a projection must copy, not hand out stored buffers");
            }
        }
    }

    /// <summary>
    /// A projection must not hand the <em>same</em> array to two readers either:
    /// a cached projection that shares its buffers lets one reader corrupt every
    /// later read, which is the same defect wearing a different hat.
    /// </summary>
    [Test]
    public void A_projection_does_not_hand_the_same_buffer_to_two_readers()
    {
        foreach (var specimen in Specimens)
        {
            foreach (var projection in specimen.Projections)
            {
                var state = specimen.CreatePopulated();
                var first = projection.Invoke(state).Where(static b => b is { Length: > 0 }).ToArray();
                var second = projection.Invoke(state).Where(static b => b is { Length: > 0 }).ToArray();

                AssertDisjoint(
                    new HashSet<byte[]>(first, ReferenceComparer.Instance),
                    new HashSet<byte[]>(second, ReferenceComparer.Instance),
                    $"{specimen.Description}.{projection.Name}: two reads must not share a mutable buffer");
            }
        }
    }

    /// <summary>
    /// Fails when a CRDT type grows a public parameterless method whose return type
    /// carries <see cref="byte"/>[] and which no specimen registers as a projection.
    /// Without this, a new read method is simply invisible to the guard.
    /// <para>
    /// Only methods are scanned, not properties: a primitive's serialized state
    /// properties (<c>Nodes</c>, <c>Entries</c>, <c>Adds</c>) are its wire surface,
    /// deliberately public and settable for the serializer, and are not egress
    /// seams. Methods returning the CRDT type itself (<c>Clone</c>, <c>Merge</c>)
    /// are covered by the clone leg and are excluded automatically because the
    /// declared return type is the type, not a byte[]-bearing shape.
    /// </para>
    /// </summary>
    [Test]
    public void Every_public_byte_array_projection_is_registered()
    {
        var unregistered = new List<string>();
        foreach (var group in Specimens.GroupBy(s => Normalise(s.CrdtType)))
        {
            var registered = group
                .SelectMany(static s => s.Projections)
                .Select(static p => p.MethodName)
                .ToHashSet(StringComparer.Ordinal);

            foreach (var method in ProjectionCandidates(group.Key))
            {
                if (!registered.Contains(method.Name))
                {
                    unregistered.Add($"{group.Key.Name}.{method.Name}");
                }
            }
        }

        Assert.That(unregistered, Is.Empty,
            $"every public byte[]-bearing projection must be registered on a specimen; unregistered: {string.Join(", ", unregistered)}");
    }

    private IEnumerable<Type> DeclaredCrdtTypes() =>
        PackageAssembly
            .GetTypes()
            .Where(t => t is { IsClass: true, IsAbstract: false })
            .Where(t => t.GetInterfaces().Any(i =>
                i.IsGenericType && i.GetGenericTypeDefinition() == CrdtInterfaceType));

    private static IEnumerable<MethodInfo> ProjectionCandidates(Type type) =>
        type.GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.DeclaredOnly)
            .Where(static m => !m.IsSpecialName && !m.IsGenericMethod && m.GetParameters().Length == 0)
            .Where(static m => MentionsByteArray(m.ReturnType, [], 0));

    /// <summary>
    /// Reduces a constructed generic type to its definition so a specimen registered
    /// as <c>OrMap&lt;string, Rga&gt;</c> covers the declared <c>OrMap&lt;,&gt;</c>.
    /// </summary>
    private static Type Normalise(Type type) =>
        type.IsGenericType && !type.IsGenericTypeDefinition ? type.GetGenericTypeDefinition() : type;

    /// <summary>
    /// Structural check on a declared return type: does it name <see cref="byte"/>[]
    /// directly, as an array element, or as a generic/tuple argument.
    /// </summary>
    private static bool MentionsByteArray(Type type, HashSet<Type> seen, int depth)
    {
        if (depth > 8) return false;
        if (type == typeof(byte[])) return true;
        if (type == typeof(string) || type.IsPrimitive || type.IsEnum) return false;
        if (!seen.Add(type)) return false;
        if (type.IsArray) return MentionsByteArray(type.GetElementType()!, seen, depth + 1);
        return type.IsGenericType
            && type.GetGenericArguments().Any(a => MentionsByteArray(a, seen, depth + 1));
    }

    private static object Invoke(object target, string name, Type[] signature, params object[] args) =>
        Resolve(target, name, signature).Invoke(target, args)
            ?? throw new InvalidOperationException($"{target.GetType().Name}.{name} returned null.");

    private static void InvokeVoid(object target, string name, Type[] signature, params object[] args) =>
        Resolve(target, name, signature).Invoke(target, args);

    private static MethodInfo Resolve(object target, string name, Type[] signature) =>
        target.GetType().GetMethod(name, signature)
            ?? throw new InvalidOperationException($"{target.GetType().Name} has no {name} method.");

    private static void AssertDisjoint(HashSet<byte[]> left, HashSet<byte[]> right, string because)
    {
        var shared = left.Where(right.Contains).ToArray();
        Assert.That(shared, Is.Empty, $"{because} (shared {shared.Length} byte[] instance(s) by reference)");
    }

    /// <summary>
    /// Collects every non-empty <see cref="byte"/>[] reachable from an object graph,
    /// keyed by reference identity. The shared <see cref="Array.Empty{T}"/> singleton
    /// is deliberately skipped: a zero-length array has no storage to corrupt, and the
    /// contract explicitly allows (indeed relies on) every empty payload sharing it.
    /// </summary>
    private static HashSet<byte[]> Buffers(object? root)
    {
        var sink = new HashSet<byte[]>(ReferenceComparer.Instance);
        Walk(root, sink, new HashSet<object>(ReferenceEqualityComparer.Instance), 0);
        return sink;
    }

    private static void Walk(object? node, HashSet<byte[]> sink, HashSet<object> visited, int depth)
    {
        if (node is null || depth > MaxWalkDepth) return;

        if (node is byte[] bytes)
        {
            if (bytes.Length > 0) sink.Add(bytes);
            return;
        }

        if (node is string) return;

        var type = node.GetType();
        if (type.IsPrimitive || type.IsEnum) return;
        if (!type.IsValueType && !visited.Add(node)) return;

        if (node is IEnumerable sequence)
        {
            foreach (var item in sequence) Walk(item, sink, visited, depth + 1);
            return;
        }

        foreach (var field in type.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic))
        {
            if (field.FieldType.IsPrimitive || field.FieldType.IsEnum) continue;
            Walk(field.GetValue(node), sink, visited, depth + 1);
        }
    }

    private sealed class ReferenceComparer : IEqualityComparer<byte[]>
    {
        public static readonly ReferenceComparer Instance = new();

        public bool Equals(byte[]? x, byte[]? y) => ReferenceEquals(x, y);

        public int GetHashCode(byte[] obj) => System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(obj);
    }
}
