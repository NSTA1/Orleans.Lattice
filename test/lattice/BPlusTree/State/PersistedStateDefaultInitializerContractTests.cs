using System.Reflection;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.State;

/// <summary>
/// Contract coverage for a serialization hazard that corrupted live production
/// state (issue 899 / issue 1883): a persisted state member whose "off" value is
/// the CLR type default, carrying a non-default property initializer.
/// <para>
/// The grain-storage serializer used in production omits any member equal to the
/// type default. This is not a hypothesis - it is visible in the raw persisted
/// blobs read off a pristine production volume, where <c>IsRegistered:true</c> is
/// present, <c>IsDeleted</c> (false) is absent, and <c>RootIsLeaf</c> is absent:
/// </para>
/// <code>
/// {"RootNodeId":"bplusinternal/6f95...","PendingPromotionRootWasLeaf":true,"IsRegistered":true, ...}
/// </code>
/// <para>
/// When such a member also carries a non-default initializer - the shape
/// <c>[Id(1)] public bool RootIsLeaf { get; set; } = true;</c> - the round trip is
/// LOSSY IN ONE DIRECTION. A correctly written <c>false</c> is omitted by the
/// serializer and then RESURRECTED AS <c>true</c> by the initializer on load. The
/// writing code is correct and the value is still destroyed. On the volume this
/// left every one of the 160 internal-rooted shard roots presenting
/// <c>RootIsLeaf == true</c> over an internal root at runtime, which is the
/// <c>InvalidCastException</c> of issue 899.
/// </para>
/// <para>
/// <b>Why the rest of the suite cannot catch this.</b> The hazard belongs to the
/// grain-storage serializer, not to Orleans' core binary serializer. A round trip
/// of <see cref="ShardRootState"/> through <c>Serializer</c> preserves
/// <c>false</c> correctly, and every cluster fixture in this repository registers
/// <c>AddMemoryGrainStorage</c>, so a cluster-based reactivation test PASSES
/// WHETHER OR NOT THE DEFECT IS PRESENT. That is why this is asserted as a
/// structural contract over the state POCOs rather than as a round-trip test: a
/// round trip through the storage provider this suite actually configures would
/// be a check that cannot fail.
/// </para>
/// <para>
/// The remedy is to let the type default carry the "off" value, so an omitted
/// member reconstructs as exactly what was written. Every member is assigned
/// explicitly by the code that owns it, so no initializer is load-bearing.
/// </para>
/// </summary>
public sealed class PersistedStateDefaultInitializerContractTests
{
    /// <summary>
    /// The persisted-state POCOs. Scoped to the state namespace deliberately: it
    /// is the grain-storage round trip that omits defaults, so this is the exact
    /// population at risk.
    /// </summary>
    private static IEnumerable<Type> PersistedStateTypes()
        => typeof(ShardRootState).Assembly
            .GetTypes()
            .Where(t => t.Namespace == typeof(ShardRootState).Namespace)
            .Where(t => t is { IsClass: true, IsAbstract: false })
            .Where(t => t.GetCustomAttributes().Any(a => a.GetType().Name == "GenerateSerializerAttribute"))
            .Where(t => t.GetConstructor(Type.EmptyTypes) is not null)
            .OrderBy(t => t.Name, StringComparer.Ordinal);

    private static bool HasIdAttribute(MemberInfo member)
        => member.GetCustomAttributes().Any(a => a.GetType().Name == "IdAttribute");

    /// <summary>
    /// Every serialized VALUE-typed member of a persisted state POCO must equal
    /// <c>default(T)</c> on a freshly constructed instance, so that a member the
    /// storage serializer omitted (because it was written as the type default)
    /// reconstructs as the value that was written rather than as an initializer's
    /// value. Reference-typed members are exempt: their default is
    /// <see langword="null"/>, and an initializer such as <c>= []</c> is not the
    /// default and is therefore never omitted.
    /// <para>
    /// This gate carries no exemptions. The two negative-sentinel members it once
    /// named - <c>AtomicActionState.FailedStepIndex</c> and
    /// <c>LeafSnapshotBlob.SnapshotOffset</c> - are nullable as of issue 1888, so
    /// their fresh-instance value is <see langword="null"/>, which IS
    /// <c>default(T)</c> for a <see cref="Nullable{T}"/>. That is the whole remedy:
    /// where <c>0</c> is a legitimate value the member becomes nullable, so absent
    /// means <see langword="null"/> means unset and a written <c>0</c> is no longer
    /// a default the serializer may drop.
    /// </para>
    /// </summary>
    [Test]
    public void Persisted_state_value_members_must_equal_the_type_default_on_a_fresh_instance()
    {
        var violations = new List<string>();

        foreach (var type in PersistedStateTypes())
        {
            var instance = Activator.CreateInstance(type);
            if (instance is null) continue;

            foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                if (!HasIdAttribute(property)) continue;
                if (!property.CanRead) continue;
                if (!property.PropertyType.IsValueType) continue;

                var actual = property.GetValue(instance);
                var expected = Activator.CreateInstance(property.PropertyType);
                if (!Equals(actual, expected))
                {
                    violations.Add(
                        $"{type.Name}.{property.Name} ({property.PropertyType.Name}) initialises to '{actual}' "
                        + $"but default({property.PropertyType.Name}) is '{expected}'. A storage serializer that "
                        + "omits type defaults will drop the written value and this initializer will resurrect the "
                        + "wrong one on load. Remove the initializer and let the default carry the 'off' value, or "
                        + "make the member nullable when 0 is a legitimate value.");
                }
            }
        }

        Assert.That(violations, Is.Empty,
            "Persisted-state members with a non-default initializer:"
            + Environment.NewLine + string.Join(Environment.NewLine, violations));
    }

    /// <summary>
    /// Named pin for the member that was actually corrupted in production, so a
    /// rename or a re-added initializer cannot silently drop the coverage above.
    /// A shard root whose root is internal persists <c>RootIsLeaf = false</c>;
    /// that must survive an omitted-member reconstruction.
    /// </summary>
    [Test]
    public void ShardRootState_RootIsLeaf_reconstructs_as_false_when_the_serializer_omitted_it()
    {
        Assert.That(new ShardRootState().RootIsLeaf, Is.False,
            "An omitted RootIsLeaf must reconstruct as false (an internal root), which is the only value that "
            + "is ever written as the type default. Any other reading turns an internal root into a claimed "
            + "leaf root and reproduces the issue-899 InvalidCastException.");
    }

    /// <summary>
    /// Named pin for the second instance of the same hazard. It is currently
    /// unfired only because no tree in production has reached depth 3: every
    /// internal node presently has genuinely leaf children, so <c>true</c> is not
    /// the type default, is written, and survives. The first internal node that
    /// gains INTERNAL children writes <c>false</c>, has it omitted, and reloads
    /// claiming leaf children - blind-casting an internal node to
    /// <c>IBPlusLeafGrain</c> on the read path. That trigger is ordinary tree
    /// growth rather than a fault, so it is certain rather than possible.
    /// </summary>
    [Test]
    public void InternalNodeState_ChildrenAreLeaves_reconstructs_as_false_when_the_serializer_omitted_it()
    {
        Assert.That(new InternalNodeState().ChildrenAreLeaves, Is.False,
            "An omitted ChildrenAreLeaves must reconstruct as false (internal children), which is the only value "
            + "that is ever written as the type default. Any other reading makes a depth->=2 node claim leaf "
            + "children over internal ones once trees grow past two levels.");
    }
}
