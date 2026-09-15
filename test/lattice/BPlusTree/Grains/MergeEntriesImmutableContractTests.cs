using System.Reflection;
using NUnit.Framework;
using Orleans.Concurrency;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the immutability marker on <c>IBPlusLeafGrain.MergeEntriesAsync</c>
/// (issue #2799).
/// <para>
/// This exists because removing the attribute is invisible: the build stays
/// green, every test stays green, and the only symptom is that a bounded merge
/// silently starts costing twice its budget again on the same-silo path. A
/// companion cluster test proves the attribute has the effect claimed here; this
/// one proves the attribute is still present to have it.
/// </para>
/// </summary>
[TestFixture]
public sealed class MergeEntriesImmutableContractTests
{
    /// <summary>
    /// Resolves the method, failing loudly rather than silently skipping if the
    /// seam is renamed or its shape changes. A contract test that cannot find
    /// its subject must go red, not green.
    /// </summary>
    private static MethodInfo ResolveMergeEntries()
    {
        var method = typeof(IBPlusLeafGrain).GetMethod(
            nameof(IBPlusLeafGrain.MergeEntriesAsync),
            BindingFlags.Public | BindingFlags.Instance);

        Assert.That(method, Is.Not.Null,
            "IBPlusLeafGrain.MergeEntriesAsync was not found. If it was renamed or removed, "
            + "update this test rather than deleting it - otherwise the [Immutable] contract "
            + "it pins is unguarded.");

        return method!;
    }

    [Test]
    public void MergeEntriesAsync_takes_exactly_one_parameter()
    {
        var parameters = ResolveMergeEntries().GetParameters();

        Assert.That(parameters, Has.Length.EqualTo(1),
            "the immutability assertion below addresses the single batch parameter by position; "
            + "if the signature grew a parameter, that assertion may no longer be looking at the batch.");
    }

    [Test]
    public void MergeEntriesAsync_batch_parameter_is_marked_immutable()
    {
        var parameter = ResolveMergeEntries().GetParameters()[0];

        var marker = parameter.GetCustomAttribute<ImmutableAttribute>();

        Assert.That(marker, Is.Not.Null,
            "MergeEntriesAsync's batch parameter must stay marked [Immutable]. Without it Orleans "
            + "deep-copies every payload on a co-located call, so a batch deliberately bounded to a "
            + "memory budget transiently occupies twice that budget. Callers must not mutate the "
            + "batch or its payload arrays after handing them over; see the XML doc on the method.");
    }
}
