using Newtonsoft.Json;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Internal;

namespace Orleans.Lattice.Tests.BPlusTree.State;

/// <summary>
/// Pins the c2-vi-followup regression: <see cref="InternalNodeState"/>
/// MUST round-trip through the JSON serialiser that
/// <c>AzureTableGrainStorage</c> uses by default. The shipping shape
/// has <c>ChildDigests</c> typed as
/// <c>Dictionary&lt;GrainId, ChildDigestSnapshot&gt;</c> and
/// Newtonsoft.Json has no built-in <c>TypeConverter</c> for
/// <see cref="Orleans.Runtime.GrainId"/>, so a write succeeds (the key
/// stringifies via <c>ToString()</c>) but a subsequent read fails with
/// <c>JsonSerializationException: Could not convert string
/// 'bplusleaf/&lt;guid&gt;' to dictionary key type 'Orleans.Runtime.GrainId'</c>.
/// <para>
/// The failure surfaces only on grain reactivation against a non-empty
/// grain-state table, so it was latent across the entire campaign from
/// step 8c-c-iii (which promoted Azure Tables grain storage to the
/// benchmark default) through c2-vi (which exposed it by forcing
/// internal-grain reactivations between rungs). Every production user
/// of the lattice with the default Azure Tables grain storage
/// configuration is exposed.
/// </para>
/// <para>
/// This test FAILS on the broken shape and PASSES after the c2-vi
/// follow-up library fix lands. Do not skip or weaken; the assertion
/// is the campaign's correctness gate for durable grain storage.
/// </para>
/// </summary>
[TestFixture]
public sealed class InternalNodeStateJsonRoundTripTests
{
    /// <summary>
    /// Builds the realistic two-child <see cref="InternalNodeState"/>
    /// shape persisted to Azure Tables after a handful of upward
    /// digest publishes from the leaf children.
    /// </summary>
    private static (InternalNodeState State, GrainId LeftLeafId, GrainId RightLeafId) BuildFixture()
    {
        var leftLeafId = GrainId.Create("bplusleaf", Guid.NewGuid().ToString("N"));
        var rightLeafId = GrainId.Create("bplusleaf", Guid.NewGuid().ToString("N"));

        var state = new InternalNodeState
        {
            ChildrenAreLeaves = true,
            Children =
            [
                new ChildEntry { SeparatorKey = null, ChildId = leftLeafId },
                new ChildEntry { SeparatorKey = "k", ChildId = rightLeafId },
            ],
            ChildDigests =
            {
                [leftLeafId] = new ChildDigestSnapshot
                {
                    Hash = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 },
                    EntryCount = 42,
                    CheckpointOffset = 100,
                },
                [rightLeafId] = new ChildDigestSnapshot
                {
                    Hash = new byte[] { 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1 },
                    EntryCount = 99,
                    CheckpointOffset = 200,
                },
            },
        };
        return (state, leftLeafId, rightLeafId);
    }

    [Test]
    public void InternalNodeState_with_ChildDigests_round_trips_through_newtonsoft_json()
    {
        // Production simulation: any host that wires lattice in calls
        // AddLattice, which calls GrainIdTypeConverterRegistration.
        // EnsureRegistered() before any grain storage runs. Call it
        // here so the test verifies the same composed behaviour the
        // production silo exhibits.
        GrainIdTypeConverterRegistration.EnsureRegistered();

        var (original, leftLeafId, rightLeafId) = BuildFixture();
        var settings = new JsonSerializerSettings();
        var json = JsonConvert.SerializeObject(original, settings);

        Assert.That(json, Is.Not.Empty);
        Assert.That(json, Does.Contain("ChildDigests"));

        var roundTripped = JsonConvert.DeserializeObject<InternalNodeState>(json, settings);

        Assert.That(roundTripped, Is.Not.Null);
        Assert.That(roundTripped!.ChildDigests, Has.Count.EqualTo(2),
            "ChildDigests dictionary must round-trip with the same number of entries.");
        Assert.That(roundTripped.ChildDigests.ContainsKey(leftLeafId), Is.True,
            "The leftLeafId GrainId key must round-trip identically via the registered TypeConverter.");
        Assert.That(roundTripped.ChildDigests.ContainsKey(rightLeafId), Is.True);
        Assert.That(roundTripped.ChildDigests[leftLeafId].EntryCount, Is.EqualTo(42));
        Assert.That(roundTripped.ChildDigests[rightLeafId].EntryCount, Is.EqualTo(99));
        Assert.That(roundTripped.ChildDigests[leftLeafId].CheckpointOffset, Is.EqualTo(100));
        Assert.That(roundTripped.ChildDigests[rightLeafId].CheckpointOffset, Is.EqualTo(200));
        Assert.That(roundTripped.ChildDigests[leftLeafId].Hash, Is.EqualTo(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 }));
    }

    [Test]
    public void GrainIdTypeConverter_round_trips_via_TypeDescriptor()
    {
        // Lower-level guard: verifies the TypeDescriptor.GetConverter
        // hook the registration installs is wired through
        // System.ComponentModel correctly. Newtonsoft.Json uses this
        // path for dictionary-key conversion, so a regression here is
        // the canary for the high-level round-trip above.
        GrainIdTypeConverterRegistration.EnsureRegistered();

        var original = GrainId.Create("bplusleaf", Guid.NewGuid().ToString("N"));
        var converter = System.ComponentModel.TypeDescriptor.GetConverter(typeof(GrainId));

        Assert.That(converter, Is.Not.Null);
        Assert.That(converter.CanConvertFrom(typeof(string)), Is.True);
        Assert.That(converter.CanConvertTo(typeof(string)), Is.True);

        var asString = converter.ConvertToString(original);
        Assert.That(asString, Is.EqualTo(original.ToString()));

        var roundTripped = (GrainId)converter.ConvertFromString(asString!)!;
        Assert.That(roundTripped, Is.EqualTo(original));
    }
}
