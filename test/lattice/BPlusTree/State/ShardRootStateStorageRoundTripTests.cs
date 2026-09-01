using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.State;

/// <summary>
/// Round-trip coverage for the serialization hazard behind issue 899 / issue 1883,
/// asserted against the JSON <b>grain-storage</b> serializer rather than against
/// Orleans' core binary serializer.
/// <para>
/// <b>Why this fixture exists, and why it is not the test that was ruled out.</b>
/// The earlier finding on this defect was that "the obvious round-trip test cannot
/// fail", and for the test that was probed that was correct: Orleans' CORE
/// serializer preserves <c>false</c> faithfully, and every cluster fixture in this
/// repository registers <c>AddMemoryGrainStorage</c>, which round-trips through
/// that core serializer - so a deactivate/reactivate test on a cluster passes
/// whether or not the defect is present. The hazard belongs to the GRAIN-STORAGE
/// serializer, which omits any member equal to the CLR type default. That
/// serializer is reachable directly, without a cluster and without the storage
/// account the production deployment uses, so the round trip IS assertable - it
/// just cannot be reached through a cluster fixture. These tests fail against the
/// pre-fix POCO.
/// </para>
/// <para>
/// <b>Fidelity.</b> The production blob is a differently-configured rendering of
/// the same serializer (its member VALUES are spelled differently, notably
/// <c>GrainId</c>), so this fixture asserts the property that actually matters and
/// that both configurations share: which members are PRESENT. That is precisely the
/// axis the production census read - <c>IsRegistered:true</c> present,
/// <c>IsDeleted</c> (false) absent, <c>RootIsLeaf</c> absent - and it is the axis
/// the defect lives on.
/// </para>
/// </summary>
public sealed class ShardRootStateStorageRoundTripTests
{
    private static JsonGrainStorageSerializer CreateStorageSerializer()
    {
        var services = new ServiceCollection();
        services.AddSerializer();
        services.AddSingleton<OrleansJsonSerializer>();
        services.AddSingleton<JsonGrainStorageSerializer>();
        return services.BuildServiceProvider().GetRequiredService<JsonGrainStorageSerializer>();
    }

    /// <summary>
    /// A shard root whose root is an internal node, in the shape the census found on
    /// the production volume.
    /// </summary>
    private static ShardRootState InternalRootedState() => new()
    {
        RootNodeId = GrainId.Create("bplusinternal", "6f95"),
        RootIsLeaf = false,
        PendingPromotionRootWasLeaf = true,
        IsRegistered = true,
    };

    /// <summary>
    /// The mechanism itself, reproduced in-process: the grain-storage serializer
    /// DROPS <c>RootIsLeaf</c> from the payload when it is <c>false</c>, because
    /// <c>false</c> is the CLR type default. This is what made a correctly-written
    /// value unrecoverable, and it is asserted alongside a member that IS retained
    /// so the check cannot pass merely because serialization produced nothing
    /// useful.
    /// </summary>
    [Test]
    public void The_storage_serializer_omits_RootIsLeaf_when_it_is_false()
    {
        var json = CreateStorageSerializer().Serialize(InternalRootedState()).ToString();

        Assert.Multiple(() =>
        {
            Assert.That(json, Does.Not.Contain("RootIsLeaf"),
                "The grain-storage serializer omits members equal to the type default, so a written false leaves no "
                + "trace in the blob. This is the omission the production census observed directly.");
            Assert.That(json, Does.Not.Contain("IsDeleted"),
                "IsDeleted is false here and must be omitted too - the same mechanism, and the control the census "
                + "used.");
            Assert.That(json, Does.Contain("IsRegistered"),
                "Vacuousness guard: a non-default member must survive, otherwise the two absence assertions above "
                + "would hold for a payload that simply serialized nothing.");
            Assert.That(json, Does.Contain("PendingPromotionRootWasLeaf"),
                "Vacuousness guard: the second non-default member must survive too.");
        });
    }

    /// <summary>
    /// The self-heal, asserted end to end. A blob from which <c>RootIsLeaf</c> was
    /// omitted must reconstruct as <c>false</c> - the value that was written - rather
    /// than being resurrected as <c>true</c> by a property initializer.
    /// <para>
    /// This is what makes the fleet repair smaller than the raw census figure
    /// suggests: every shard root whose blob never carried the member is corrected by
    /// the POCO change alone, on load, with no write and no migration. Only a shard
    /// that was RE-SAVED after a bad reload - and whose blob therefore literally
    /// contains <c>"RootIsLeaf":true</c> - needs the activation repair, because for
    /// that shard there is no omitted value left to reconstruct.
    /// </para>
    /// </summary>
    [Test]
    public void A_persisted_blob_that_omitted_RootIsLeaf_reconstructs_as_false()
    {
        var serializer = CreateStorageSerializer();
        var payload = serializer.Serialize(InternalRootedState());

        var reloaded = serializer.Deserialize<ShardRootState>(payload);

        Assert.Multiple(() =>
        {
            Assert.That(reloaded.RootIsLeaf, Is.False,
                "An omitted RootIsLeaf must reconstruct as the false that was written. Reconstructing true is the "
                + "issue-899 lie, and it is what a non-default property initializer on this member produces.");
            Assert.That(reloaded.RootNodeId, Is.EqualTo(GrainId.Create("bplusinternal", "6f95")),
                "Vacuousness guard: the root id must survive the round trip, otherwise the flag assertion above "
                + "would be reading a blank object rather than a reconstructed one.");
            Assert.That(reloaded.PendingPromotionRootWasLeaf, Is.True,
                "Vacuousness guard: a true-valued bool must survive, otherwise 'RootIsLeaf is false' would hold for "
                + "any deserialization that simply lost every bool.");
        });
    }

    /// <summary>
    /// The baked half of the population, which the round trip cannot repair. A blob
    /// that literally carries <c>RootIsLeaf: true</c> over an internal root
    /// reconstructs it faithfully, which is correct serializer behaviour and exactly
    /// why those shards need an activation repair rather than a POCO change.
    /// </summary>
    [Test]
    public void A_persisted_blob_that_carries_RootIsLeaf_true_reconstructs_it_unchanged()
    {
        var serializer = CreateStorageSerializer();
        var baked = InternalRootedState();
        baked.RootIsLeaf = true;
        var payload = serializer.Serialize(baked);

        var reloaded = serializer.Deserialize<ShardRootState>(payload);

        Assert.Multiple(() =>
        {
            Assert.That(payload.ToString(), Does.Contain("RootIsLeaf"),
                "A true value is not the type default, so it is written to the blob - which is how the lie became "
                + "durable once a shard was re-saved after a bad reload.");
            Assert.That(reloaded.RootIsLeaf, Is.True,
                "No round trip can undo this: the value is really in the blob. Repairing it requires a write, which "
                + "is what the activation repair in ShardRootGrain performs.");
        });
    }
}
