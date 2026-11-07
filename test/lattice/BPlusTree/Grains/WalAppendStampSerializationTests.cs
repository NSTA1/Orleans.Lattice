using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The wire cost of <see cref="WalAppendStamp"/> (a5e2e2ef's S1 on #5866): the stamp rides
/// every stamped append, and most of its members - the forward chain, the reader's narrow
/// tag, the pending closes - are default on almost every one. It omits default members,
/// so a plain pre-arm stamp costs its leaf, activation and routing only, and a fully
/// populated stamp still round-trips.
/// </summary>
[TestFixture]
public sealed class WalAppendStampSerializationTests
{
    private static Serializer<WalAppendStamp> CreateSerializer(out ServiceProvider services)
    {
        services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        return services.GetRequiredService<Serializer<WalAppendStamp>>();
    }

    [Test]
    public void A_plain_pre_arm_stamp_omits_its_default_members()
    {
        var serializer = CreateSerializer(out var services);
        using (services)
        {
            var plain = WalAppendStamp.PreArm(Guid.NewGuid(), Guid.NewGuid()) with
            {
                Initiator = WalRoutingInitiator.ShardRoot(0),
                Generation = 3,
            };

            var bytes = serializer.SerializeToArray(plain);

            Assert.Multiple(() =>
            {
                Assert.That(bytes.Length, Is.LessThan(96),
                    "a pre-arm stamp carried its default forward, narrow and close members on the wire");
                Assert.That(serializer.Deserialize(bytes), Is.EqualTo(plain));
            });
        }
    }

    [Test]
    public void A_fully_populated_stamp_round_trips()
    {
        var serializer = CreateSerializer(out var services);
        using (services)
        {
            var full = new WalAppendStamp(
                Guid.NewGuid(),
                4,
                WalRoutingInitiator.ShardRoot(2),
                9,
                Guid.NewGuid(),
                Guid.NewGuid(),
                Guid.NewGuid(),
                Guid.NewGuid(),
                Guid.NewGuid(),
                Guid.NewGuid(),
                Guid.NewGuid(),
                Guid.NewGuid(),
                new WalPendingClose(Guid.NewGuid(), Guid.NewGuid(), 2, 7),
                new WalPendingClose(Guid.NewGuid(), Guid.NewGuid(), 1, 8));

            Assert.That(serializer.Deserialize(serializer.SerializeToArray(full)), Is.EqualTo(full));
        }
    }
}
