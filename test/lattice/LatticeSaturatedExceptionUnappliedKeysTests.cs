using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;
using Orleans.Serialization.Cloning;

namespace Orleans.Lattice.Tests;

/// <summary>
/// <see cref="LatticeSaturatedException.UnappliedKeys"/> (issue #5970): an
/// immutable, de-duplicated, bounded list that never reaches the message, and an
/// additive wire field that a 10.2.0 reader skips and a 10.2.0 payload leaves
/// null.
/// </summary>
[TestFixture]
public sealed class LatticeSaturatedExceptionUnappliedKeysTests
{
    private ServiceProvider _services = null!;
    private Serializer<LatticeSaturatedException> _serializer = null!;
    private Serializer<PreUnappliedKeysSaturatedException> _oldSerializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<LatticeSaturatedException>>();
        _oldSerializer = _services.GetRequiredService<Serializer<PreUnappliedKeysSaturatedException>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private static LatticeSaturatedException Refusal(IEnumerable<string> keys, int batchCount = 100)
        => new("refused.", "tree", LatticeSaturationSource.ReplayPermitAdmission, keys, batchCount, innerException: null);

    [Test]
    public void The_list_is_a_read_only_copy_with_each_key_once()
    {
        var keys = new List<string> { "a", "b", "a", "c", "b" };
        var refusal = Refusal(keys);
        keys.Add("d");

        Assert.Multiple(() =>
        {
            Assert.That(refusal.UnappliedKeys, Is.EqualTo(new[] { "a", "b", "c" }));
            Assert.That(refusal.UnappliedKeys, Is.InstanceOf<System.Collections.ObjectModel.ReadOnlyCollection<string>>(),
                "the exception is shared, not copied, by the same-silo copier, so its list must not be mutable");
            Assert.That(refusal.Message, Does.Contain("3 of 100 key(s)"));
        });
    }

    [Test]
    public void Keys_never_reach_the_message_or_the_string_form()
    {
        var refusal = Refusal(["secret-customer-0001", "secret-customer-0002"]);
        var wrapped = new LatticeSaturatedException(
            "outer.", "tree", LatticeSaturationSource.SetManyFanOut, refusal.UnappliedKeys!, 2, refusal);

        Assert.Multiple(() =>
        {
            Assert.That(refusal.Message, Does.Not.Contain("secret-customer"));
            Assert.That(refusal.ToString(), Does.Not.Contain("secret-customer"));
            Assert.That(wrapped.ToString(), Does.Not.Contain("secret-customer"),
                "nor through an inner refusal's string form");
            Assert.That(refusal.Message, Does.Contain("2 of 100 key(s)"), "the count is reported instead");
        });
    }

    [Test]
    public void A_list_at_the_key_cap_is_kept_and_one_past_it_is_null_never_truncated()
    {
        var atCap = Refusal(Enumerable.Range(0, LatticeSaturatedException.MaxUnappliedKeys).Select(i => $"k{i}"), 100_000);
        var pastCap = Refusal(Enumerable.Range(0, LatticeSaturatedException.MaxUnappliedKeys + 1).Select(i => $"k{i}"), 100_000);

        Assert.Multiple(() =>
        {
            Assert.That(atCap.UnappliedKeys, Has.Count.EqualTo(LatticeSaturatedException.MaxUnappliedKeys));
            Assert.That(pastCap.UnappliedKeys, Is.Null, "a truncated list would not be a superset");
            Assert.That(pastCap.Message, Does.Contain($"{LatticeSaturatedException.MaxUnappliedKeys + 1} of 100000 key(s)"),
                "the count is still reported");
        });
    }

    [Test]
    public void A_list_past_the_character_cap_is_null()
    {
        var wide = new string('x', (LatticeSaturatedException.MaxUnappliedKeyChars / 2) - 1);
        var atCap = Refusal([wide + "1", wide + "2"]);
        var pastCap = Refusal([wide + "1", wide + "2", "3"]);

        Assert.Multiple(() =>
        {
            Assert.That(atCap.UnappliedKeys, Has.Count.EqualTo(2));
            Assert.That(pastCap.UnappliedKeys, Is.Null);
        });
    }

    [Test]
    public void The_list_round_trips_and_the_same_silo_copy_shares_it()
    {
        var refusal = Refusal(["a", "b"]);

        var decoded = _serializer.Deserialize(_serializer.SerializeToArray(refusal));
        var copier = _services.GetRequiredService<DeepCopier<LatticeSaturatedException>>();

        Assert.Multiple(() =>
        {
            Assert.That(decoded.UnappliedKeys, Is.EqualTo(new[] { "a", "b" }));
            Assert.That(decoded.TreeId, Is.EqualTo("tree"));
            Assert.That(decoded.SaturationSource, Is.EqualTo(LatticeSaturationSource.ReplayPermitAdmission));
            Assert.That(copier.Copy(refusal), Is.SameAs(refusal));
        });
    }

    [Test]
    public void A_refusal_crosses_a_rolling_upgrade_both_ways()
    {
        var onOldSilo = _oldSerializer.Deserialize(_serializer.SerializeToArray(Refusal(["a", "b"])));
        var fromOldSilo = _serializer.Deserialize(_oldSerializer.SerializeToArray(
            new PreUnappliedKeysSaturatedException("old.", "tree", LatticeSaturationSource.WalAdmission)));

        Assert.Multiple(() =>
        {
            Assert.That(onOldSilo.TreeId, Is.EqualTo("tree"), "a 10.2.0 reader skips the new field");
            Assert.That(onOldSilo.SaturationSource, Is.EqualTo(LatticeSaturationSource.ReplayPermitAdmission));
            Assert.That(fromOldSilo.TreeId, Is.EqualTo("tree"));
            Assert.That(fromOldSilo.SaturationSource, Is.EqualTo(LatticeSaturationSource.WalAdmission));
            Assert.That(fromOldSilo.UnappliedKeys, Is.Null, "a 10.2.0 payload means unknown, which is today's behaviour");
        });
    }

    /// <summary>
    /// The 10.2.0 shape of <see cref="LatticeSaturatedException"/>: the same
    /// field ids, without <see cref="LatticeSaturatedException.UnappliedKeys"/>.
    /// </summary>
    [GenerateSerializer]
    [Alias("test.pre5970.LatticeSaturatedException")]
    public sealed class PreUnappliedKeysSaturatedException : InvalidOperationException
    {
        public PreUnappliedKeysSaturatedException(string message, string treeId, LatticeSaturationSource source)
            : base(message)
        {
            TreeId = treeId;
            SaturationSource = source;
        }

        [Id(0)]
        public string TreeId { get; }

        [Id(1)]
        public LatticeSaturationSource SaturationSource { get; }
    }

    /// <summary>The same-silo copier the contract requires of a BCL-derived exception.</summary>
    [RegisterCopier]
    internal sealed class PreUnappliedKeysSaturatedExceptionCopier : IDeepCopier<PreUnappliedKeysSaturatedException>
    {
        public PreUnappliedKeysSaturatedException DeepCopy(PreUnappliedKeysSaturatedException input, CopyContext context) => input;
    }
}
