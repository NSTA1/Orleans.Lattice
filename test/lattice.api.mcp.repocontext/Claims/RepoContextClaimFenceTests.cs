using System.Text;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Claims;

/// <summary>
/// Coverage for <see cref="RepoContextClaimFence"/>, <see cref="RepoContextClaimState"/>,
/// and <see cref="RepoContextClaimNames"/> - the pure half of the claim surface.
/// <para>
/// These are the tests that actually establish the safety property. Everything the
/// store does around them is plumbing; the question of whether a given write may
/// proceed is decided entirely by <see cref="RepoContextClaimFence.Evaluate"/>, and
/// it is decided without a clock, a lock, or any I/O. That is deliberate: a lease
/// that expires is not observed here at all, it is observed as the next grant
/// minting a strictly higher token, so none of this coverage can be flaky.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextClaimFenceTests
{
    private const string Region = "local";

    private static MemoryRecord Record() => new()
    {
        RepoId = "lattice",
        Topic = "backlog",
        Id = "item-1",
    };

    private static MemoryRecord Claimed(long token, string owner = "agent-a", string region = Region)
    {
        var record = Record();
        RepoContextClaimFence.StampClaim(record, token, owner, region);
        return record;
    }

    // ---- token codec ------------------------------------------------------

    [TestCase(0L)]
    [TestCase(1L)]
    [TestCase(-1L)]
    [TestCase(long.MaxValue)]
    [TestCase(long.MinValue)]
    public void Encode_then_decode_round_trips_the_token(long token)
    {
        var register = new BoundedRegister();
        var encoded = RepoContextClaimFence.Encode(token);
        register.Set(encoded, encoded);

        Assert.That(RepoContextClaimFence.Decode(register), Is.EqualTo(token));
    }

    [Test]
    public void Encode_produces_a_fixed_width_key()
        => Assert.That(RepoContextClaimFence.Encode(7L), Has.Length.EqualTo(RepoContextClaimFence.TokenWidth));

    [TestCase(-1L, 0L)]
    [TestCase(0L, 1L)]
    [TestCase(long.MinValue, long.MaxValue)]
    [TestCase(41L, 42L)]
    public void Encode_orders_bytes_the_same_way_it_orders_tokens(long lower, long higher)
    {
        // The whole fencing guarantee rests on this: BoundedRegister only advances
        // on a strictly greater order key, so byte order must agree with numeric
        // order across the entire long range, sign bit included.
        var left = RepoContextClaimFence.Encode(lower).AsSpan();
        var right = RepoContextClaimFence.Encode(higher).AsSpan();

        Assert.That(left.SequenceCompareTo(right), Is.LessThan(0));
    }

    [Test]
    public void A_lower_token_cannot_displace_a_higher_one_in_the_register()
    {
        var record = Claimed(9L);
        RepoContextClaimFence.StampClaim(record, 4L, "agent-b", Region);

        Assert.Multiple(() =>
        {
            Assert.That(RepoContextClaimFence.Decode(record.ClaimFence), Is.EqualTo(9L));
            Assert.That(RepoContextClaimFence.DecodeText(record.ClaimOwner), Is.EqualTo("agent-a"));
        });
    }

    [Test]
    public void A_higher_token_advances_the_fence_and_its_owner_together()
    {
        var record = Claimed(3L);
        RepoContextClaimFence.StampClaim(record, 11L, "agent-b", "east");

        var state = RepoContextClaimFence.Read(record);
        Assert.Multiple(() =>
        {
            Assert.That(state.FencingToken, Is.EqualTo(11L));
            Assert.That(state.Owner, Is.EqualTo("agent-b"));
            Assert.That(state.Region, Is.EqualTo("east"));
        });
    }

    [Test]
    public void Decode_returns_null_for_an_unwritten_register()
        => Assert.That(RepoContextClaimFence.Decode(new BoundedRegister()), Is.Null);

    [Test]
    public void Decode_returns_null_for_a_wrong_width_value()
    {
        var register = new BoundedRegister();
        register.Set([1, 2, 3], [1]);

        Assert.That(RepoContextClaimFence.Decode(register), Is.Null);
    }

    [Test]
    public void Decode_rejects_a_null_register()
        => Assert.That(() => RepoContextClaimFence.Decode(null!), Throws.ArgumentNullException);

    [Test]
    public void DecodeText_returns_null_for_an_unwritten_register()
        => Assert.That(RepoContextClaimFence.DecodeText(new BoundedRegister()), Is.Null);

    [Test]
    public void DecodeText_rejects_a_null_register()
        => Assert.That(() => RepoContextClaimFence.DecodeText(null!), Throws.ArgumentNullException);

    // ---- text comparison --------------------------------------------------

    [Test]
    public void TextEquals_matches_a_stored_value()
    {
        var register = new BoundedRegister();
        register.Set(Encoding.UTF8.GetBytes("east"), [1]);

        Assert.That(RepoContextClaimFence.TextEquals(register, "east"), Is.True);
    }

    [Test]
    public void TextEquals_rejects_a_different_value_of_the_same_length()
    {
        var register = new BoundedRegister();
        register.Set(Encoding.UTF8.GetBytes("east"), [1]);

        Assert.That(RepoContextClaimFence.TextEquals(register, "west"), Is.False);
    }

    [Test]
    public void TextEquals_rejects_a_value_of_a_different_length()
    {
        var register = new BoundedRegister();
        register.Set(Encoding.UTF8.GetBytes("east"), [1]);

        Assert.That(RepoContextClaimFence.TextEquals(register, "east-1"), Is.False);
    }

    [Test]
    public void TextEquals_rejects_an_unwritten_register()
        => Assert.That(RepoContextClaimFence.TextEquals(new BoundedRegister(), "east"), Is.False);

    [Test]
    public void TextEquals_handles_a_value_past_the_stack_buffer()
    {
        // Exercises the heap fallback: region identifiers are short in practice, so
        // this arm would otherwise never be executed by any other test.
        var oversized = new string('r', 400);
        var register = new BoundedRegister();
        register.Set(Encoding.UTF8.GetBytes(oversized), [1]);

        Assert.Multiple(() =>
        {
            Assert.That(RepoContextClaimFence.TextEquals(register, oversized), Is.True);
            Assert.That(RepoContextClaimFence.TextEquals(register, new string('s', 400)), Is.False);
        });
    }

    [Test]
    public void TextEquals_rejects_null_arguments()
        => Assert.Multiple(() =>
        {
            Assert.That(() => RepoContextClaimFence.TextEquals(null!, "east"), Throws.ArgumentNullException);
            Assert.That(
                () => RepoContextClaimFence.TextEquals(new BoundedRegister(), null!), Throws.ArgumentNullException);
        });

    // ---- claim state ------------------------------------------------------

    [Test]
    public void Read_projects_an_unclaimed_record_as_empty()
    {
        var state = RepoContextClaimFence.Read(Record());

        Assert.Multiple(() =>
        {
            Assert.That(state.FencingToken, Is.Null);
            Assert.That(state.ReleasedFencingToken, Is.Null);
            Assert.That(state.Owner, Is.Null);
            Assert.That(state.Region, Is.Null);
            Assert.That(state.IsClaimLive, Is.False);
        });
    }

    [Test]
    public void Read_rejects_a_null_record()
        => Assert.That(() => RepoContextClaimFence.Read(null!), Throws.ArgumentNullException);

    [Test]
    public void A_stamped_claim_is_live()
        => Assert.That(RepoContextClaimFence.Read(Claimed(5L)).IsClaimLive, Is.True);

    [Test]
    public void A_released_claim_is_not_live()
    {
        var record = Claimed(5L);
        RepoContextClaimFence.StampRelease(record, 5L);

        Assert.That(RepoContextClaimFence.Read(record).IsClaimLive, Is.False);
    }

    [Test]
    public void A_release_below_the_fence_leaves_the_claim_live()
    {
        // A late release from a superseded holder must not un-claim the record out
        // from under the holder that fenced past it.
        var record = Claimed(5L);
        RepoContextClaimFence.StampRelease(record, 5L);
        RepoContextClaimFence.StampClaim(record, 6L, "agent-b", Region);

        Assert.That(RepoContextClaimFence.Read(record).IsClaimLive, Is.True);
    }

    [Test]
    public void StampRelease_never_lowers_the_released_high_water_mark()
    {
        var record = Claimed(9L);
        RepoContextClaimFence.StampRelease(record, 9L);
        RepoContextClaimFence.StampRelease(record, 2L);

        Assert.That(RepoContextClaimFence.Read(record).ReleasedFencingToken, Is.EqualTo(9L));
    }

    [Test]
    public void Stamps_reject_null_arguments()
        => Assert.Multiple(() =>
        {
            Assert.That(
                () => RepoContextClaimFence.StampClaim(null!, 1L, "a", Region), Throws.ArgumentNullException);
            Assert.That(
                () => RepoContextClaimFence.StampClaim(Record(), 1L, null!, Region), Throws.ArgumentNullException);
            Assert.That(
                () => RepoContextClaimFence.StampClaim(Record(), 1L, "a", null!), Throws.ArgumentNullException);
            Assert.That(() => RepoContextClaimFence.StampRelease(null!, 1L), Throws.ArgumentNullException);
        });

    [Test]
    public void Claim_state_reports_liveness_from_its_two_marks()
        => Assert.Multiple(() =>
        {
            Assert.That(new RepoContextClaimState(null, null, null, null).IsClaimLive, Is.False);
            Assert.That(new RepoContextClaimState(4L, null, "a", Region).IsClaimLive, Is.True);
            Assert.That(new RepoContextClaimState(4L, 3L, "a", Region).IsClaimLive, Is.True);
            Assert.That(new RepoContextClaimState(4L, 4L, "a", Region).IsClaimLive, Is.False);
            Assert.That(new RepoContextClaimState(4L, 9L, "a", Region).IsClaimLive, Is.False);
        });

    // ---- admission --------------------------------------------------------

    [Test]
    public void An_unclaimed_record_admits_an_unfenced_write()
        => Assert.That(
            RepoContextClaimFence.Evaluate(Record(), presentedToken: null, Region),
            Is.EqualTo(RepoContextFenceVerdict.Accepted));

    [Test]
    public void An_unclaimed_record_admits_a_fenced_write()
        => Assert.That(
            RepoContextClaimFence.Evaluate(Record(), presentedToken: 7L, Region),
            Is.EqualTo(RepoContextFenceVerdict.Accepted));

    [Test]
    public void A_claimed_record_refuses_an_unfenced_write()
        => Assert.That(
            RepoContextClaimFence.Evaluate(Claimed(4L), presentedToken: null, Region),
            Is.EqualTo(RepoContextFenceVerdict.ClaimRequired));

    [Test]
    public void A_claimed_record_admits_the_current_holder()
        => Assert.That(
            RepoContextClaimFence.Evaluate(Claimed(4L), presentedToken: 4L, Region),
            Is.EqualTo(RepoContextFenceVerdict.Accepted));

    [Test]
    public void A_claimed_record_refuses_a_superseded_holder()
        => Assert.That(
            RepoContextClaimFence.Evaluate(Claimed(9L), presentedToken: 4L, Region),
            Is.EqualTo(RepoContextFenceVerdict.StaleToken));

    [Test]
    public void A_claimed_record_admits_a_token_ahead_of_its_stamp()
    {
        // The grant is authoritative and the record's stamp trails it: a holder
        // whose claim stamp has not landed yet must not be locked out of its own
        // record.
        Assert.That(
            RepoContextClaimFence.Evaluate(Claimed(4L), presentedToken: 5L, Region),
            Is.EqualTo(RepoContextFenceVerdict.Accepted));
    }

    [Test]
    public void A_released_record_admits_an_unfenced_write_again()
    {
        var record = Claimed(4L);
        RepoContextClaimFence.StampRelease(record, 4L);

        Assert.That(
            RepoContextClaimFence.Evaluate(record, presentedToken: null, Region),
            Is.EqualTo(RepoContextFenceVerdict.Accepted));
    }

    [Test]
    public void A_released_holder_may_not_keep_writing_under_its_own_token()
    {
        var record = Claimed(4L);
        RepoContextClaimFence.StampRelease(record, 4L);

        Assert.That(
            RepoContextClaimFence.Evaluate(record, presentedToken: 4L, Region),
            Is.EqualTo(RepoContextFenceVerdict.ClaimReleased));
    }

    [Test]
    public void A_superseded_token_stays_refused_after_the_claim_is_released()
    {
        var record = Claimed(9L);
        RepoContextClaimFence.StampRelease(record, 9L);

        Assert.That(
            RepoContextClaimFence.Evaluate(record, presentedToken: 4L, Region),
            Is.EqualTo(RepoContextFenceVerdict.StaleToken));
    }

    [Test]
    public void A_claim_taken_in_another_region_refuses_a_local_write()
        => Assert.That(
            RepoContextClaimFence.Evaluate(Claimed(4L, region: "east"), presentedToken: 4L, "west"),
            Is.EqualTo(RepoContextFenceVerdict.ForeignRegion));

    [Test]
    public void Evaluate_rejects_null_arguments()
        => Assert.Multiple(() =>
        {
            Assert.That(() => RepoContextClaimFence.Evaluate(null!, 1L, Region), Throws.ArgumentNullException);
            Assert.That(() => RepoContextClaimFence.Evaluate(Record(), 1L, null!), Throws.ArgumentNullException);
        });

    // ---- diagnostics ------------------------------------------------------

    [Test]
    public void Explain_names_the_remedy_for_each_refusal()
    {
        // The enum is internal, so this cannot be a [TestCase]-driven test: an
        // NUnit test method must be public, and a public method may not take an
        // internal parameter type.
        var state = new RepoContextClaimState(9L, null, "agent-a", "east");
        const string key = "repo/lattice/mem/backlog/item-1";

        Assert.Multiple(() =>
        {
            foreach (var (verdict, expected) in new[]
                     {
                         (RepoContextFenceVerdict.StaleToken, "superseded"),
                         (RepoContextFenceVerdict.ClaimRequired, "fencingToken"),
                         (RepoContextFenceVerdict.ClaimReleased, "already been released"),
                         (RepoContextFenceVerdict.ForeignRegion, "home region"),
                     })
            {
                var message = RepoContextClaimFence.Explain(verdict, key, state, 4L, "west");
                Assert.That(message, Does.Contain(expected), verdict.ToString());
                Assert.That(message, Does.Contain(key), verdict.ToString());
            }
        });
    }

    [Test]
    public void Explain_describes_an_admitted_write()
        => Assert.That(
            RepoContextClaimFence.Explain(
                RepoContextFenceVerdict.Accepted, "k", default, null, Region),
            Does.Contain("admitted"));

    // ---- lock naming ------------------------------------------------------

    [Test]
    public void The_lock_name_is_the_record_key_under_the_claim_namespace()
        => Assert.That(
            RepoContextClaimNames.LockName("repo/lattice/mem/backlog/item-1"),
            Is.EqualTo(RepoContextClaimNames.LockNamespace + "repo/lattice/mem/backlog/item-1"));

    [Test]
    public void Distinct_records_take_distinct_locks()
        => Assert.That(
            RepoContextClaimNames.LockName("repo/lattice/mem/backlog/a"),
            Is.Not.EqualTo(RepoContextClaimNames.LockName("repo/lattice/mem/backlog/b")));

    [Test]
    public void The_lock_name_rejects_a_null_key()
        => Assert.That(() => RepoContextClaimNames.LockName(null!), Throws.ArgumentNullException);
}
