namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Claims;

/// <summary>
/// Coverage for the public shapes the claim tools return over the wire and for
/// <see cref="RepoContextClaimConflictException"/>'s public constructors. The
/// production throw shape is covered by
/// <see cref="RepoContextStoreClaimTests"/>; this fixture pins the parts of the
/// public surface a host or a caller can construct directly.
/// </summary>
[TestFixture]
public sealed class RepoContextClaimContractTests
{
    [Test]
    public void A_claim_result_carries_no_grant_detail_when_it_was_not_granted()
    {
        var result = new RepoContextClaimResult { Key = "k", LockName = "l", Granted = false };

        Assert.Multiple(() =>
        {
            Assert.That(result.Key, Is.EqualTo("k"));
            Assert.That(result.LockName, Is.EqualTo("l"));
            Assert.That(result.Granted, Is.False);
            Assert.That(result.FencingToken, Is.Null);
            Assert.That(result.Owner, Is.Null);
            Assert.That(result.Region, Is.Null);
            Assert.That(result.LeaseExpiresAtUtc, Is.Null);
            Assert.That(result.LeaseSeconds, Is.Null);
            Assert.That(result.Reason, Is.Null);
        });
    }

    [Test]
    public void A_claim_result_round_trips_its_grant_detail()
    {
        var result = new RepoContextClaimResult
        {
            Key = "k",
            LockName = "l",
            Granted = true,
            FencingToken = 7L,
            Owner = "agent-a",
            Region = "local",
            LeaseExpiresAtUtc = "2026-01-01T00:00:00.0000000Z",
            LeaseSeconds = 30d,
            Reason = null,
        };

        Assert.Multiple(() =>
        {
            Assert.That(result.Granted, Is.True);
            Assert.That(result.FencingToken, Is.EqualTo(7L));
            Assert.That(result.Owner, Is.EqualTo("agent-a"));
            Assert.That(result.Region, Is.EqualTo("local"));
            Assert.That(result.LeaseExpiresAtUtc, Is.EqualTo("2026-01-01T00:00:00.0000000Z"));
            Assert.That(result.LeaseSeconds, Is.EqualTo(30d));
        });
    }

    [Test]
    public void A_release_result_carries_the_token_it_declined_to_release()
    {
        var result = new RepoContextReleaseClaimResult
        {
            Key = "k",
            LockName = "l",
            Released = false,
            FencingToken = 4L,
            Reason = "stale",
        };

        Assert.Multiple(() =>
        {
            Assert.That(result.Key, Is.EqualTo("k"));
            Assert.That(result.LockName, Is.EqualTo("l"));
            Assert.That(result.Released, Is.False);
            Assert.That(result.FencingToken, Is.EqualTo(4L));
            Assert.That(result.Reason, Is.EqualTo("stale"));
        });
    }

    [Test]
    public void A_release_result_omits_a_reason_when_it_released()
    {
        var result = new RepoContextReleaseClaimResult
        {
            Key = "k",
            LockName = "l",
            Released = true,
            FencingToken = 4L,
        };

        Assert.Multiple(() =>
        {
            Assert.That(result.Released, Is.True);
            Assert.That(result.Reason, Is.Null);
        });
    }

    [Test]
    public void A_status_result_is_never_authoritative()
    {
        // The lock grant is the only authority on who holds a claim; this snapshot
        // is observability, and says so on the wire so no caller builds an
        // acquire decision on it. The flag is computed, not settable, so there is
        // no object-initializer spelling that could ever report otherwise.
        var result = new RepoContextClaimStatusResult
        {
            Key = "k",
            LockName = "l",
            Claimed = false,
            IsHeld = false,
            LockFencingToken = 0L,
            QueueDepth = 0,
            Exists = false,
        };

        Assert.Multiple(() =>
        {
            Assert.That(result.Authoritative, Is.False);
            Assert.That(result.Exists, Is.False);
            Assert.That(result.Claimed, Is.False);
            Assert.That(result.IsHeld, Is.False);
            Assert.That(result.FencingToken, Is.Null);
            Assert.That(result.ReleasedFencingToken, Is.Null);
            Assert.That(result.Owner, Is.Null);
            Assert.That(result.Region, Is.Null);
            Assert.That(result.LockFencingToken, Is.Zero);
            Assert.That(result.LeaseExpiresAtUtc, Is.Null);
            Assert.That(result.QueueDepth, Is.Zero);
        });
    }

    [Test]
    public void A_status_result_round_trips_a_live_claim()
    {
        var result = new RepoContextClaimStatusResult
        {
            Key = "k",
            LockName = "l",
            Claimed = true,
            IsHeld = true,
            FencingToken = 3L,
            ReleasedFencingToken = 2L,
            Owner = "agent-a",
            Region = "local",
            LockFencingToken = 3L,
            LeaseExpiresAtUtc = "2026-01-01T00:00:00.0000000Z",
            QueueDepth = 2,
            Exists = true,
        };

        Assert.Multiple(() =>
        {
            Assert.That(result.Claimed, Is.True);
            Assert.That(result.IsHeld, Is.True);
            Assert.That(result.FencingToken, Is.EqualTo(3L));
            Assert.That(result.ReleasedFencingToken, Is.EqualTo(2L));
            Assert.That(result.Owner, Is.EqualTo("agent-a"));
            Assert.That(result.Region, Is.EqualTo("local"));
            Assert.That(result.LockFencingToken, Is.EqualTo(3L));
            Assert.That(result.LeaseExpiresAtUtc, Is.EqualTo("2026-01-01T00:00:00.0000000Z"));
            Assert.That(result.QueueDepth, Is.EqualTo(2));
            Assert.That(result.Exists, Is.True);
            Assert.That(result.Authoritative, Is.False);
        });
    }

    [Test]
    public void The_conflict_exception_carries_a_default_message_and_reason()
    {
        var error = new RepoContextClaimConflictException();

        Assert.Multiple(() =>
        {
            Assert.That(error.Message, Is.Not.Empty);
            Assert.That(error.Key, Is.Empty);
            Assert.That(error.Reason, Is.EqualTo(nameof(RepoContextFenceVerdict.StaleToken)));
            Assert.That(error.PresentedFencingToken, Is.Null);
            Assert.That(error.CurrentFencingToken, Is.Null);
            Assert.That(error.Owner, Is.Null);
            Assert.That(error.Region, Is.Null);
        });
    }

    [Test]
    public void The_conflict_exception_accepts_a_message()
        => Assert.That(new RepoContextClaimConflictException("refused").Message, Is.EqualTo("refused"));

    [Test]
    public void The_conflict_exception_accepts_a_message_and_inner_exception()
    {
        var inner = new InvalidOperationException("cause");
        var error = new RepoContextClaimConflictException("refused", inner);

        Assert.Multiple(() =>
        {
            Assert.That(error.Message, Is.EqualTo("refused"));
            Assert.That(error.InnerException, Is.SameAs(inner));
        });
    }

    [Test]
    public void The_conflict_exception_travels_the_protocol_error_channel()
        => Assert.That(
            new RepoContextClaimConflictException(),
            Is.InstanceOf<ModelContextProtocol.McpException>());
}
