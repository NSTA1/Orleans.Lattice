using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Claims;

/// <summary>
/// Coverage for the claim surface as the store actually exposes it: the four claim
/// operations, and the fencing check the record write path applies to
/// <c>remember</c>, <c>update</c>, and <c>forget</c>.
/// <para>
/// The lock is a deterministic in-process fake rather than a real grain, so every
/// assertion here is about a stated transition. A lapsed lease is expressed as
/// <see cref="FakeLatticeLockGrain.ExpireLease"/> followed by a fresh grant, never
/// as elapsed wall-clock time, so nothing in this fixture can flake on a slow
/// machine.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextStoreClaimTests
{
    private const string RepoId = "lattice";
    private const string Topic = "backlog";
    private const string ItemId = "item-1";
    private const string Key = $"repo/{RepoId}/mem/{Topic}/{ItemId}";
    private const string FileKey = $"repo/{RepoId}/file/src/lattice/Foo.cs";

    private static readonly Serializer Serializer = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

    private SubstitutedClaimSurface _surface = null!;
    private RepoContextStore _store = null!;

    [SetUp]
    public void CreateSurface()
    {
        _surface = new SubstitutedClaimSurface(Serializer);
        _store = _surface.Store();
    }

    private Task SeedAsync(string? body = "seed") => _store.RememberAsync(
        RepoId, Topic, ItemId, MemoryKind.Note, "Item", body, "author", null, null, null, null, null,
        CancellationToken.None);

    private Task<RepoContextRememberResult> WriteBodyAsync(string body, long? fencingToken) => _store.RememberAsync(
        RepoId, Topic, ItemId, MemoryKind.Note, null, body, null, null, null, null, null, null, fencingToken,
        CancellationToken.None);

    private Task<RepoContextClaimResult> ClaimAsync(string owner = "agent-a", long? maxWaitSeconds = null)
        => _store.ClaimAsync(Key, owner, leaseSeconds: null, maxWaitSeconds, CancellationToken.None);

    // ---- claim ------------------------------------------------------------

    [Test]
    public async Task Claim_grants_a_fencing_token_and_stamps_the_record()
    {
        await SeedAsync();

        var claim = await ClaimAsync();

        Assert.Multiple(() =>
        {
            Assert.That(claim.Granted, Is.True);
            Assert.That(claim.Key, Is.EqualTo(Key));
            Assert.That(claim.LockName, Is.EqualTo(RepoContextClaimNames.LockName(Key)));
            Assert.That(claim.FencingToken, Is.EqualTo(1L));
            Assert.That(claim.Owner, Is.EqualTo("agent-a"));
            Assert.That(claim.Region, Is.EqualTo(LocalRepoContextReplicaIdentity.LocalReplicaId));
            Assert.That(claim.LeaseSeconds, Is.GreaterThan(0d));
            Assert.That(claim.LeaseExpiresAtUtc, Is.Not.Null);
            Assert.That(claim.Reason, Is.Null);
            Assert.That(RepoContextClaimFence.Read(_surface.Read(Key)!).FencingToken, Is.EqualTo(1L));
        });
    }

    [Test]
    public async Task Claim_on_a_record_that_does_not_exist_is_refused_rather_than_thrown()
    {
        var claim = await ClaimAsync();

        Assert.Multiple(() =>
        {
            Assert.That(claim.Granted, Is.False);
            Assert.That(claim.Reason, Is.EqualTo("missing"));
            Assert.That(claim.FencingToken, Is.Null);
        });
    }

    [Test]
    public async Task Claim_loses_the_race_when_the_lock_is_already_held()
    {
        await SeedAsync();
        await ClaimAsync();

        var loser = await ClaimAsync("agent-b");

        Assert.Multiple(() =>
        {
            Assert.That(loser.Granted, Is.False);
            Assert.That(loser.Reason, Is.EqualTo("contended"));
        });
    }

    [Test]
    public async Task Claim_that_waits_reports_a_timeout_rather_than_faulting()
    {
        await SeedAsync();
        await ClaimAsync();

        var loser = await ClaimAsync("agent-b", maxWaitSeconds: 5L);

        Assert.Multiple(() =>
        {
            Assert.That(loser.Granted, Is.False);
            Assert.That(loser.Reason, Is.EqualTo("timeout"));
        });
    }

    [Test]
    public async Task Claim_hands_the_lock_back_when_the_record_vanishes_before_the_stamp()
    {
        await SeedAsync();
        _surface.AfterRead = key => _surface.Drop(key);

        var claim = await ClaimAsync();
        _surface.AfterRead = null;

        Assert.Multiple(() =>
        {
            Assert.That(claim.Granted, Is.False);
            Assert.That(claim.Reason, Is.EqualTo("missing"));
            Assert.That(_surface.LockFor(Key).IsHeld, Is.False, "the lock must not be left held");
        });
    }

    [Test]
    public async Task Claim_rejects_a_key_that_is_not_a_memory_record()
    {
        var error = Assert.ThrowsAsync<McpException>(
            () => _store.ClaimAsync(FileKey, "agent-a", null, null, CancellationToken.None));

        Assert.That(error!.Message, Does.Contain("memory records only"));
        await Task.CompletedTask;
    }

    [Test]
    public void Claim_rejects_an_empty_owner()
        => Assert.That(
            () => _store.ClaimAsync(Key, "  ", null, null, CancellationToken.None),
            Throws.TypeOf<McpException>());

    [TestCase(0L)]
    [TestCase(-1L)]
    public void Claim_rejects_a_non_positive_lease(long leaseSeconds)
        => Assert.That(
            () => _store.ClaimAsync(Key, "agent-a", leaseSeconds, null, CancellationToken.None),
            Throws.TypeOf<McpException>().With.Message.Contains("leaseSeconds"));

    [Test]
    public void Claim_rejects_a_non_positive_wait()
        => Assert.That(
            () => _store.ClaimAsync(Key, "agent-a", null, -1L, CancellationToken.None),
            Throws.TypeOf<McpException>().With.Message.Contains("maxWaitSeconds"));

    // ---- write-path fencing ----------------------------------------------

    [Test]
    public async Task An_unclaimed_record_still_accepts_an_unfenced_write()
    {
        await SeedAsync();

        await WriteBodyAsync("rewritten", fencingToken: null);

        Assert.That(BodyOf(Key), Is.EqualTo("rewritten"));
    }

    [Test]
    public async Task The_claim_holder_may_write_the_body_under_its_token()
    {
        await SeedAsync();
        var claim = await ClaimAsync();

        await WriteBodyAsync("progress", claim.FencingToken);

        Assert.That(BodyOf(Key), Is.EqualTo("progress"));
    }

    [Test]
    public async Task A_claimed_record_refuses_an_unfenced_body_write()
    {
        await SeedAsync();
        await ClaimAsync();

        var error = Assert.ThrowsAsync<RepoContextClaimConflictException>(
            () => WriteBodyAsync("stomp", fencingToken: null));

        Assert.Multiple(() =>
        {
            Assert.That(error!.Reason, Is.EqualTo(nameof(RepoContextFenceVerdict.ClaimRequired)));
            Assert.That(error.Key, Is.EqualTo(Key));
            Assert.That(error.CurrentFencingToken, Is.EqualTo(1L));
            Assert.That(error.Owner, Is.EqualTo("agent-a"));
            Assert.That(BodyOf(Key), Is.EqualTo("seed"));
        });
    }

    [Test]
    public async Task A_superseded_holder_cannot_overwrite_the_body()
    {
        // The load-bearing case. The item schema carries its resume state in the
        // body register, which is last-writer-wins: without this refusal a holder
        // that lost its lease would silently clobber its successor's progress.
        await SeedAsync();
        var first = await ClaimAsync("agent-a");

        _surface.LockFor(Key).ExpireLease();
        var second = await ClaimAsync("agent-b");
        await WriteBodyAsync("owned by b", second.FencingToken);

        var error = Assert.ThrowsAsync<RepoContextClaimConflictException>(
            () => WriteBodyAsync("stale write from a", first.FencingToken));

        Assert.Multiple(() =>
        {
            Assert.That(second.FencingToken, Is.Not.Null);
            Assert.That(second.FencingToken!.Value, Is.GreaterThan(first.FencingToken!.Value));
            Assert.That(error!.Reason, Is.EqualTo(nameof(RepoContextFenceVerdict.StaleToken)));
            Assert.That(error.PresentedFencingToken, Is.EqualTo(first.FencingToken));
            Assert.That(error.CurrentFencingToken, Is.EqualTo(second.FencingToken));
            Assert.That(BodyOf(Key), Is.EqualTo("owned by b"));
        });
    }

    [Test]
    public async Task A_released_holder_cannot_keep_writing_under_its_own_token()
    {
        await SeedAsync();
        var claim = await ClaimAsync();
        await _store.ReleaseClaimAsync(Key, claim.FencingToken!.Value, CancellationToken.None);

        var error = Assert.ThrowsAsync<RepoContextClaimConflictException>(
            () => WriteBodyAsync("after release", claim.FencingToken));

        Assert.That(error!.Reason, Is.EqualTo(nameof(RepoContextFenceVerdict.ClaimReleased)));
    }

    [Test]
    public async Task A_claim_taken_in_another_region_refuses_a_local_write()
    {
        await SeedAsync();
        var east = Substitute.For<IRepoContextReplicaIdentity>();
        east.ReplicaId.Returns("east");
        var claim = await _surface.Store(east).ClaimAsync(Key, "agent-a", null, null, CancellationToken.None);

        var error = Assert.ThrowsAsync<RepoContextClaimConflictException>(
            () => WriteBodyAsync("cross region", claim.FencingToken));

        Assert.Multiple(() =>
        {
            Assert.That(error!.Reason, Is.EqualTo(nameof(RepoContextFenceVerdict.ForeignRegion)));
            Assert.That(error.Region, Is.EqualTo("east"));
        });
    }

    [Test]
    public async Task Update_is_fenced_on_the_same_terms_as_remember()
    {
        await SeedAsync();
        var claim = await ClaimAsync();

        var refused = Assert.ThrowsAsync<RepoContextClaimConflictException>(
            () => _store.UpdateAsync(
                Key, new Dictionary<string, string> { ["body"] = "stomp" }, null, null, null, null,
                CancellationToken.None));

        await _store.UpdateAsync(
            Key, new Dictionary<string, string> { ["body"] = "owned" }, null, null, null, null,
            claim.FencingToken, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(refused!.Reason, Is.EqualTo(nameof(RepoContextFenceVerdict.ClaimRequired)));
            Assert.That(BodyOf(Key), Is.EqualTo("owned"));
        });
    }

    [Test]
    public async Task Forget_is_fenced_on_the_same_terms_as_remember()
    {
        await SeedAsync();
        var claim = await ClaimAsync();

        var refused = Assert.ThrowsAsync<RepoContextClaimConflictException>(
            () => _store.ForgetAsync(Key, lapse: false, null, CancellationToken.None));

        await _store.ForgetAsync(Key, lapse: false, null, claim.FencingToken, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(refused!.Reason, Is.EqualTo(nameof(RepoContextFenceVerdict.ClaimRequired)));
            Assert.That(_surface.Exists(Key), Is.False);
        });
    }

    [Test]
    public void A_fencing_token_presented_against_a_non_memory_record_is_rejected()
        => Assert.Multiple(() =>
        {
            Assert.That(
                () => _store.UpdateAsync(FileKey, null, null, null, null, null, 1L, CancellationToken.None),
                Throws.TypeOf<McpException>().With.Message.Contains("memory records only"));
            Assert.That(
                () => _store.ForgetAsync(FileKey, lapse: false, null, 1L, CancellationToken.None),
                Throws.TypeOf<McpException>().With.Message.Contains("memory records only"));
        });

    // ---- renew ------------------------------------------------------------

    [Test]
    public async Task Renew_extends_the_lease_without_changing_the_token()
    {
        await SeedAsync();
        var claim = await ClaimAsync();

        var renewed = await _store.RenewClaimAsync(
            Key, claim.FencingToken!.Value, leaseSeconds: 90L, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(renewed.Granted, Is.True);
            Assert.That(renewed.FencingToken, Is.EqualTo(claim.FencingToken));
            Assert.That(renewed.Owner, Is.EqualTo("agent-a"));
            Assert.That(renewed.Region, Is.EqualTo(LocalRepoContextReplicaIdentity.LocalReplicaId));
            Assert.That(renewed.LeaseSeconds, Is.EqualTo(90d));
        });
    }

    [Test]
    public async Task Renew_reports_a_superseded_token_rather_than_faulting()
    {
        await SeedAsync();
        var first = await ClaimAsync("agent-a");
        _surface.LockFor(Key).ExpireLease();
        await ClaimAsync("agent-b");

        var renewed = await _store.RenewClaimAsync(
            Key, first.FencingToken!.Value, null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(renewed.Granted, Is.False);
            Assert.That(renewed.Reason, Is.EqualTo("superseded"));
        });
    }

    [Test]
    public void Renew_rejects_a_key_that_is_not_a_memory_record()
        => Assert.That(
            () => _store.RenewClaimAsync(FileKey, 1L, null, CancellationToken.None),
            Throws.TypeOf<McpException>());

    [Test]
    public void Renew_rejects_a_non_positive_lease()
        => Assert.That(
            () => _store.RenewClaimAsync(Key, 1L, 0L, CancellationToken.None),
            Throws.TypeOf<McpException>().With.Message.Contains("leaseSeconds"));

    // ---- release ----------------------------------------------------------

    [Test]
    public async Task Release_marks_the_claim_dead_and_readmits_unfenced_writes()
    {
        await SeedAsync();
        var claim = await ClaimAsync();

        var released = await _store.ReleaseClaimAsync(Key, claim.FencingToken!.Value, CancellationToken.None);
        await WriteBodyAsync("unfenced again", fencingToken: null);

        Assert.Multiple(() =>
        {
            Assert.That(released.Released, Is.True);
            Assert.That(released.FencingToken, Is.EqualTo(claim.FencingToken));
            Assert.That(released.Reason, Is.Null);
            Assert.That(_surface.LockFor(Key).IsHeld, Is.False);
            Assert.That(BodyOf(Key), Is.EqualTo("unfenced again"));
        });
    }

    [Test]
    public async Task Release_of_a_superseded_token_is_refused_and_leaves_the_claim_live()
    {
        await SeedAsync();
        var first = await ClaimAsync("agent-a");
        _surface.LockFor(Key).ExpireLease();
        await ClaimAsync("agent-b");

        var released = await _store.ReleaseClaimAsync(Key, first.FencingToken!.Value, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(released.Released, Is.False);
            Assert.That(released.Reason, Is.EqualTo("stale"));
            Assert.That(RepoContextClaimFence.Read(_surface.Read(Key)!).IsClaimLive, Is.True);
        });
    }

    [Test]
    public async Task Release_of_a_missing_record_is_reported_rather_than_thrown()
    {
        var released = await _store.ReleaseClaimAsync(Key, 1L, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(released.Released, Is.False);
            Assert.That(released.Reason, Is.EqualTo("missing"));
        });
    }

    [Test]
    public async Task Release_is_idempotent()
    {
        await SeedAsync();
        var claim = await ClaimAsync();
        await _store.ReleaseClaimAsync(Key, claim.FencingToken!.Value, CancellationToken.None);

        var again = await _store.ReleaseClaimAsync(Key, claim.FencingToken!.Value, CancellationToken.None);

        Assert.That(again.Released, Is.True);
    }

    [Test]
    public void Release_rejects_a_key_that_is_not_a_memory_record()
        => Assert.That(
            () => _store.ReleaseClaimAsync(FileKey, 1L, CancellationToken.None),
            Throws.TypeOf<McpException>());

    // ---- status -----------------------------------------------------------

    [Test]
    public async Task Status_reports_a_live_claim_and_the_advisory_lock_state()
    {
        await SeedAsync();
        _surface.LockFor(Key).QueueDepth = 2;
        var claim = await ClaimAsync();

        var status = await _store.ClaimStatusAsync(Key, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(status.Key, Is.EqualTo(Key));
            Assert.That(status.LockName, Is.EqualTo(RepoContextClaimNames.LockName(Key)));
            Assert.That(status.Exists, Is.True);
            Assert.That(status.Claimed, Is.True);
            Assert.That(status.IsHeld, Is.True);
            Assert.That(status.FencingToken, Is.EqualTo(claim.FencingToken));
            Assert.That(status.ReleasedFencingToken, Is.Null);
            Assert.That(status.Owner, Is.EqualTo("agent-a"));
            Assert.That(status.Region, Is.EqualTo(LocalRepoContextReplicaIdentity.LocalReplicaId));
            Assert.That(status.LockFencingToken, Is.EqualTo(claim.FencingToken));
            Assert.That(status.LeaseExpiresAtUtc, Is.Not.Null);
            Assert.That(status.QueueDepth, Is.EqualTo(2));
            Assert.That(status.Authoritative, Is.False, "the lock grant, not this snapshot, is authoritative");
        });
    }

    [Test]
    public async Task Status_reports_a_released_claim_as_no_longer_claimed()
    {
        await SeedAsync();
        var claim = await ClaimAsync();
        await _store.ReleaseClaimAsync(Key, claim.FencingToken!.Value, CancellationToken.None);

        var status = await _store.ClaimStatusAsync(Key, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(status.Claimed, Is.False);
            Assert.That(status.IsHeld, Is.False);
            Assert.That(status.ReleasedFencingToken, Is.EqualTo(claim.FencingToken));
            Assert.That(status.LeaseExpiresAtUtc, Is.Null);
        });
    }

    [Test]
    public async Task Status_reports_a_record_that_does_not_exist()
    {
        var status = await _store.ClaimStatusAsync(Key, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(status.Exists, Is.False);
            Assert.That(status.Claimed, Is.False);
            Assert.That(status.FencingToken, Is.Null);
            Assert.That(status.Owner, Is.Null);
        });
    }

    [Test]
    public void Status_rejects_a_key_that_is_not_a_memory_record()
        => Assert.That(
            () => _store.ClaimStatusAsync(FileKey, CancellationToken.None),
            Throws.TypeOf<McpException>());

    private string? BodyOf(string key)
    {
        var record = _surface.Read(key);
        return record is null ? null : RepoContextClaimFence.DecodeText(record.Body);
    }
}
