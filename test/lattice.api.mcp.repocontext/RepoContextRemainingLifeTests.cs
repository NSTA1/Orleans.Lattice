namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Unit tests for <see cref="RepoContextRemainingLife"/>: the read-time
/// projection of a repository-context entry's remaining life derived from the
/// absolute UTC expiry that core stamps on a TTL'd write. Pure value logic, so it
/// stays in the fast unit tier.
/// </summary>
[TestFixture]
public sealed class RepoContextRemainingLifeTests
{
    private static readonly DateTime Now =
        new(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);

    [Test]
    public void NeverExpires_reports_a_durable_entry()
    {
        var life = RepoContextRemainingLife.NeverExpires;

        Assert.Multiple(() =>
        {
            Assert.That(life.Expires, Is.False);
            Assert.That(life.HasExpired, Is.False);
            Assert.That(life.Remaining, Is.EqualTo(TimeSpan.Zero));
            Assert.That(life.ExpiresAtTicks, Is.EqualTo(0L));
            Assert.That(life.ExpiresAtUtc, Is.Null);
        });
    }

    [Test]
    public void FromExpiry_with_zero_ticks_is_never_expires()
    {
        var life = RepoContextRemainingLife.FromExpiry(0L, Now.Ticks);
        Assert.That(life, Is.EqualTo(RepoContextRemainingLife.NeverExpires));
    }

    [Test]
    public void FromExpiry_with_a_future_expiry_reports_remaining_life()
    {
        var expiresAt = Now.AddMinutes(30);
        var life = RepoContextRemainingLife.FromExpiry(expiresAt.Ticks, Now.Ticks);

        Assert.Multiple(() =>
        {
            Assert.That(life.Expires, Is.True);
            Assert.That(life.HasExpired, Is.False);
            Assert.That(life.Remaining, Is.EqualTo(TimeSpan.FromMinutes(30)));
            Assert.That(life.ExpiresAtTicks, Is.EqualTo(expiresAt.Ticks));
            Assert.That(life.ExpiresAtUtc, Is.EqualTo(expiresAt));
            Assert.That(life.ExpiresAtUtc!.Value.Kind, Is.EqualTo(DateTimeKind.Utc));
        });
    }

    [Test]
    public void FromExpiry_with_a_past_expiry_reports_expired_with_zero_remaining()
    {
        var expiresAt = Now.AddMinutes(-5);
        var life = RepoContextRemainingLife.FromExpiry(expiresAt.Ticks, Now.Ticks);

        Assert.Multiple(() =>
        {
            Assert.That(life.Expires, Is.True);
            Assert.That(life.HasExpired, Is.True);
            Assert.That(life.Remaining, Is.EqualTo(TimeSpan.Zero));
            Assert.That(life.ExpiresAtTicks, Is.EqualTo(expiresAt.Ticks));
        });
    }

    [Test]
    public void FromExpiry_at_the_exact_expiry_instant_is_expired()
    {
        var life = RepoContextRemainingLife.FromExpiry(Now.Ticks, Now.Ticks);

        Assert.Multiple(() =>
        {
            Assert.That(life.HasExpired, Is.True);
            Assert.That(life.Remaining, Is.EqualTo(TimeSpan.Zero));
        });
    }

    [Test]
    public void FromExpiry_datetime_overload_matches_the_ticks_overload()
    {
        var expiresAt = Now.AddHours(2);

        var viaDateTime = RepoContextRemainingLife.FromExpiry(expiresAt.Ticks, Now);
        var viaTicks = RepoContextRemainingLife.FromExpiry(expiresAt.Ticks, Now.Ticks);

        Assert.That(viaDateTime, Is.EqualTo(viaTicks));
    }

    [Test]
    public void FromExpiry_datetime_overload_converts_a_local_instant_to_utc()
    {
        var expiresAt = Now.AddHours(1);
        var nowLocal = Now.ToLocalTime();

        var life = RepoContextRemainingLife.FromExpiry(expiresAt.Ticks, nowLocal);

        Assert.That(life.Remaining, Is.EqualTo(TimeSpan.FromHours(1)));
    }

    [Test]
    public void FromVersionedValue_projects_the_stored_expiry()
    {
        var expiresAt = Now.AddMinutes(10);
        var value = new VersionedValue { ExpiresAtTicks = expiresAt.Ticks };

        var life = RepoContextRemainingLife.FromVersionedValue(value, Now);

        Assert.Multiple(() =>
        {
            Assert.That(life.Expires, Is.True);
            Assert.That(life.Remaining, Is.EqualTo(TimeSpan.FromMinutes(10)));
            Assert.That(life.ExpiresAtTicks, Is.EqualTo(expiresAt.Ticks));
        });
    }

    [Test]
    public void FromVersionedValue_on_a_non_expiring_value_is_never_expires()
    {
        var value = new VersionedValue { ExpiresAtTicks = 0L };
        var life = RepoContextRemainingLife.FromVersionedValue(value, Now);
        Assert.That(life, Is.EqualTo(RepoContextRemainingLife.NeverExpires));
    }

    [Test]
    public void FromVersionedValue_rejects_a_null_value()
        => Assert.Throws<ArgumentNullException>(
            () => RepoContextRemainingLife.FromVersionedValue(null!, Now));
}
