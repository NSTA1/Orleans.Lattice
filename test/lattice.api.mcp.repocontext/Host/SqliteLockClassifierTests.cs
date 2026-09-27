using Microsoft.Data.Sqlite;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers <see cref="SqliteLockClassifier"/>, which decides which grain-storage
/// failures are attributed as lock failures (issue #2431).
/// </summary>
[TestFixture]
public sealed class SqliteLockClassifierTests
{
    [TestCase(SqliteLockClassifier.SqliteBusy)]
    [TestCase(SqliteLockClassifier.SqliteLocked)]
    public void A_busy_or_locked_result_code_is_a_lock_failure(int code)
    {
        var failure = new SqliteException("database is locked", code);

        Assert.Multiple(() =>
        {
            Assert.That(SqliteLockClassifier.TryFind(failure, out var found), Is.True);
            Assert.That(found, Is.SameAs(failure));
        });
    }

    [Test]
    public void An_extended_busy_code_is_classified_by_its_primary_code()
    {
        // SQLITE_BUSY_SNAPSHOT (517) is primary code 5; Microsoft.Data.Sqlite reports the
        // primary code on SqliteErrorCode and the extended one separately.
        var failure = new SqliteException("database is locked", SqliteLockClassifier.SqliteBusy, 517);

        Assert.That(SqliteLockClassifier.TryFind(failure, out _), Is.True);
    }

    [TestCase(1)]
    [TestCase(19)]
    public void Any_other_sqlite_result_code_is_not_a_lock_failure(int code)
    {
        Assert.That(SqliteLockClassifier.TryFind(new SqliteException("other", code), out var found), Is.False);
        Assert.That(found, Is.Null);
    }

    [Test]
    public void A_lock_failure_wrapped_in_inner_exceptions_is_found()
    {
        var locked = new SqliteException("database is locked", SqliteLockClassifier.SqliteBusy);
        var wrapped = new InvalidOperationException("outer", new TimeoutException("middle", locked));

        Assert.Multiple(() =>
        {
            Assert.That(SqliteLockClassifier.TryFind(wrapped, out var found), Is.True,
                "A wrapping layer added above the provider must not silently turn every lock failure "
                + "into an unattributed one.");
            Assert.That(found, Is.SameAs(locked));
        });
    }

    [Test]
    public void A_lock_failure_in_any_branch_of_an_aggregate_is_found()
    {
        var locked = new SqliteException("database is locked", SqliteLockClassifier.SqliteLocked);
        var aggregate = new AggregateException(new InvalidOperationException("first"), locked);

        Assert.Multiple(() =>
        {
            Assert.That(SqliteLockClassifier.TryFind(aggregate, out var found), Is.True);
            Assert.That(found, Is.SameAs(locked));
        });
    }

    [Test]
    public void A_null_or_unrelated_exception_is_not_a_lock_failure()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SqliteLockClassifier.TryFind(null, out _), Is.False);
            Assert.That(SqliteLockClassifier.TryFind(new InvalidOperationException("database is locked"), out _), Is.False,
                "Classification is by result code, never by message text: a message match would "
                + "attribute an exception that only quotes the phrase.");
        });
    }

    [Test]
    public void The_cause_chain_walk_is_bounded()
    {
        Exception chain = new SqliteException("database is locked", SqliteLockClassifier.SqliteBusy);
        for (var i = 0; i < 64; i++)
        {
            chain = new InvalidOperationException("layer " + i, chain);
        }

        Assert.That(SqliteLockClassifier.TryFind(chain, out _), Is.False,
            "Beyond the depth bound the walk stops rather than following an arbitrarily deep chain.");
    }
}
