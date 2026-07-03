using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Regression guard for OC-6: a resilient, strongly-consistent list scan issued
/// under a system-origin scope must observe every durably-written rule even when
/// the underlying enumerator is reopened mid-scan.
///
/// The resilient scan wrapper (LatticeExtensions.ScanEntriesAsyncCore) emulates
/// one logical scan as a sequence of physical EntriesAsync segments, reopening
/// after a transient EnumerationAbortedException. Those aborts are provoked here
/// by the background CompiledPolicySnapshotMaintainer, which rescans the same
/// sys-auth-policy activation on every rule edit. Before the fix, the caller's
/// system-origin scope was lost on every reopen: the resumed segment resolved to
/// an anonymous subject, the fail-closed gate (DefaultEffect=Deny) denied its
/// range-read and returned a reject-all key-filter, and the segment completed
/// normally with zero rows, silently truncating the scan (typically to the single
/// key seen before the first abort). The fix re-asserts the system-origin scope
/// around every physical segment.
///
/// Racing_scan_after_durable_writes_observes_both_rules is the guard (fails on the
/// pre-fix baseline). Quiesced_scan_after_durable_writes_observes_both_rules is the
/// control: with the maintainer drained no reopen occurs, so it passes even on the
/// baseline, isolating the concurrent-reopen trigger.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ScanReopenPreservesSystemOriginTests
{
    private AuthClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AuthClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    private static async Task<List<string>> CollectRuleIdsAsync(IAsyncEnumerable<LatticeAuthorizationRule> rules)
    {
        var ids = new List<string>();
        await foreach (var rule in rules) ids.Add(rule.RuleId);
        return ids;
    }

    [Test]
    public async Task Racing_scan_after_durable_writes_observes_both_rules()
    {
        var store = _fixture.Store;
        var failures = new List<string>();
        var expected = 0;

        for (int i = 0; i < 30; i++)
        {
            var t1 = $"raw-tree-{i}-x";
            var t2 = $"raw-tree-{i}-y";
            var r1 = $"raw-{i}-1";
            var r2 = $"raw-{i}-2";
            await store.PutRuleAsync(new LatticeAuthorizationRule(r1, LatticeSubjectSelector.User("u"), LatticeScope.Tree(t1), LatticeOperation.Read, LatticeEffect.Allow));
            await store.PutRuleAsync(new LatticeAuthorizationRule(r2, LatticeSubjectSelector.User("u"), LatticeScope.Tree(t2), LatticeOperation.Read, LatticeEffect.Allow));
            expected += 2;

            List<string> seen;
            try { seen = await CollectRuleIdsAsync(store.ListRulesAsync()); }
            catch (Exception ex) { failures.Add($"iter {i}: THREW {ex.GetType().Name}: {ex.Message}"); continue; }

            if (!seen.Contains(r1) || !seen.Contains(r2))
            {
                var missing = new[] { r1, r2 }.Where(r => !seen.Contains(r));
                failures.Add($"iter {i}: missing [{string.Join(",", missing)}] saw {seen.Count}/{expected} distinct={seen.Distinct().Count()}");
            }
        }

        Assert.That(failures, Is.Empty,
            $"RACING single scan omitted a durably-written rule ({failures.Count} failures):\n " +
            string.Join("\n ", failures.Take(30)));
    }

    [Test]
    public async Task Quiesced_scan_after_durable_writes_observes_both_rules()
    {
        var store = _fixture.Store;
        var failures = new List<string>();

        for (int i = 0; i < 30 && failures.Count == 0; i++)
        {
            var t1 = $"quiet-tree-{i}-x";
            var t2 = $"quiet-tree-{i}-y";
            var r1 = $"quiet-{i}-1";
            var r2 = $"quiet-{i}-2";
            await store.PutRuleAsync(new LatticeAuthorizationRule(r1, LatticeSubjectSelector.User("u"), LatticeScope.Tree(t1), LatticeOperation.Read, LatticeEffect.Allow));
            await store.PutRuleAsync(new LatticeAuthorizationRule(r2, LatticeSubjectSelector.User("u"), LatticeScope.Tree(t2), LatticeOperation.Read, LatticeEffect.Allow));

            // Quiesce the background maintainer: force a synchronous rebuild then
            // wait, so no maintainer rescan overlaps the caller scan below.
            await _fixture.RebuildPolicyAsync();
            await Task.Delay(300);

            List<string> seen;
            try { seen = await CollectRuleIdsAsync(store.ListRulesAsync()); }
            catch (Exception ex) { failures.Add($"iter {i}: THREW {ex.GetType().Name}: {ex.Message}"); break; }

            if (!seen.Contains(r1) || !seen.Contains(r2))
            {
                var missing = new[] { r1, r2 }.Where(r => !seen.Contains(r));
                failures.Add($"iter {i}: missing [{string.Join(",", missing)}] (saw {seen.Count} ids: {string.Join(",", seen)})");
            }
        }

        Assert.That(failures, Is.Empty,
            $"QUIESCED single scan omitted a durably-written rule ({failures.Count} failures):\n " +
            string.Join("\n ", failures.Take(10)));
    }
}
