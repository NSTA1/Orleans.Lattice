using System.Text;
using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Acceptance matrix for the change-feed (<see cref="ILatticeStateObserver"/>)
/// arm of the state-API auth-backed read visibility (issue #981). The change
/// feed tails the write-ahead log directly rather than flowing through the gated
/// <see cref="ILattice"/> surface, so it must honour the data-plane read policy
/// itself: an anonymous or unauthorized subject is refused the subscription, a
/// partial (prefix) grant only observes changes to keys it may read (and never a
/// range delete, which cannot be narrowed to an authorized subset), and the
/// bootstrap administrator observes everything - proving the filter scopes by
/// decision rather than blanket-denying.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AuthApiStateChangeFeedVisibilityTests
{
    private AuthApiStateClusterFixture _fixture = null!;

    private const string TreeA = "feed-tree-a";
    private const string TreeB = "feed-tree-b";
    private const string Reader = "feed-reader";

    private ILattice _treeA = null!;
    private ILattice _treeB = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AuthApiStateClusterFixture();
        await _fixture.InitializeAsync();

        _treeA = await _fixture.RegisterTreeAsync(TreeA);
        _treeB = await _fixture.RegisterTreeAsync(TreeB);

        // The reader may read only treeA's "x/" prefix; "y/" on treeA and all of
        // treeB are off-limits.
        await _fixture.GrantAsync(new LatticeAuthorizationRule(
            "feed-reader-x",
            LatticeSubjectSelector.User(Reader),
            LatticeScope.Prefix(TreeA, "x/"),
            LatticeOperation.Read | LatticeOperation.RangeRead,
            LatticeEffect.Allow));
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    private static byte[] V(string value) => Encoding.UTF8.GetBytes(value);

    private Task WriteAsAdminAsync(Func<Task> writes)
    {
        return RunAsync();

        async Task RunAsync()
        {
            using (AuthApiStateClusterFixture.AsSubject(AuthApiStateClusterFixture.BootstrapAdmin))
            {
                await writes();
            }
        }
    }

    [Test]
    public void Observe_unresolved_identity_is_denied()
    {
        // No AsSubject scope: the caller is anonymous. The subscription is refused
        // fail-closed, reported as not-found like every other read surface, rather
        // than streaming the tree's write-ahead log.
        Assert.ThrowsAsync<KeyNotFoundException>(async () =>
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await foreach (var _ in _fixture.Observer.ObserveAsync(
                new StateObserveRequest { TreeId = TreeA }, cts.Token))
            {
                break;
            }
        });
    }

    [Test]
    public void Observe_reader_is_denied_an_unreadable_tree()
    {
        Assert.ThrowsAsync<KeyNotFoundException>(async () =>
        {
            using (AuthApiStateClusterFixture.AsSubject(Reader))
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                await foreach (var _ in _fixture.Observer.ObserveAsync(
                    new StateObserveRequest { TreeId = TreeB }, cts.Token))
                {
                    break;
                }
            }
        });
    }

    [Test]
    public async Task Observe_reader_sees_only_the_permitted_prefix()
    {
        using (AuthApiStateClusterFixture.AsSubject(Reader))
        {
            var notifications = await _fixture.ObserveWhileAsync(
                new StateObserveRequest { TreeId = TreeA },
                expectedCount: 1,
                () => WriteAsAdminAsync(async () =>
                {
                    await _treeA.SetAsync("y/live", V("hidden"));
                    await _treeA.SetAsync("x/live", V("visible"));
                }));

            Assert.That(notifications.Select(n => n.Key), Is.EqualTo(new[] { "x/live" }));
        }
    }

    [Test]
    public async Task Observe_partial_reader_never_sees_a_range_delete()
    {
        using (AuthApiStateClusterFixture.AsSubject(Reader))
        {
            // A range delete over the readable prefix cannot be narrowed to an
            // authorized key subset, so a partially-authorized subscriber must not
            // observe it; the subsequent point set to a readable key confirms the
            // stream is still live and keeps delivering point changes.
            var notifications = await _fixture.ObserveWhileAsync(
                new StateObserveRequest { TreeId = TreeA },
                expectedCount: 1,
                () => WriteAsAdminAsync(async () =>
                {
                    await _treeA.DeleteRangeAsync("x/", "x/\uffff");
                    await _treeA.SetAsync("x/after", V("v"));
                }));

            Assert.Multiple(() =>
            {
                Assert.That(notifications.Select(n => n.Key), Is.EqualTo(new[] { "x/after" }));
                Assert.That(notifications.Any(n => n.Kind == StateChangeKind.DeleteRange), Is.False);
            });
        }
    }

    [Test]
    public async Task Observe_bootstrap_admin_sees_every_change()
    {
        using (AuthApiStateClusterFixture.AsSubject(AuthApiStateClusterFixture.BootstrapAdmin))
        {
            var notifications = await _fixture.ObserveWhileAsync(
                new StateObserveRequest { TreeId = TreeB },
                expectedCount: 2,
                () => WriteAsAdminAsync(async () =>
                {
                    await _treeB.SetAsync("b/1", V("v1"));
                    await _treeB.SetAsync("b/2", V("v2"));
                }));

            Assert.That(
                notifications.Select(n => n.Key).OrderBy(k => k, StringComparer.Ordinal),
                Is.EqualTo(new[] { "b/1", "b/2" }));
        }
    }
}
