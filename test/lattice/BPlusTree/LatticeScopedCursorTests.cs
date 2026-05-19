using NSubstitute;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for the <see cref="LatticeScopedCursor"/> type and the
/// <c>LatticeExtensions.Open*CursorScopeAsync</c> family of extension
/// methods. Validates the IAsyncDisposable contract, the implicit
/// string conversion, and the per-overload forwarding to the underlying
/// <see cref="ILattice"/> open call.
/// </summary>
public class LatticeScopedCursorTests
{
    // --- LatticeScopedCursor type ---

    [Test]
    public void Ctor_throws_on_null_lattice()
    {
        Assert.That(() => new LatticeScopedCursor(null!, "cursor-1"),
            Throws.TypeOf<ArgumentNullException>());
    }

    [Test]
    public void Ctor_throws_on_null_cursorId()
    {
        var lattice = Substitute.For<ILattice>();
        Assert.That(() => new LatticeScopedCursor(lattice, null!),
            Throws.TypeOf<ArgumentNullException>());
    }

    [Test]
    public void Id_returns_cursorId_passed_to_constructor()
    {
        var lattice = Substitute.For<ILattice>();
        var scope = new LatticeScopedCursor(lattice, "cursor-42");

        Assert.That(scope.Id, Is.EqualTo("cursor-42"));
    }

    [Test]
    public void Implicit_string_conversion_returns_cursor_id()
    {
        var lattice = Substitute.For<ILattice>();
        var scope = new LatticeScopedCursor(lattice, "cursor-7");

        string asString = scope;

        Assert.That(asString, Is.EqualTo("cursor-7"));
    }

    [Test]
    public void Implicit_string_conversion_throws_on_null_scope()
    {
        LatticeScopedCursor? scope = null;
        Assert.That(() => { string _ = scope!; },
            Throws.TypeOf<ArgumentNullException>());
    }

    [Test]
    public async Task DisposeAsync_closes_underlying_cursor_once()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.CloseCursorAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var scope = new LatticeScopedCursor(lattice, "cursor-9");
        await scope.DisposeAsync();

        await lattice.Received(1).CloseCursorAsync("cursor-9", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task DisposeAsync_is_idempotent()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.CloseCursorAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var scope = new LatticeScopedCursor(lattice, "cursor-9");
        await scope.DisposeAsync();
        await scope.DisposeAsync();
        await scope.DisposeAsync();

        await lattice.Received(1).CloseCursorAsync("cursor-9", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task AwaitUsing_block_closes_cursor_on_normal_exit()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.CloseCursorAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        await using (var scope = new LatticeScopedCursor(lattice, "cursor-x"))
        {
            Assert.That(scope.Id, Is.EqualTo("cursor-x"));
        }

        await lattice.Received(1).CloseCursorAsync("cursor-x", Arg.Any<CancellationToken>());
    }

    [Test]
    public void AwaitUsing_block_closes_cursor_on_exception()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.CloseCursorAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        Assert.That(async () =>
        {
            await using var scope = new LatticeScopedCursor(lattice, "cursor-y");
            throw new InvalidOperationException("boom");
        }, Throws.TypeOf<InvalidOperationException>());

        lattice.Received(1).CloseCursorAsync("cursor-y", Arg.Any<CancellationToken>());
    }

    // --- OpenKeyCursorScopeAsync ---

    [Test]
    public void OpenKeyCursorScopeAsync_throws_on_null_lattice()
    {
        Assert.That(async () => await LatticeExtensions.OpenKeyCursorScopeAsync(null!),
            Throws.TypeOf<ArgumentNullException>());
    }

    [Test]
    public async Task OpenKeyCursorScopeAsync_forwards_arguments_and_returns_scope()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.OpenKeyCursorAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(),
            Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult("cursor-k"));

        var scope = await lattice.OpenKeyCursorScopeAsync(
            startInclusive: "a",
            endExclusive: "z",
            reverse: true,
            pointInTime: true);

        Assert.That(scope.Id, Is.EqualTo("cursor-k"));
        await lattice.Received(1).OpenKeyCursorAsync("a", "z", true, true, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task OpenKeyCursorScopeAsync_dispose_calls_CloseCursorAsync_with_returned_id()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.OpenKeyCursorAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(),
            Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult("cursor-keep"));
        lattice.CloseCursorAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        await using (var _ = await lattice.OpenKeyCursorScopeAsync()) { }

        await lattice.Received(1).CloseCursorAsync("cursor-keep", Arg.Any<CancellationToken>());
    }

    // --- OpenEntryCursorScopeAsync ---

    [Test]
    public void OpenEntryCursorScopeAsync_throws_on_null_lattice()
    {
        Assert.That(async () => await LatticeExtensions.OpenEntryCursorScopeAsync(null!),
            Throws.TypeOf<ArgumentNullException>());
    }

    [Test]
    public async Task OpenEntryCursorScopeAsync_forwards_arguments_and_returns_scope()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.OpenEntryCursorAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(),
            Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult("cursor-e"));

        var scope = await lattice.OpenEntryCursorScopeAsync(
            startInclusive: "k",
            endExclusive: "m",
            reverse: false,
            pointInTime: true);

        Assert.That(scope.Id, Is.EqualTo("cursor-e"));
        await lattice.Received(1).OpenEntryCursorAsync("k", "m", false, true, Arg.Any<CancellationToken>());
    }

    // --- OpenSnapshotKeyCursorScopeAsync ---

    [Test]
    public void OpenSnapshotKeyCursorScopeAsync_throws_on_null_lattice()
    {
        Assert.That(async () => await LatticeExtensions.OpenSnapshotKeyCursorScopeAsync(null!),
            Throws.TypeOf<ArgumentNullException>());
    }

    [Test]
    public async Task OpenSnapshotKeyCursorScopeAsync_forwards_arguments_and_returns_scope()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.OpenSnapshotKeyCursorAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(),
            Arg.Any<CancellationToken>())
            .Returns(Task.FromResult("cursor-sk"));

        var scope = await lattice.OpenSnapshotKeyCursorScopeAsync(
            startInclusive: "p",
            endExclusive: "t",
            reverse: true);

        Assert.That(scope.Id, Is.EqualTo("cursor-sk"));
        await lattice.Received(1).OpenSnapshotKeyCursorAsync("p", "t", true, Arg.Any<CancellationToken>());
    }

    // --- OpenSnapshotEntryCursorScopeAsync ---

    [Test]
    public void OpenSnapshotEntryCursorScopeAsync_throws_on_null_lattice()
    {
        Assert.That(async () => await LatticeExtensions.OpenSnapshotEntryCursorScopeAsync(null!),
            Throws.TypeOf<ArgumentNullException>());
    }

    [Test]
    public async Task OpenSnapshotEntryCursorScopeAsync_forwards_arguments_and_returns_scope()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.OpenSnapshotEntryCursorAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(),
            Arg.Any<CancellationToken>())
            .Returns(Task.FromResult("cursor-se"));

        var scope = await lattice.OpenSnapshotEntryCursorScopeAsync();

        Assert.That(scope.Id, Is.EqualTo("cursor-se"));
        await lattice.Received(1).OpenSnapshotEntryCursorAsync(null, null, false, Arg.Any<CancellationToken>());
    }

    // --- OpenDeleteRangeCursorScopeAsync ---

    [Test]
    public void OpenDeleteRangeCursorScopeAsync_throws_on_null_lattice()
    {
        Assert.That(async () => await LatticeExtensions.OpenDeleteRangeCursorScopeAsync(null!, "a", "z"),
            Throws.TypeOf<ArgumentNullException>());
    }

    [Test]
    public async Task OpenDeleteRangeCursorScopeAsync_forwards_arguments_and_returns_scope()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.OpenDeleteRangeCursorAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult("cursor-d"));

        var scope = await lattice.OpenDeleteRangeCursorScopeAsync("a", "z");

        Assert.That(scope.Id, Is.EqualTo("cursor-d"));
        await lattice.Received(1).OpenDeleteRangeCursorAsync("a", "z", Arg.Any<CancellationToken>());
    }

    // --- Implicit conversion in caller code ---

    [Test]
    public async Task Scope_can_be_passed_directly_to_NextKeysAsync_via_implicit_conversion()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.OpenKeyCursorAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(),
            Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult("cursor-impl"));
        var page = new LatticeCursorKeysPage { Keys = new[] { "a" }, HasMore = false };
        lattice.NextKeysAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(page));

        await using var scope = await lattice.OpenKeyCursorScopeAsync();
        await lattice.NextKeysAsync(scope, 10);

        await lattice.Received(1).NextKeysAsync("cursor-impl", 10, Arg.Any<CancellationToken>());
    }
}

