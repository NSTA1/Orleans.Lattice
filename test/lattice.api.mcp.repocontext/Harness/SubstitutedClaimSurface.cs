using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// A unit-lane stand-in for the two grains the claim surface talks to: the memory
/// tree it reads and writes records through, and the named distributed lock it
/// takes claims under.
/// <para>
/// It is deliberately faithful on the two points the fencing design rests on. The
/// memory tree applies a real <see cref="MvRegisterDelta"/> through
/// <see cref="MvRegister.MergeDelta"/>, so a write travels the same
/// read-fold-merge path a silo would give it rather than a blind dictionary
/// overwrite. The lock mints a strictly increasing fencing token per grant and
/// never reuses one, which is the single property the whole fencing argument
/// depends on.
/// </para>
/// <para>
/// It contains no clock and no timer: leases are advanced by an explicit
/// <see cref="FakeLatticeLockGrain.ExpireLease"/> call, so an expiry test states
/// the transition it wants instead of waiting for one.
/// </para>
/// </summary>
internal sealed class SubstitutedClaimSurface
{
    private readonly SortedDictionary<string, byte[]> _memory = new(StringComparer.Ordinal);
    private readonly Dictionary<string, long> _expiries = new(StringComparer.Ordinal);
    private readonly Dictionary<string, FakeLatticeLockGrain> _locks = new(StringComparer.Ordinal);
    private readonly Serializer _serializer;

    /// <summary>Creates the substituted surface.</summary>
    /// <param name="serializer">The Orleans serializer records are written with. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="serializer"/> is null.</exception>
    public SubstitutedClaimSurface(Serializer serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        _serializer = serializer;

        MemoryTree = BuildMemoryTree();
        var other = BuildInertTree();

        GrainFactory = Substitute.For<IGrainFactory>();
        GrainFactory.GetGrain<ILattice>(Arg.Any<string>()).Returns(call =>
            call.ArgAt<string>(0) == RepoContextTrees.Memory ? MemoryTree : other);
        GrainFactory.GetGrain<ILatticeLockGrain>(Arg.Any<string>()).Returns(call => Lock(call.ArgAt<string>(0)));
    }

    /// <summary>The substituted grain factory the store resolves trees and locks through.</summary>
    public IGrainFactory GrainFactory { get; }

    /// <summary>The substituted memory tree.</summary>
    public ILattice MemoryTree { get; }

    /// <summary>
    /// Invoked after every memory read, so a test can interleave a concurrent
    /// delete between a store's existence probe and the write that follows it.
    /// </summary>
    public Action<string>? AfterRead { get; set; }

    /// <summary>Builds a store wired to this surface.</summary>
    /// <param name="replicaIdentity">The replica identity to author writes under, or <see langword="null"/> for the local identity.</param>
    /// <returns>The store under test.</returns>
    public RepoContextStore Store(IRepoContextReplicaIdentity? replicaIdentity = null)
        => new(
            GrainFactory,
            Substitute.For<IRepoIndexRunner>(),
            _serializer,
            new RepoContextVectorWriter(
                GrainFactory,
                _serializer,
                Substitute.For<ILatticeReplicationContext>(),
                new RepoContextVectorCache(TimeProvider.System, new RepoContextIndexingOptions()),
                RepoContextVectorPlaneTestDoubles.ReDeriver(GrainFactory)),
            TtlOptions(),
            TimeProvider.System,
            replicaIdentity);

    private static IOptionsMonitor<RepoContextTtlOptions> TtlOptions()
    {
        // The store reads the per-repo TTL window on every memory write, so a bare
        // substitute (whose Get returns null) would fault before the fence is ever
        // exercised.
        var options = new RepoContextTtlOptions();
        var monitor = Substitute.For<IOptionsMonitor<RepoContextTtlOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    /// <summary>Returns the fake lock for <paramref name="lockName"/>, creating it on first use.</summary>
    /// <param name="lockName">The lock name.</param>
    /// <returns>The fake lock grain.</returns>
    public FakeLatticeLockGrain Lock(string lockName)
    {
        if (!_locks.TryGetValue(lockName, out var padlock))
        {
            padlock = new FakeLatticeLockGrain();
            _locks[lockName] = padlock;
        }

        return padlock;
    }

    /// <summary>Returns the fake lock guarding <paramref name="key"/>.</summary>
    /// <param name="key">The claimed record's key.</param>
    /// <returns>The fake lock grain.</returns>
    public FakeLatticeLockGrain LockFor(string key) => Lock(RepoContextClaimNames.LockName(key));

    /// <summary>Whether a live value is stored at <paramref name="key"/>.</summary>
    /// <param name="key">The record key.</param>
    /// <returns><see langword="true"/> when a value is stored.</returns>
    public bool Exists(string key) => _memory.ContainsKey(key);

    /// <summary>Reads the folded memory record at <paramref name="key"/>, or null.</summary>
    /// <param name="key">The record key.</param>
    /// <returns>The folded record, or <see langword="null"/>.</returns>
    public MemoryRecord? Read(string key)
        => RepoContextMemoryCodec.Fold(_memory.TryGetValue(key, out var stored) ? stored : null, _serializer);

    /// <summary>Removes the value at <paramref name="key"/>, as a concurrent delete would.</summary>
    /// <param name="key">The record key.</param>
    public void Drop(string key) => _memory.Remove(key);

    private ILattice BuildMemoryTree()
    {
        var tree = Substitute.For<ILattice>();

        tree.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ReturnsForAnyArgs(call =>
            {
                var key = call.ArgAt<string>(0);
                var value = _memory.TryGetValue(key, out var stored) ? stored : null;
                AfterRead?.Invoke(key);
                return Task.FromResult(value);
            });

        tree.GetWithVersionAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ReturnsForAnyArgs(call =>
            {
                var key = call.ArgAt<string>(0);
                return Task.FromResult(new VersionedValue
                {
                    Value = _memory.TryGetValue(key, out var value) ? value : null,
                    ExpiresAtTicks = _expiries.TryGetValue(key, out var expiry) ? expiry : 0L,
                });
            });

        tree.ApplyCrdtDeltaAsync(
                Arg.Any<string>(), Arg.Any<LatticeMergeMode>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .ReturnsForAnyArgs(call => Task.FromResult(
                Apply(call.ArgAt<string>(0), call.ArgAt<byte[]>(2), ttl: null)));

        tree.ApplyCrdtDeltaAsync(
                Arg.Any<string>(), Arg.Any<LatticeMergeMode>(), Arg.Any<byte[]>(), Arg.Any<TimeSpan>(),
                Arg.Any<CancellationToken>())
            .ReturnsForAnyArgs(call => Task.FromResult(
                Apply(call.ArgAt<string>(0), call.ArgAt<byte[]>(2), call.ArgAt<TimeSpan>(3))));

        tree.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .ReturnsForAnyArgs(call =>
            {
                _memory[call.ArgAt<string>(0)] = call.ArgAt<byte[]>(1);
                return Task.CompletedTask;
            });

        tree.DeleteAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ReturnsForAnyArgs(call => Task.FromResult(_memory.Remove(call.ArgAt<string>(0))));

        return tree;
    }

    private HybridLogicalClock Apply(string key, byte[] deltaBytes, TimeSpan? ttl)
    {
        var register = _memory.TryGetValue(key, out var stored)
            ? JsonLatticeSerializer<MvRegister>.Default.Deserialize(stored)
            : new MvRegister();
        register.MergeDelta(JsonLatticeSerializer<MvRegisterDelta>.Default.Deserialize(deltaBytes));
        _memory[key] = JsonLatticeSerializer<MvRegister>.Default.Serialize(register);
        if (ttl is { } window)
        {
            _expiries[key] = DateTime.UtcNow.Add(window).Ticks;
        }

        return HybridLogicalClock.Tick(HybridLogicalClock.Zero);
    }

    private static ILattice BuildInertTree()
    {
        var tree = Substitute.For<ILattice>();
        tree.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ReturnsForAnyArgs(Task.FromResult<byte[]?>(null));
        return tree;
    }
}
