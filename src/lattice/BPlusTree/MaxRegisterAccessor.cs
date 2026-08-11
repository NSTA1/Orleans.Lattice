using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice;

/// <summary>
/// Typed value-surface accessor for a monotone <em>max</em> register stored
/// under a single key in an <see cref="ILattice"/> - the high-water-mark
/// primitive that keeps the greatest totally-ordered value ever written. The
/// accessor is a lightweight, allocation-free wrapper - construct it once via
/// <see cref="CrdtLatticeExtensions.MaxRegister{T}(ILattice, string, System.Func{T, byte[]}, ILatticeSerializer{T}?)"/>
/// and reuse it for any number of operations on the same key.
/// <para>
/// A write advances the register only when the candidate beats the current
/// value under the total order carried on the wire (a strictly greater order
/// key); backwards writes and duplicate deliveries are no-ops. Concurrent
/// active-active writes from different clusters converge on the single greatest
/// value because the fold is a directional max over a total order - commutative,
/// associative, and idempotent - and the receiver never needs the domain
/// comparer.
/// </para>
/// </summary>
/// <typeparam name="T">The user-facing value type. Serialised to and from <see cref="byte"/>[] through <see cref="ILatticeSerializer{T}"/>.</typeparam>
public readonly record struct MaxRegisterAccessor<T>
{
    /// <summary>Default CAS retry budget for mutating operations.</summary>
    public const int DefaultMaxAttempts = 16;

    private const bool Direction = false;

    private readonly ILattice _lattice;
    private readonly string _key;
    private readonly ILatticeSerializer<T> _serializer;
    private readonly Func<T, byte[]> _orderKeySelector;

    internal MaxRegisterAccessor(ILattice lattice, string key, Func<T, byte[]> orderKeySelector, ILatticeSerializer<T> serializer)
    {
        _lattice = lattice;
        _key = key;
        _orderKeySelector = orderKeySelector;
        _serializer = serializer;
    }

    /// <summary>The tree the accessor is bound to.</summary>
    public ILattice Lattice => _lattice;

    /// <summary>The key the accessor reads and writes.</summary>
    public string Key => _key;

    /// <summary>The serializer used to translate <typeparamref name="T"/> to and from <see cref="byte"/>[].</summary>
    public ILatticeSerializer<T> Serializer => _serializer;

    /// <summary>The order-key selector deriving the total-order key carried on the wire for a value.</summary>
    public Func<T, byte[]> OrderKeySelector => _orderKeySelector;

    /// <summary>
    /// Advances the register towards <paramref name="value"/>. The write is a
    /// blind candidate delta: the receiver folds it as a directional max and
    /// keeps it only when it beats the current value, so a backwards write is a
    /// durable no-op. No read is required, keeping the write single-hop.
    /// </summary>
    /// <param name="value">The candidate value to store.</param>
    /// <param name="cancellationToken">Cancels the write hop.</param>
    /// <param name="maxAttempts">Reserved for API parity with the read-modify-write accessors; the blind-delta write does not retry.</param>
    public Task SetAsync(T value, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);
        EnsureInitialised();
        var deltaBytes = BoundedRegisterAccessorHelper.EncodeDelta(_serializer, _orderKeySelector, value);
        return _lattice.ApplyCrdtDeltaAsync(_key, LatticeMergeMode.MaxRegister, deltaBytes, cancellationToken);
    }

    /// <summary>
    /// Reads the current value, or <see langword="default"/> when the register
    /// has never been written.
    /// </summary>
    public async Task<T?> GetAsync(CancellationToken cancellationToken = default)
    {
        EnsureInitialised();
        var register = await BoundedRegisterAccessorHelper.ReadAsync(_lattice, _key, Direction, cancellationToken).ConfigureAwait(false);
        return register.HasValue ? _serializer.Deserialize(register.Value!) : default;
    }

    /// <summary>Returns whether the register has been written at least once.</summary>
    public async Task<bool> HasValueAsync(CancellationToken cancellationToken = default)
    {
        EnsureInitialised();
        var register = await BoundedRegisterAccessorHelper.ReadAsync(_lattice, _key, Direction, cancellationToken).ConfigureAwait(false);
        return register.HasValue;
    }

    /// <summary>
    /// Reads the current register state. Returns an empty (never-written) max
    /// register when the key is absent.
    /// </summary>
    public Task<BoundedRegister> GetRegisterAsync(CancellationToken cancellationToken = default)
    {
        EnsureInitialised();
        return BoundedRegisterAccessorHelper.ReadAsync(_lattice, _key, Direction, cancellationToken);
    }

    /// <summary>
    /// Merges <paramref name="other"/> into the stored register: the other side's
    /// value advances the register only when it beats the current value under the
    /// max direction. Useful for replication consumers that have computed a
    /// register out-of-band.
    /// </summary>
    public Task MergeAsync(BoundedRegister other, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentNullException.ThrowIfNull(other);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);
        EnsureInitialised();
        if (!other.HasValue) return Task.CompletedTask;
        var delta = new BoundedRegisterDelta
        {
            Value = other.Value,
            OrderKey = other.OrderKey,
            HasValue = true,
        };
        var deltaBytes = JsonLatticeSerializer<BoundedRegisterDelta>.Default.Serialize(delta);
        return _lattice.ApplyCrdtDeltaAsync(_key, LatticeMergeMode.MaxRegister, deltaBytes, cancellationToken);
    }

    private void EnsureInitialised()
    {
        if (_lattice is null || _serializer is null || _orderKeySelector is null)
        {
            throw new InvalidOperationException(
                "MaxRegisterAccessor is uninitialised; obtain it via ILattice.MaxRegister<T>(key, orderKeySelector) instead of `default`.");
        }
    }
}
