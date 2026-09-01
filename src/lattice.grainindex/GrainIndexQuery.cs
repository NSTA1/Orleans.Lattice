using System.Runtime.CompilerServices;
using Orleans.Lattice.GrainIndex.Query;

namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// The planned-query implementation behind
/// <see cref="IGrainIndexQuery{TGrain}"/>. Immutable: the plan is built once and
/// then shared by every derived query and every enumeration.
/// </summary>
/// <typeparam name="TGrain">The indexed grain interface type.</typeparam>
internal sealed class GrainIndexQuery<TGrain> : IGrainIndexQuery<TGrain>
    where TGrain : IGrain
{
    private readonly GrainIndexQueryPlan _plan;
    private readonly GrainIndexQueryExecutor _executor;
    private readonly IGrainKeyCodec<TGrain> _keyCodec;
    private readonly IGrainFactory _grainFactory;

    internal GrainIndexQuery(
        GrainIndexQueryPlan plan,
        GrainIndexQueryExecutor executor,
        IGrainKeyCodec<TGrain> keyCodec,
        IGrainFactory grainFactory,
        int pageSize,
        GrainIndexQueryExecution execution)
    {
        _plan = plan;
        _executor = executor;
        _keyCodec = keyCodec;
        _grainFactory = grainFactory;
        PageSize = pageSize;
        Execution = execution;
    }

    /// <inheritdoc />
    public int PageSize { get; }

    /// <inheritdoc />
    public GrainIndexQueryExecution Execution { get; }

    /// <inheritdoc />
    public IGrainIndexQuery<TGrain> WithPageSize(int pageSize)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(pageSize, 1);
        return new GrainIndexQuery<TGrain>(_plan, _executor, _keyCodec, _grainFactory, pageSize, Execution);
    }

    /// <inheritdoc />
    public IGrainIndexQuery<TGrain> WithExecution(GrainIndexQueryExecution execution)
    {
        if (execution is not (GrainIndexQueryExecution.DurableCursor
            or GrainIndexQueryExecution.Stream
            or GrainIndexQueryExecution.SnapshotCursor))
        {
            throw new ArgumentOutOfRangeException(
                nameof(execution),
                execution,
                "Unknown grain-index query execution mode.");
        }

        return new GrainIndexQuery<TGrain>(_plan, _executor, _keyCodec, _grainFactory, PageSize, execution);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<TGrain> ToGrainsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Payloads are not requested: resolving a grain needs its key only, so
        // the scan runs over the key-only surface and no entry body is
        // transferred.
        await foreach (var match in _executor
            .ExecuteAsync(_plan, PageSize, Execution, payloads: false, cancellationToken)
            .ConfigureAwait(false))
        {
            yield return _keyCodec.Resolve(_grainFactory, match.GrainKey);
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<string> ToKeysAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var match in _executor
            .ExecuteAsync(_plan, PageSize, Execution, payloads: false, cancellationToken)
            .ConfigureAwait(false))
        {
            yield return match.GrainKey;
        }
    }

    /// <inheritdoc />
    public IAsyncEnumerable<GrainIndexMatch> ToMatchesAsync(CancellationToken cancellationToken = default) =>
        _executor.ExecuteAsync(_plan, PageSize, Execution, payloads: true, cancellationToken);

    /// <inheritdoc />
    public async Task<IReadOnlyList<TGrain>> ToGrainListAsync(CancellationToken cancellationToken = default)
    {
        List<TGrain>? grains = null;
        await foreach (var match in _executor
            .ExecuteAsync(_plan, PageSize, Execution, payloads: false, cancellationToken)
            .ConfigureAwait(false))
        {
            grains ??= [];
            grains.Add(_keyCodec.Resolve(_grainFactory, match.GrainKey));
        }

        return grains is null ? [] : grains;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<string>> ToKeyListAsync(CancellationToken cancellationToken = default)
    {
        List<string>? keys = null;
        await foreach (var match in _executor
            .ExecuteAsync(_plan, PageSize, Execution, payloads: false, cancellationToken)
            .ConfigureAwait(false))
        {
            keys ??= [];
            keys.Add(match.GrainKey);
        }

        return keys is null ? [] : keys;
    }

    /// <inheritdoc />
    public async Task<bool> AnyAsync(CancellationToken cancellationToken = default)
    {
        await foreach (var _ in _executor
            .ExecuteAsync(_plan, PageSize, Execution, payloads: false, cancellationToken)
            .ConfigureAwait(false))
        {
            // Abandoning the enumeration here disposes the iterator, which closes
            // any cursor the scan opened.
            return true;
        }

        return false;
    }
}
