using System.Collections.Concurrent;
using System.Net;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Operations;
using Orleans.Runtime;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// A real <see cref="LatticeOperationRunner"/> over in-memory operation grains, so a
/// facade's accept-then-poll verbs can be driven without a cluster: progress
/// reports and the terminal outcome are folded into each operation's record exactly
/// as the durable grain folds them.
/// </summary>
internal sealed class InMemoryOperationRunner
{
    private readonly ConcurrentDictionary<string, Grain> _grains = new(StringComparer.Ordinal);
    private readonly List<string> _index = [];

    public InMemoryOperationRunner()
    {
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeOperationGrain>(Arg.Any<string>(), null)
            .Returns(call => _grains.GetOrAdd(call.ArgAt<string>(0), key => new Grain(key, this)));
        var index = Substitute.For<ILatticeOperationIndexGrain>();
        index.ListAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<int>())
            .Returns(call =>
            {
                var prefix = call.ArgAt<string?>(0);
                List<string> ids;
                lock (_index)
                {
                    ids = _index
                        .Where(key => prefix is null || _grains[key].Record!.Kind.StartsWith(prefix, StringComparison.Ordinal))
                        .Select(key => LatticeOperationKey.Parse(key).OperationId)
                        .Reverse()
                        .ToList();
                }

                return Task.FromResult(new LatticeOperationIndexPage(ids, null));
            });
        factory.GetGrain<ILatticeOperationIndexGrain>(Arg.Any<string>(), null).Returns(index);

        var silo = Substitute.For<ILocalSiloDetails>();
        silo.SiloAddress.Returns(SiloAddress.New(new IPEndPoint(IPAddress.Loopback, 22222), 1));
        Runner = new LatticeOperationRunner(
            factory,
            silo,
            Options.Create(new LatticeOperationOptions()),
            NullLogger<LatticeOperationRunner>.Instance);
    }

    public LatticeOperationRunner Runner { get; }

    public LatticeOperationRecord? Record(string tenantId, string operationId) =>
        _grains.TryGetValue(LatticeOperationKey.For(tenantId, operationId), out var grain) ? grain.Record : null;

    public IReadOnlyList<LatticeOperationProgressReport> Reports(string tenantId, string operationId) =>
        _grains.TryGetValue(LatticeOperationKey.For(tenantId, operationId), out var grain) ? grain.Reports : [];

    private sealed class Grain(string key, InMemoryOperationRunner owner) : ILatticeOperationGrain
    {
        private readonly object _sync = new();

        public LatticeOperationRecord? Record { get; private set; }

        public List<LatticeOperationProgressReport> Reports { get; } = [];

        public Task<LatticeOperationBeginResult> BeginAsync(LatticeOperationBeginRequest request)
        {
            lock (_sync)
            {
                if (Record is { } existing)
                {
                    if (!string.Equals(existing.Kind, request.Kind, StringComparison.Ordinal))
                    {
                        throw new InvalidOperationException("The id is in use by an operation of a different kind.");
                    }

                    return Task.FromResult(new LatticeOperationBeginResult(false, existing));
                }

                var (tenant, id) = LatticeOperationKey.Parse(key);
                Record = new LatticeOperationRecord
                {
                    OperationId = id,
                    Kind = request.Kind,
                    TenantId = tenant,
                    TreeIds = request.TreeIds,
                    State = LatticeOperationState.Queued,
                    Phase = LatticeOperationPhaseNames.Queued,
                    Phases = request.Phases,
                    PhaseCount = request.Phases.Count == 0 ? null : request.Phases.Count,
                    Attributes = request.Attributes,
                    StartedAtUtc = DateTimeOffset.UtcNow,
                };
                lock (owner._index)
                {
                    owner._index.Add(key);
                }

                return Task.FromResult(new LatticeOperationBeginResult(true, Record));
            }
        }

        public Task<bool> ReportAsync(LatticeOperationProgressReport report)
        {
            lock (_sync)
            {
                Reports.Add(report);
                var index = Record!.Phases.ToList().IndexOf(report.Phase);
                Record = Record with
                {
                    State = LatticeOperationState.Running,
                    Phase = report.Phase,
                    PhaseIndex = index < 0 ? null : index,
                    CompletedUnits = report.CompletedUnits,
                    TotalUnits = report.TotalUnits,
                    UnitName = report.UnitName,
                };
                return Task.FromResult(Record.CancelRequested);
            }
        }

        public Task<bool> HeartbeatAsync()
        {
            lock (_sync)
            {
                return Task.FromResult(Record?.CancelRequested ?? false);
            }
        }

        public Task<LatticeOperationRecord?> CompleteAsync(LatticeOperationCompletion completion)
        {
            lock (_sync)
            {
                Record = Record! with
                {
                    State = completion.State,
                    FinishedAtUtc = DateTimeOffset.UtcNow,
                    FailureReason = completion.FailureReason,
                    ResultReference = completion.ResultReference,
                    Result = completion.Result,
                };
                return Task.FromResult<LatticeOperationRecord?>(Record);
            }
        }

        public Task<LatticeOperationRecord?> GetAsync()
        {
            lock (_sync)
            {
                return Task.FromResult(Record);
            }
        }

        public Task<LatticeOperationRecord?> RequestCancelAsync()
        {
            lock (_sync)
            {
                if (Record is { IsTerminal: false })
                {
                    Record = Record with { CancelRequested = true };
                }

                return Task.FromResult(Record);
            }
        }
    }
}
