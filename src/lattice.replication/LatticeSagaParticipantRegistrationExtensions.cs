using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Replication;

public static partial class LatticeReplicationServiceCollectionExtensions
{
    /// <summary>
    /// Enlists a host-defined <see cref="ISagaParticipant"/> so it runs in the
    /// <b>same</b> cross-cluster saga alongside the built-in restore participant.
    /// Every enlisted participant on every cluster is driven through one
    /// unanimous prepare, a single global commit-or-abort decision, and
    /// compensation on abort (a participant whose fence expires asks the
    /// coordinator for the decision and never compensates on the timer alone);
    /// see <see cref="ISagaParticipant"/> for the full
    /// contract.
    /// <para>
    /// Registration is idempotent per participant type <b>and per form</b>. Each
    /// enlistment is added via
    /// <see cref="ServiceCollectionDescriptorExtensions.TryAddEnumerable(IServiceCollection, ServiceDescriptor)"/>,
    /// which deduplicates on the enlisted implementation type, so repeated unnamed
    /// calls for the same <typeparamref name="TParticipant"/> enlist it once, and
    /// repeated named calls enlist its diagnostic wrapper once (the first call's name
    /// is kept; a later call's name is ignored). Mixing the two forms is <b>not</b>
    /// deduplicated: an unnamed and a named call for the same
    /// <typeparamref name="TParticipant"/> enlist two entries that both delegate to
    /// the one participant instance, so every saga drives that instance through
    /// each phase twice. Use a single form per participant type.
    /// <typeparamref name="TParticipant"/> is registered as a singleton and
    /// resolved from the container, so it may take constructor dependencies on
    /// other registered services.
    /// </para>
    /// <para>
    /// <b>Guardrails a custom participant must honour.</b> The implementation must
    /// be <b>idempotent</b> - a duplicate prepare, commit, or abort must be safe
    /// and must not double-apply or double-compensate - and its compensation must
    /// be <b>total</b>: once it votes <see cref="SagaVote.Commit"/> from
    /// <see cref="ISagaParticipant.PrepareAsync"/>, its
    /// <see cref="ISagaParticipant.AbortAsync"/> must always be able to undo that
    /// prepare, matching the intra-cluster cross-tree saga contract. A participant
    /// that cannot honour these guarantees must vote
    /// <see cref="SagaVote.Abort"/> instead of preparing.
    /// </para>
    /// </summary>
    /// <typeparam name="TParticipant">The participant implementation type.</typeparam>
    /// <param name="builder">The silo builder.</param>
    /// <param name="name">
    /// Optional operator-chosen name used for diagnostics and logging only. When
    /// supplied, the participant is wrapped in a lightweight diagnostic decorator
    /// that logs its prepare vote, commit, and abort under this name; the name
    /// never affects the saga wire contract or the drive model. When
    /// <see langword="null"/> the participant is enlisted directly.
    /// </param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    public static ISiloBuilder AddLatticeSagaParticipant<TParticipant>(
        this ISiloBuilder builder,
        string? name = null)
        where TParticipant : class, ISagaParticipant
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Validate an explicit name up front, before mutating the container.
        if (name is not null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(name);
        }

        // Register the concrete participant once so every enlisted view shares a
        // single instance and hosts can depend on it directly.
        builder.Services.TryAddSingleton<TParticipant>();

        if (name is null)
        {
            // Enlist the participant directly. TryAddEnumerable keys on the
            // (ISagaParticipant, TParticipant) pair, so repeated calls for the
            // same participant type add exactly one enumerable entry.
            builder.Services.TryAddEnumerable(
                ServiceDescriptor.Singleton<ISagaParticipant, TParticipant>(
                    static sp => sp.GetRequiredService<TParticipant>()));
            return builder;
        }

        // Enlist a named diagnostic wrapper. The closed generic
        // NamedSagaParticipant<TParticipant> is a distinct implementation type per
        // participant type, so TryAddEnumerable dedupes repeated named calls per
        // participant (keeping the first name) while the wrapper adds only
        // diagnostic logging. It is also distinct from TParticipant itself, so a
        // named call does NOT dedupe against an unnamed one for the same type:
        // mixing the two forms enlists the participant twice.
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<ISagaParticipant, NamedSagaParticipant<TParticipant>>(
                sp => new NamedSagaParticipant<TParticipant>(
                    name,
                    sp.GetRequiredService<TParticipant>(),
                    sp.GetRequiredService<ILogger<NamedSagaParticipant<TParticipant>>>())));
        return builder;
    }
}
