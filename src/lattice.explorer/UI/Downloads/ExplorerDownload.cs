namespace Orleans.Lattice.Explorer.UI.Downloads;

/// <summary>
/// One file the Explorer streams to a browser through the download endpoint: its
/// name, its content type, and how to write its body and its trailer. A circuit
/// builds one, mints a ticket for it with <see cref="ExplorerDownloads.Mint"/>, and
/// the endpoint streams it when the browser presents that ticket.
/// </summary>
/// <remarks>
/// <para>
/// <b>Streamed, never buffered.</b> The body is written to the response as it is
/// produced; an implementation holds at most a bounded window (one page) of it.
/// </para>
/// <para>
/// <b>Always a trailer.</b> <see cref="RunAsync"/> writes the trailer whatever happens
/// to the body - a fault or a cancellation becomes a trailer that says the file is not
/// complete and why - so a file is never a silent partial.
/// </para>
/// <para>
/// <b>Holds no services.</b> A download outlives the circuit call that built it and is
/// run on a request outside the circuit, so it captures values (a connection snapshot,
/// a scope), never the circuit's scoped services.
/// </para>
/// </remarks>
internal abstract class ExplorerDownload
{
    /// <summary>The file name the browser saves the download as.</summary>
    public abstract string FileName { get; }

    /// <summary>The response content type, such as <c>application/x-ndjson</c>.</summary>
    public abstract string ContentType { get; }

    /// <summary>The trailer's <see cref="ExplorerDownloadTrailer.Format"/> for this kind of file.</summary>
    protected abstract string Format { get; }

    /// <summary>
    /// Streams the body, then the trailer, to <paramref name="body"/>, reporting progress
    /// to <paramref name="ticket"/>, and finishes the ticket with the trailer it wrote.
    /// </summary>
    /// <param name="body">The response body.</param>
    /// <param name="ticket">The ticket being redeemed, which carries progress back to its circuit.</param>
    /// <param name="cancellationToken">Stops the download: the browser went away or the circuit withdrew it.</param>
    /// <returns>The trailer the file ends with.</returns>
    public async Task<ExplorerDownloadTrailer> RunAsync(Stream body, ExplorerDownloadTicket ticket, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(body);
        ArgumentNullException.ThrowIfNull(ticket);

        ExplorerDownloadTrailer trailer;
        try
        {
            trailer = await WriteBodyAsync(body, ticket, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            trailer = ExplorerDownloadTrailer.Stopped(Format, ticket.Items, "The download was stopped before it finished.");
        }
        catch (Exception exception)
        {
            trailer = ExplorerDownloadTrailer.Stopped(Format, ticket.Items, DescribeFailure(exception));
        }

        try
        {
            // Written whatever stopped the body, so a reader can tell; when the browser
            // itself went away there is no one to read it and the write simply fails.
            await WriteTrailerAsync(body, trailer, CancellationToken.None).ConfigureAwait(false);
            await body.FlushAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Nobody is left to read it: the browser went away mid-download.
        }

        ticket.Finish(trailer);
        return trailer;
    }

    /// <summary>
    /// Writes the body and returns the trailer that describes it. Report progress with
    /// <see cref="ExplorerDownloadTicket.Report"/> as items are written.
    /// </summary>
    /// <param name="body">The response body.</param>
    /// <param name="ticket">The ticket, for progress.</param>
    /// <param name="cancellationToken">Stops the body.</param>
    /// <returns>The trailer for a body that ran to its end.</returns>
    protected abstract Task<ExplorerDownloadTrailer> WriteBodyAsync(Stream body, ExplorerDownloadTicket ticket, CancellationToken cancellationToken);

    /// <summary>Writes <paramref name="trailer"/> in this download's format.</summary>
    /// <param name="body">The response body.</param>
    /// <param name="trailer">The trailer.</param>
    /// <param name="cancellationToken">Stops the write.</param>
    /// <returns>A task that completes when the trailer is written.</returns>
    protected abstract ValueTask WriteTrailerAsync(Stream body, ExplorerDownloadTrailer trailer, CancellationToken cancellationToken);

    /// <summary>
    /// The plain reason the trailer gives when the body fails with <paramref name="exception"/>.
    /// It is written into a file a user keeps, so it never carries the exception's own text.
    /// </summary>
    /// <param name="exception">The failure.</param>
    /// <returns>The reason.</returns>
    protected virtual string DescribeFailure(Exception exception) =>
        "The download stopped because the cluster could not be read.";
}
