using Microsoft.AspNetCore.Antiforgery;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Routing;
using Microsoft.Net.Http.Headers;
using Orleans.Lattice.Explorer.UI.Downloads;

namespace Orleans.Lattice.Explorer.Web;

/// <summary>
/// Maps the download endpoint (issue #5366): the one place a file the Explorer
/// produces - a data export, an import's rejection list - is streamed to the browser.
/// </summary>
/// <remarks>
/// <para>
/// The circuit mints a ticket and renders a form that posts its token and the
/// circuit's binding here, with the antiforgery token. The endpoint redeems the ticket
/// - single use, short-lived, bound to the circuit - and streams the ticket's download
/// straight into the response body as it is produced, with buffering off, so nothing
/// beyond the download's own bounded window is held.
/// </para>
/// <para>
/// <b>Fails closed and says nothing.</b> A missing, unknown, reused, expired or
/// wrongly bound token is answered with the same empty <c>204 No Content</c>: nothing
/// is served, the endpoint is no oracle for which tokens exist, and the browser, which
/// does not navigate on a <c>204</c>, keeps the console open. The minting circuit learns
/// the outcome from its own ticket. A post without a valid antiforgery token is a
/// <c>400</c>, as for sign-in.
/// </para>
/// <para>
/// The endpoint carries no credential of its own and resolves no circuit service: the
/// download runs over the connection snapshot its circuit captured, as that user.
/// </para>
/// </remarks>
internal static class ExplorerDownloadEndpoints
{
    /// <summary>The endpoint's route, relative to the Explorer's mount.</summary>
    public const string Route = "/" + ExplorerDownloads.FormAction;

    /// <summary>Maps the download endpoint.</summary>
    /// <param name="endpoints">The endpoint route builder.</param>
    /// <returns>The same builder, for chaining.</returns>
    public static IEndpointRouteBuilder MapExplorerDownloads(this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        endpoints.MapPost(Route, HandleAsync);
        return endpoints;
    }

    /// <summary>Redeems the posted ticket and streams its download.</summary>
    /// <param name="context">The request.</param>
    /// <param name="store">The ticket store.</param>
    /// <param name="antiforgery">Antiforgery validation.</param>
    /// <returns>The result: empty once the download has been streamed.</returns>
    internal static async Task<IResult> HandleAsync(HttpContext context, ExplorerDownloadTicketStore store, IAntiforgery antiforgery)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(antiforgery);

        if (!context.Request.HasFormContentType)
        {
            return Results.NoContent();
        }

        try
        {
            await antiforgery.ValidateRequestAsync(context);
        }
        catch (AntiforgeryValidationException)
        {
            return Results.BadRequest();
        }

        var form = await context.Request.ReadFormAsync(context.RequestAborted);
        var outcome = store.Redeem(
            form[ExplorerDownloads.TokenField].ToString(),
            form[ExplorerDownloads.BindingField].ToString(),
            out var ticket);
        if (outcome != ExplorerDownloadRedemption.Redeemed || ticket is null)
        {
            return Results.NoContent();
        }

        var download = ticket.Download;
        var response = context.Response;
        response.ContentType = download.ContentType;
        var disposition = new ContentDispositionHeaderValue("attachment");
        disposition.SetHttpFileName(download.FileName);
        response.Headers.ContentDisposition = disposition.ToString();
        response.Headers.CacheControl = "no-store";
        context.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();

        using var stop = CancellationTokenSource.CreateLinkedTokenSource(context.RequestAborted, ticket.Withdrawn);
        await download.RunAsync(response.Body, ticket, stop.Token);
        return Results.Empty;
    }
}
