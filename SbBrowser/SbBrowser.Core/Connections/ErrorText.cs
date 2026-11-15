using Azure;
using Azure.Messaging.ServiceBus;

namespace SbBrowser.Core.Connections;

/// <summary>Turns a failed operation's exception into one paragraph a user can act on, without SDK noise.</summary>
public static class ErrorText
{
    public static string Describe(Exception exception)
    {
        string message = exception switch
        {
            ServiceBusException { Reason: ServiceBusFailureReason.ServiceCommunicationProblem } sb =>
                $"The namespace cannot be reached. {sb.Message}",
            RequestFailedException { Status: 0 } rf => $"The namespace cannot be reached. {rf.Message}",
            RequestFailedException { Status: 401 or 403 } => "The namespace refused the credentials in the connection string.",
            _ => exception.Message,
        };

        // SDK messages append status codes, headers and a troubleshooting link after a blank line.
        int cut = message.IndexOf("\n\n", StringComparison.Ordinal);
        int crlf = message.IndexOf("\r\n\r\n", StringComparison.Ordinal);
        if (crlf >= 0 && (cut < 0 || crlf < cut)) cut = crlf;
        return (cut > 0 ? message[..cut] : message).Trim();
    }
}
