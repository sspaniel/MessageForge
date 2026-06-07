namespace MessageForge.Errors;

/// <summary>
/// Error that occurred during message delivery and processing.
/// </summary>
public class MessageForgeError
{
    /// <summary>
    /// Initializes a new instance of the <see cref="MessageForgeError"/> class.
    /// </summary>
    public MessageForgeError()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="MessageForgeError"/> class.
    /// </summary>
    /// <param name="consumerName">Destination consumer that encountered the exception.</param>
    /// <param name="exception">Exception thrown by the consumer.</param>
    public MessageForgeError(string consumerName, Exception exception)
    {
        ConsumerName = consumerName;
        Message = exception.Message;
        StackTrace = exception.StackTrace ?? string.Empty;
        InnerError = exception.InnerException != null ? new MessageForgeError(consumerName, exception.InnerException) : null;
    }

    /// <summary>
    /// Gets or sets the name of the consumer that encountered the exception.
    /// </summary>
    public string ConsumerName { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets message of the exception.
    /// </summary>
    public string Message { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets stack trace of the exception.
    /// </summary>
    public string StackTrace { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets inner exception.
    /// </summary>
    public MessageForgeError? InnerError { get; set; }
}
