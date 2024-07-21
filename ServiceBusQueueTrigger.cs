using Azure.Messaging.ServiceBus;
using Dapper;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace EventManagementFunctions.Function
{
    public class ServiceBusQueueTrigger
    {
        private readonly ILogger<ServiceBusQueueTrigger> _logger;
        private readonly string _connectionString;

        public ServiceBusQueueTrigger(ILogger<ServiceBusQueueTrigger> logger, IConfiguration configuration)
        {
            _logger = logger;
            _connectionString = configuration["DatabaseConnectionString"];

        }

        [Function("ServiceBusQueueTrigger")]
        public async Task Run([ServiceBusTrigger("eventappqueue", Connection = "ServiceBus")] ServiceBusReceivedMessage message, ServiceBusMessageActions messageActions)
        {
            _logger.LogInformation($"#Message ID: {message.MessageId}, " +
                                               $"Message Body: {message.Body}, " +
                                               $"Message Content-Type: {message.ContentType}, " +
                                               $"Enqueued Time: {message.EnqueuedTime}, " +
                                               $"Sequence Number: {message.SequenceNumber}");

            var registrationDto = System.Text.Json.JsonSerializer.Deserialize<EventRegistrationDto>(message.Body.ToString());

            try
            {
                if (registrationDto.Action == "Register")
                {
                    await RegisterEventAsync(registrationDto.EventId, registrationDto.UserId);
                    _logger.LogInformation($"User {registrationDto.UserId} registered for event {registrationDto.EventId} successfully");
                }
                else if (registrationDto.Action == "Unregister")
                {
                    await UnregisterEventAsync(registrationDto.EventId, registrationDto.UserId);
                    _logger.LogInformation($"User {registrationDto.UserId} unregistered for event {registrationDto.EventId} successfully");
                }

                await messageActions.CompleteMessageAsync(message);
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error processing message: {ex.Message}");
            }
        }

        private async Task RegisterEventAsync(string eventId, string userId)
        {
            using (var connection = new NpgsqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        var eventGuid = Guid.Parse(eventId);
                        var eventQuery = "SELECT * FROM \"Events\" WHERE \"Id\" = @EventId FOR UPDATE";
                        var eventDetails = await connection.QuerySingleOrDefaultAsync(eventQuery, new { EventId = eventGuid }, transaction);

                        if (eventDetails == null)
                        {
                            throw new Exception("Event not found.");
                        }

                        if (eventDetails.RegisteredCount >= eventDetails.TotalSpots)
                        {
                            throw new Exception("No available spots.");
                        }

                        var parameters = new { EventId = eventGuid, UserId = userId };
                        var insertRegistrationQuery = "INSERT INTO \"EventRegistrations\" (\"EventId\", \"UserId\") VALUES (@EventId, @UserId)";

                        await connection.ExecuteAsync(insertRegistrationQuery, parameters, transaction);

                        var updateEventQuery = "UPDATE \"Events\" SET \"RegisteredCount\" = \"RegisteredCount\" + 1 WHERE \"Id\" = @EventId";
                        await connection.ExecuteAsync(updateEventQuery, new { EventId = eventGuid }, transaction);
                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        _logger.LogError($"Error during registration: {ex.Message}");
                        throw;
                    }
                }
            }
        }

        private async Task UnregisterEventAsync(string eventId, string userId)
        {
            using (var connection = new NpgsqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        var deleteRegistrationQuery = "DELETE FROM \"EventRegistrations\" WHERE \"EventId\" = @EventId AND \"UserId\" = @UserId";
                        var result = await connection.ExecuteAsync(deleteRegistrationQuery, new { EventId = eventId, UserId = userId }, transaction);

                        var updateEventQuery = "UPDATE \"Events\" SET \"RegisteredCount\" = \"RegisteredCount\" - 1 WHERE \"Id\" = @EventId AND \"RegisteredCount\" > 0;";
                        await connection.ExecuteAsync(updateEventQuery, new { EventId = Guid.Parse(eventId) }, transaction);

                        _logger.LogInformation($"Successfully deleted registration for User {userId} from Event {eventId}. Event count decremented.");


                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        _logger.LogError($"Error during unregistration: {ex.Message}");
                        throw;
                    }
                }
            }
        }
    }

    public class EventRegistrationDto
    {
        public string EventId { get; set; }
        public string UserId { get; set; }
        public string Action { get; set; }
    }
}