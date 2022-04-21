using Dapper;
using Hangfire;
using Serilog;
using System;
using System.Data.SqlClient;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace HangfireJobHandler
{
    [Queue("handler")]
    public class JobHandler : IJobHandler
    {
        private readonly ILogger _logger;

        public JobHandler(ILogger logger)
        {
            _logger = logger;
        }
        public async Task<bool> TryEnqueueJobAsync(string jobId, Expression<Func<Task>> expression)
        {
            string query = $@"SELECT COUNT(1) 
FROM [{Environment.GetEnvironmentVariable("HANGFIRE_SCHEMA")}].[{Environment.GetEnvironmentVariable("HANGFIRE_JOB_TABLE")}]
WHERE JobId = '{jobId}'";
            using (var connection = new SqlConnection($"{Environment.GetEnvironmentVariable("SQL_CONNECTIONSTRING")};database={Environment.GetEnvironmentVariable("HANGFIRE_DATABASE")};")) 
            {
                int count = await connection.ExecuteScalarAsync<int>(query);
                if(count == 0)
                {
                    string result = BackgroundJob.Enqueue(expression);
                    string command = $@"INSERT INTO [{Environment.GetEnvironmentVariable("HANGFIRE_SCHEMA")}].[{Environment.GetEnvironmentVariable("HANGFIRE_JOB_TABLE")}] (JobId, JobRef) 
VALUES ('{jobId}', '{result}')";
                    BackgroundJob.ContinueJobWith(result, () => DeleteJobFromQueueAsync(jobId), JobContinuationOptions.OnAnyFinishedState);
                    await connection.ExecuteAsync(command, commandTimeout: 60);
                    return true;
                }
            }
            _logger.Information($"Skipping duplicate job: {jobId}.");
            return false;
        }

        public async Task DeleteJobFromQueueAsync(string jobId)
        {
            string command = $@"DELETE FROM [{Environment.GetEnvironmentVariable("HANGFIRE_SCHEMA")}].[{Environment.GetEnvironmentVariable("HANGFIRE_JOB_TABLE")}] 
WHERE JobId = '{jobId}'";
            using (var connection = new SqlConnection($"{Environment.GetEnvironmentVariable("SQL_CONNECTIONSTRING")};database={Environment.GetEnvironmentVariable("HANGFIRE_DATABASE")};"))
            {
                await connection.ExecuteAsync(command, commandTimeout: 60);
            }
        }

        public void CleanupHangingTasks()
        {
            string command = $@"DELETE FROM [{Environment.GetEnvironmentVariable("HANGFIRE_SCHEMA")}].[{Environment.GetEnvironmentVariable("HANGFIRE_JOB_TABLE")}] 
WHERE ID IN (SELECT pj.[Id] FROM [{Environment.GetEnvironmentVariable("HANGFIRE_SCHEMA")}].[{Environment.GetEnvironmentVariable("HANGFIRE_JOB_TABLE")}] pj 
LEFT JOIN [{Environment.GetEnvironmentVariable("HANGFIRE_SCHEMA")}].[Job] j on j.id = JobRef 
WHERE j.StateId is null)";
            using (var connection = new SqlConnection($"{Environment.GetEnvironmentVariable("SQL_CONNECTIONSTRING")};database={Environment.GetEnvironmentVariable("HANGFIRE_DATABASE")};"))
            {
                connection.Execute(command, commandTimeout: 60);
            }
        }
    }
}
