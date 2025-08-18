using Dapper;
using Hangfire;
using Hangfire.States;
using Serilog;
using System;
using System.Data.SqlClient;
using System.Linq;
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
        public async Task<bool> TryEnqueueJobAsync(string jobId, Expression<Func<Task>> expression, int delay)
        {
            string query = $@"SELECT JobRef
FROM [{Environment.GetEnvironmentVariable("HANGFIRE_SCHEMA")}].[{Environment.GetEnvironmentVariable("HANGFIRE_JOB_TABLE")}]
WHERE JobId = '{jobId}'";
            using (var connection = new SqlConnection($"{Environment.GetEnvironmentVariable("SQL_CONNECTIONSTRING")};database={Environment.GetEnvironmentVariable("HANGFIRE_DATABASE")};")) 
            {
                string jobRef = await connection.ExecuteScalarAsync<string>(query);
                if(jobRef == null)
                {
                    string result = delay > 0 ? BackgroundJob.Schedule(expression, TimeSpan.FromMinutes(delay)) : BackgroundJob.Enqueue(expression);
                    string command = $@"INSERT INTO [{Environment.GetEnvironmentVariable("HANGFIRE_SCHEMA")}].[{Environment.GetEnvironmentVariable("HANGFIRE_JOB_TABLE")}] (JobId, JobRef) 
VALUES ('{jobId}', '{result}')";
                    BackgroundJob.ContinueJobWith(result, () => DeleteJobFromQueueAsync(jobId, result), JobContinuationOptions.OnAnyFinishedState);
                    await connection.ExecuteAsync(command, commandTimeout: 60);
                    return true;
                }
                else if (jobRef != null && delay > 0)
                {
                    _logger.Information($"Replacing scheduled job: {jobId}, ref: {jobRef}.");
                    BackgroundJob.Delete(jobRef);
                    string result = BackgroundJob.Schedule(expression, TimeSpan.FromMinutes(delay));
                    string command = $@"INSERT INTO [{Environment.GetEnvironmentVariable("HANGFIRE_SCHEMA")}].[{Environment.GetEnvironmentVariable("HANGFIRE_JOB_TABLE")}] (JobId, JobRef) 
VALUES ('{jobId}', '{result}')";
                    BackgroundJob.ContinueJobWith(result, () => DeleteJobFromQueueAsync(jobId, result), JobContinuationOptions.OnAnyFinishedState);
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

        public async Task DeleteJobFromQueueAsync(string jobId, string jobRef)
        {
            string command = $@"DELETE FROM [{Environment.GetEnvironmentVariable("HANGFIRE_SCHEMA")}].[{Environment.GetEnvironmentVariable("HANGFIRE_JOB_TABLE")}] 
WHERE JobId = '{jobId}' and JobRef = '{jobRef}'";
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

        public async Task<bool> EnqueueAwaitingJobs(int treshold)
        {
            string command = $@"SELECT j.Id FROM [{Environment.GetEnvironmentVariable("HANGFIRE_SCHEMA")}].[{Environment.GetEnvironmentVariable("HANGFIRE_JOB_TABLE")}] j
INNER JOIN [{Environment.GetEnvironmentVariable("HANGFIRE_SCHEMA")}].[State] s ON j.Id = s.JobId
WHERE s.Name = 'Awaiting'
AND s.CreatedAt < DATEADD(MINUTE, -@treshold, GETUTCDATE());";
            using (var connection = new SqlConnection($"{Environment.GetEnvironmentVariable("SQL_CONNECTIONSTRING")};database={Environment.GetEnvironmentVariable("HANGFIRE_DATABASE")};"))
            {
                var jobsResult = await connection.QueryAsync<long>(command, new { treshold }, commandTimeout: 60);
                var jobs = jobsResult.Select(x => x.ToString()).ToList();
                var client = new BackgroundJobClient();
                foreach (var jobId in jobs)
                {
                    client.ChangeState(jobId, new EnqueuedState(), "Awaiting");
                }
                return true;
            }
        }
    }
}
