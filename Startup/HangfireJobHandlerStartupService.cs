using Hangfire;
using Hangfire.Common;
using Microsoft.Extensions.Hosting;
using System.Threading;
using System.Threading.Tasks;

namespace HangfireJobHandler.Startup
{
    public class HangfireJobHandlerStartupService : IHostedService
    {
        private readonly IJobHandler _handler;
        public HangfireJobHandlerStartupService(IJobHandler handler)
        {
            _handler = handler;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            var manager = new RecurringJobManager();
            manager.AddOrUpdate("CleanupHangingTasks", Job.FromExpression(() => _handler.CleanupHangingTasks()), "0 * * * *",
               new RecurringJobOptions { QueueName = "handler" });
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            var manager = new RecurringJobManager();
            manager.RemoveIfExists("CleanupHangingTasks");
            return Task.CompletedTask;
        }
    }
}
