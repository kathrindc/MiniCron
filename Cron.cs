using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using System.Reflection;

namespace MiniCron
{
    public class Cron
    {
        public const int Any = -1;

        private record CronValue(bool Every = false, int Value = Any)
        {
            public bool Match(int value)
            {
                return (Value == Any) || (Every ? (value % Value == 0) : (value == Value));
            }

            public override string ToString()
            {
                return (Value == Any) ? "*" : (Every ? $"*/{Value}" : $"{Value}");
            }

            public static CronValue Parse(string text)
            {
                text = text.Trim();

                if (text == "*")
                {
                    return new();
                }

                bool every = false;
                int value = Any;

                if (text.StartsWith("*/"))
                {
                    text = text[2..];
                    every = true;
                }

                value = int.Parse(text);

                return new(every, value);
            }
        }

        public abstract class Job
        {
            private static int _nextId = 0;
            
            private readonly int _id;
            private readonly long _version;
            private readonly string _name;
            private readonly Cron _cron;
            private DateTime? _lastOkExec;
            private string _lastError;
            private int _lastReturn;

            protected Job(String name, long version, Cron cron)
            {
                _id = ++_nextId;
                _version = version;
                _name = name;
                _cron = cron;
                _lastOkExec = null;
                _lastError = null;
                _lastReturn = -1;
            }

            public int Id => _id;
            public long Version => _version;
            public string Name => _name;
            public DateTime? LastSuccessful => _lastOkExec;
            public string LastError => _lastError;
            public int LastReturn => _lastReturn;

            public bool IsActive => _cron.IsActive;

            public async Task Run(CancellationToken stoppingToken)
            {
                try
                {
                    _lastReturn = await InternalRun(stoppingToken);
                    _lastOkExec = DateTime.Now;
                }
                catch (Exception ex)
                {
                    _lastError = ex.Message;
                    _lastReturn = -1;
                }
            }

            public override bool Equals(object obj)
            {
                if (obj is not Job)
                {
                    return false;
                }

                return ((Job)obj).Id == _id;
            }

            public override int GetHashCode()
            {
                return base.GetHashCode();
            }

            public JobInfo GetInfo()
            {
                return new(
                    _id,
                    _name,
                    _version,
                    _cron.ToUnixString(),
                    _lastOkExec,
                    _lastError,
                    _lastReturn
                );
            }

            public abstract void Initialise(IServiceProvider services);
            protected abstract Task<int> InternalRun(CancellationToken stoppingToken);
        }

        public record JobInfo(int Id, string Name, long Version, string Cron, DateTime? LastSuccessful, string LastError, int LastReturn);

        public class SchedulerService : BackgroundService
        {
            public record TickInfo(DateTime When, double Ms);

            private readonly TimeSpan _timerTick;
            private readonly IServiceProvider _services;
            private readonly List<Job> _jobs;
            private bool _autoScanned;

            public bool Running { get; private set; }
            public DateTime LastTick { get; private set; }
            public Queue<TickInfo> History { get; private set; }

            public SchedulerService(IServiceProvider services)
            {
                _timerTick = TimeSpan.FromSeconds(5);
                _services = services;
                _jobs = new();
                _autoScanned = false;

                Running = false;
                LastTick = DateTime.MinValue;
                History = new();
            }

            public void Register<T>() where T : Job, new()
            {
                var scope = _services.CreateScope();
                var job = new T();

                job.Initialise(scope.ServiceProvider);
                _jobs.Add(job);
            }

            public List<JobInfo> GetJobs()
            {
                return _jobs.Select(j => j.GetInfo()).ToList();
            }

            public void ScanAutoRegister(Assembly assembly)
            {
                if (_autoScanned)
                {
                    return;
                }

                _autoScanned = true;

                foreach (var type in assembly.GetTypes())
                {
                    if (type.IsSubclassOf(typeof(Job)) && type.GetCustomAttributes<AutoRegisterAttribute>(false).Any())
                    {
                        GetType()
                            .GetMethod("Register")
                            .MakeGenericMethod(new[] { type })
                            .Invoke(this, Array.Empty<object>());
                    }
                }
            }

            protected override async Task ExecuteAsync(CancellationToken stoppingToken)
            {
                using PeriodicTimer timer = new(_timerTick);
                using var scope = _services.CreateScope();
                var activeTasks = new List<Task>();

                Running = true;

                while (!stoppingToken.IsCancellationRequested && await timer.WaitForNextTickAsync(stoppingToken))
                {
                    var tickTime = DateTime.Now;
                    LastTick = tickTime;

                    foreach (var job in _jobs)
                    {
                        if (job.IsActive) activeTasks.Add(job.Run(stoppingToken));
                    }

                    if (activeTasks.Any())
                    {
                        await Task.WhenAll(activeTasks);

                        activeTasks.Clear();
                    }

                    var ms = (DateTime.Now - tickTime).TotalMilliseconds;
                    var info = new TickInfo(tickTime, ms);

                    History.Enqueue(info);

                    while (History.Count > 1000)
                    {
                        History.Dequeue();
                    }
                }

                Running = false;
            }
        }

        [System.AttributeUsage(System.AttributeTargets.Class)]
        public class AutoRegisterAttribute : System.Attribute
        {
            public AutoRegisterAttribute() { }
        }

        private readonly CronValue _minute;
        private readonly CronValue _hour;
        private readonly CronValue _dayMonth;
        private readonly CronValue _month;
        private readonly CronValue _dayWeek;
        private bool _tick = true;

        public bool IsActive
        {
            get
            {
                var now = DateTime.Now;
                var active = _minute.Match(now.Minute)
                    && _hour.Match(now.Hour)
                    && _dayMonth.Match(now.Day)
                    && _month.Match(now.Month)
                    && _dayWeek.Match(((int)now.DayOfWeek));
                var result = _tick && active;

                if (active)
                {
                    _tick = false;
                }
                else if (!_tick)
                {
                    _tick = true;
                }

                return result;
            }
        }

        public Cron(string text)
        {
            var parts = text
                .Trim()
                .Split(" ")
                .Where(p => !string.IsNullOrWhiteSpace(p))
                .ToList();

            if (parts.Count != 5)
            {
                throw new FormatException("Invalid unix-style cron string");
            }

            _minute = CronValue.Parse(parts[0]);
            _hour = CronValue.Parse(parts[1]);
            _dayMonth = CronValue.Parse(parts[2]);
            _month = CronValue.Parse(parts[3]);
            _dayWeek = CronValue.Parse(parts[4]);
        }

        public override string ToString()
        {
            return $"Cron[{ToUnixString()}]";
        }

        public string ToUnixString()
        {
            return $"{_minute} {_hour} {_dayMonth} {_month} {_dayWeek}";
        }
    }

    public static class CronServiceExtension
    {
        public static void AddCronScheduler<StartupType>(this IServiceCollection services)
        {
            services.AddSingleton<Cron.SchedulerService>();
            services.AddHostedService(provider =>
            {
                var scheduler = provider.GetRequiredService<Cron.SchedulerService>();
                var startupType = typeof(StartupType);

                scheduler.ScanAutoRegister(startupType.Assembly);

                return scheduler;
            });
        }
    }
}
