using System.Runtime.Serialization.Json;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using AssemblySoft.DevOps;
using AssemblySoft.Serialization;
using log4net;
using AssemblySoft.IO;

namespace AssemblySoft.WonkaAgent
{
    class Program
    {
        private static readonly HttpClient client = new HttpClient();
        static ILog logger = LogManager.GetLogger("wonka-agent");

        static void Main(string[] args)
        {

            Log(string.Format("Wonka Agent version: {0}", "1.1.0"));

            var taskDefinitionModel = GetDefinition(ConfigurationManager.AppSettings["task"]).Result;
            var tasks = GetTasks(taskDefinitionModel).Result;


            string runPath = string.Empty;
            try
            {
                runPath = InitialiseAgent(taskDefinitionModel);

            }
            catch (Exception e)
            {
                HandleException(e);
            }

            var taskRunner = new TaskRunner(runPath);
            taskRunner.TaskStatus += (e) =>
            {
                Log(e.Status);
            };

            taskRunner.TasksCompleted += (e) =>
            {
                Log("task completed");


                if (e.Status == TaskStatus.Faulted.ToString())
                {
                    Log("task faulted");
                }
                else
                {
                    Log("task completed - finished");
                }
            };


            try
            {

                Task bootstrapTask;
                CancellationTokenSource _b_cts = new CancellationTokenSource();
                var bootstrapToken = _b_cts.Token;
                List<Task> bootstrapTasks = new List<Task>();
                bootstrapTask = new Task(() =>
                {
                    taskRunner.Run(bootstrapToken, tasks);
                });

                bootstrapTask.Start();
                Console.Write("waiting for bootstrap task runner...");
                bootstrapTask.Wait();
                Console.Write("finished waiting for bootstrap task runner...");

                Task t1;
                CancellationTokenSource _cts = new CancellationTokenSource();
                var token = _cts.Token;
                taskDefinitionModel.Path = runPath;
                var localtasks = LoadTasks(taskDefinitionModel);
                t1 = new Task(() =>
                {
                    taskRunner.Run(token, localtasks);

                });


                t1.Start();
                Console.Write("waiting for task runner...");
                t1.Wait();
                Console.Write("finished waiting for task runner...");

            }
            catch (DevOpsTaskException ex)
            {
                HandleException(ex);
            }
            catch (AggregateException ag)
            {
                HandleException(ag);
            }
            catch (Exception ex)
            {
                HandleException(ex);
            }
            finally
            {
                var taskState = taskRunner.GetDevOpsTaskWithState();
            }

            Log("Agent complete...");
        }


        public static IEnumerable<DevOpsTask> LoadTasks(TaskModel model)
        {
            var definitionPath = Path.Combine(model.Path, "tasks", string.Format("{0}.local.tasks", model.Task));

            if (!File.Exists(definitionPath))
            {
                throw new DevOpsTaskException(string.Format("Cannot find local task definition {0}", definitionPath));
            }

            var tasks = XmlSerialisationManager<DevOpsTask>.DeserializeObjects(definitionPath);
            if (tasks != null)
            {
                return tasks;
            }

            return null;
        }


        private static async Task<TaskModel> GetDefinition(string definitionName)
        {
            var serializer = new DataContractJsonSerializer(typeof(TaskModel));

            UriBuilder builder = new UriBuilder(ConfigurationManager.AppSettings["taskDefinitionBaseUri"])
            {
                Query = string.Format("name={0}&sourceStore={1}", definitionName, ConfigurationManager.AppSettings["sourceStore"])
            };

            var streamTask = client.GetStreamAsync(builder.Uri);
            var model = serializer.ReadObject(await streamTask) as TaskModel;

            return model;
        }

        private static async Task<IEnumerable<DevOpsTask>> GetTasks(TaskModel model)
        {

            var serializer = new DataContractJsonSerializer(typeof(IEnumerable<DevOpsTask>));

            UriBuilder builder = new UriBuilder(ConfigurationManager.AppSettings["tasksBaseUri"])
            {

                Query = string.Format("project={0}&fullname={1}", model.Project, model.FullName)
            };

            var streamTask = client.GetStreamAsync(builder.Uri);
            var tasks = serializer.ReadObject(await streamTask) as IEnumerable<DevOpsTask>;

            return tasks;
        }

        private static async Task GetTaskDependencies(TaskModel model, string runPath)
        {
            UriBuilder builder = new UriBuilder(ConfigurationManager.AppSettings["taskDependenciesBaseUri"])
            {

                Query = string.Format("source={0}&sourceStore={1}&id=1", model.FullName, ConfigurationManager.AppSettings["sourceStore"])
            };

            var streamTask = await client.GetStreamAsync(builder.Uri);
            var fileStream = File.Create(Path.Combine(runPath, "defs.zip"));
            streamTask.CopyTo(fileStream);
            fileStream.Close();
        }

        private static async Task EmitStatus(string msg)
        {
            UriBuilder builder = new UriBuilder(ConfigurationManager.AppSettings["taskStatusMessageBaseUri"]);
            builder.Query = string.Format("msg={0}", msg);
            await client.GetAsync(builder.Uri);
        }


        private static string InitialiseAgent(TaskModel model)
        {
            var tasksDestinationPath = Path.Combine(ConfigurationManager.AppSettings["tasksRunnerRootPath"], model.Project);

            //root path for the source task artifacts
            var tasksSourcePath = model.Path;

            //create new directory for tasks to run
            if (!Directory.Exists(tasksDestinationPath))
            {
                Directory.CreateDirectory(tasksDestinationPath);
            }

            int latestCount = GetNextBuildNumber(tasksDestinationPath);

            var runPath = Path.Combine(tasksDestinationPath, string.Format("{0}", latestCount));
            Directory.CreateDirectory(runPath);

            GetTaskDependencies(model, runPath).Wait();
            FileClient.UnzipFileToDirectory(Path.Combine(runPath, ConfigurationManager.AppSettings["targetZipFileName"]), runPath);
            return runPath;
        }

        private static void Log(string message, bool emitRemote = true)
        {
            logger.Info(message);

            if (emitRemote)
            {
                EmitStatus(message).Wait();
            }
        }

        private static int GetNextBuildNumber(string rootPath)
        {
            DirectoryInfo info = new DirectoryInfo(rootPath);
            var directories = info.GetDirectories();
            int latestCount = 0;
            foreach (var dir in directories)
            {
                int res;
                if (int.TryParse(dir.Name, out res))
                {
                    if (res > latestCount)
                    {
                        latestCount = res;
                    }
                }

            }
            latestCount++;
            return latestCount;
        }

        /// <summary>
        /// Handles exceptions
        /// </summary>
        /// <param name="e"></param>
        private static void HandleException(Exception e)
        {
            logger.Error(e.Message, e);

            if (e is DevOpsTaskException)
            {
                var devOpsEx = e as DevOpsTaskException;
                Log(string.Format("{0} failed with error: {1}", devOpsEx.Task != null ? devOpsEx.Task.Description : string.Empty, devOpsEx.Message));
            }
            else
            {
                Log(string.Format("Failed with error: {0}", e.Message));
            }
        }

    }

    public class TaskModel
    {
        public string Task { get; set; }
        public string FullName { get; set; }
        public string Project { get; set; }
        public string Path { get; set; }
        public string Version { get; set; }
        public string Description { get; set; }
        //public string Definition { get; set; }

        public override string ToString()
        {
            return Task + Environment.NewLine + FullName + Environment.NewLine + Project + Environment.NewLine + Path + Environment.NewLine + Description + Environment.NewLine + base.ToString();
        }
    }
}
