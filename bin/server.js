import express from "express";
import Queue from "bull";

// Serve on PORT on Heroku and on localhost:3000 locally
let PORT = process.env.PORT || "3000";
// Connect to a local redis intance locally, and the Heroku-provided URL in production
let REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379";

let app = express();
app.use(express.json());

// Create / Connect to a named work queue
let workQueue = new Queue("work", REDIS_URL);

// Kick off a new job by adding it to the work queue
app.post("/job", async (req, res) => {
  console.log(`redis url: ${REDIS_URL}`);
  console.log(`req: ${req}`);
  const body = req.body || {};
  console.log(`Received job with body ${JSON.stringify(body)}`);
  let error;
  try {
    if (!body.message) {
      const invalidError = new Error();
      invalidError.data = {
        code: 400,
        message: "The message parameter is required.",
      };
      // noinspection ExceptionCaughtLocallyJS
      throw invalidError;
    }

    console.log("adding job");
    // This would be where you could pass arguments to the job
    // Ex: workQueue.add({ url: 'https://www.heroku.com' })
    // Docs: https://github.com/OptimalBits/bull/blob/develop/REFERENCE.md#queueadd
    let job = await workQueue.add({
      body,
    });
    console.log(`added job: ${job.id}`);
    res.json({ id: job.id });
  } catch (e) {
    error = e;
    res
      .status(e.data?.code || 500)
      .json({ message: e.data?.message || e.message });
  }
});

// Allows the client to query the state of a background job
app.get("/job/:id", async (req, res) => {
  let id = req.params.id;
  let job = await workQueue.getJob(id);

  if (job === null) {
    res.status(404).end();
  } else {
    let state = await job.getState();
    let reason = job.failedReason;
    let result = job.returnvalue; // get the job's result
    res.json({ id, state, reason, result });
  }
});

// You can listen to global events to get notified when jobs are processed
workQueue.on("global:completed", (jobId, result) => {
  console.log(`Job completed with result ${result}`);
});

app.listen(
  {
    port: PORT,
    host: "0.0.0.0",
  },
  (error) => {
    if (error) {
      console.error(error);
      process.exit(1);
    }
  }
);
