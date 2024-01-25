import throng from "throng";
import Queue from "bull";
import BingAIClient from "../src/BingAIClient.js";
import fs from "fs";
import { pathToFileURL } from "url";
import { KeyvFile } from "keyv-file";

const arg = process.argv.find((_arg) => _arg.startsWith("--settings"));
const path = arg?.split("=")[1] ?? "./settings.js";

let settings;
if (fs.existsSync(path)) {
  // get the full path
  const fullPath = fs.realpathSync(path);
  settings = (await import(pathToFileURL(fullPath).toString())).default;
} else {
  if (arg) {
    console.error(
      "Error: the file specified by the --settings parameter does not exist."
    );
  } else {
    console.error("Error: the settings.js file does not exist.");
  }
  process.exit(1);
}

if (settings.storageFilePath && !settings.cacheOptions.store) {
  // make the directory and file if they don't exist
  const dir = settings.storageFilePath.split("/").slice(0, -1).join("/");
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
  if (!fs.existsSync(settings.storageFilePath)) {
    fs.writeFileSync(settings.storageFilePath, "");
  }

  settings.cacheOptions.store = new KeyvFile({
    filename: settings.storageFilePath,
  });
}

// Connect to a local redis instance locally, and the Heroku-provided URL in production
let REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379";

// Spin up multiple processes to handle jobs to take advantage of more CPU cores
// See: https://devcenter.heroku.com/articles/node-concurrency for more info
let workers = process.env.WEB_CONCURRENCY || 2;

// The maximum number of jobs each worker should process at once. This will need
// to be tuned for your application. If each job is mostly waiting on network
// responses it can be much higher. If each job is CPU-intensive, it might need
// to be much lower.
let maxJobsPerWorker = 50;

function start() {
  console.log("Starting worker...");
  console.log(`redis url: ${REDIS_URL}`);
  // Connect to the named work queue
  let workQueue = new Queue("work", REDIS_URL);

  workQueue.process(maxJobsPerWorker, async (job) => {
    console.log("processing job");
    // Access the body argument
    const body = job.data.body;

    const clientToUse = "bing";
    let clientToUseForMessage = clientToUse;

    let result;
    let error;

    try {
      const clientOptions = filterClientOptions(
        body.clientOptions,
        clientToUseForMessage
      );

      if (clientOptions && clientOptions.clientToUse) {
        clientToUseForMessage = clientOptions.clientToUse;
        delete clientOptions.clientToUse;
      }

      const messageClient = new BingAIClient({
        ...settings.bingAiClient,
        cache: settings.cacheOptions,
      });

      console.log(`processing: ${JSON.stringify(body)}`);

      result = await messageClient.sendMessage(body.message, {
        jailbreakConversationId: body.jailbreakConversationId,
        conversationId: body.conversationId
          ? body.conversationId.toString()
          : undefined,
        parentMessageId: body.parentMessageId
          ? body.parentMessageId.toString()
          : undefined,
        systemMessage: body.systemMessage,
        context: body.context,
        conversationSignature: body.conversationSignature,
        clientId: body.clientId,
        invocationId: body.invocationId,
        toneStyle: body.toneStyle,
        clientOptions,
      });
    } catch (e) {
      error = e;
    }

    // A job can return values that will be stored in Redis as JSON
    // This return value is unused in this demo application.

    if (result !== undefined) {
      return result;
    }

    const code =
      error?.data?.code || (error.name === "UnauthorizedRequest" ? 401 : 503);
    if (code === 503) {
      console.error(error);
    } else if (settings.apiOptions?.debug) {
      console.debug(error);
    }
    const message =
      error?.data?.message ||
      error?.message ||
      `There was an error communicating with ${
        clientToUse === "bing" ? "Bing" : "ChatGPT"
      }.`;

    return {
      error: message,
    };
  });
}

// Initialize the clustered worker process
// See: https://devcenter.heroku.com/articles/node-concurrency for more info
throng({ workers, start });

/**
 * Filter objects to only include whitelisted properties set in
 * `settings.js` > `apiOptions.perMessageClientOptionsWhitelist`.
 * Returns original object if no whitelist is set.
 * @param {*} inputOptions
 * @param clientToUseForMessage
 */
function filterClientOptions(inputOptions, clientToUseForMessage) {
  if (!inputOptions) {
    return null;
  }

  inputOptions.clientToUse = clientToUseForMessage;

  // No whitelist, return all options
  return inputOptions;
}
