require("dotenv").config({ path: "./.env" });
console.log("ENV CHECK:");
console.log("EMAIL_USER:", process.env.EMAIL_USER);
console.log("EMAIL_PASS:", process.env.EMAIL_PASS);
const fs = require("fs");
const csv = require("csv-parser");
const multer = require("multer");
const upload = multer({ dest: "uploads/" });

const emailBody = `<div style="font-family: Georgia, serif; font-size: 14px; line-height: 1.4; color: #000;">

<p>Respected Sir/Madam,</p>
<div>Welcome to Task Queue System</div>
</div>`;

const express = require('express');
const app = express();
const logger = require("./logger");
app.use(express.json());
const sendEmail = require("./jobs/sendEmail");

let shuttingDown = false;
let emailJobsExecuted = 0;

setInterval(() => {
    emailJobsExecuted = 0;
}, 1000);

const mongoose = require("mongoose");
mongoose.connect("mongodb://127.0.0.1:27017/taskQueueDB")
    .then(() => console.log("MongoDB Connected"))
    .catch(err => console.log(err));

app.get("/", (req, res) => {
    res.send("Server running");
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`Server started on port ${PORT}`);
});

const taskSchema = new mongoose.Schema({
    type: String,
    payload: Object,
    status: {
        type: String,
        default: "pending"
    },
    retryCount: {
        type: Number,
        default: 0
    },
    priority: {
        type: Number,
        default: 0
    },
    runAt: {
        type: Date,
        default: Date.now
    },
    deadLetter: {
        type: Boolean,
        default: false
    },
    emailSent: {
        type: Boolean,
        default: false
    }
}, { timestamps: true });

const Task = mongoose.model("Task", taskSchema);


app.post("/task", async (req, res) => {

    const { type, payload, priority, runAt } = req.body;

    const newTask = new Task({
        type,
        payload,
        priority: priority || 0,
        runAt: runAt || Date.now()
    });

    await newTask.save();

    res.json(newTask);
});

app.get("/tasks", async (req, res) => {
    const allTasks = await Task.find();
    res.json(allTasks);

});

app.delete("/task/:id", (req, res) => {
    const id = Number(req.params.id);

    tasks = tasks.filter(task => task.id !== id);

    res.json({ message: "Task deleted" });
});

app.put("/task/:id", (req, res) => {
    const id = Number(req.params.id);

    const task = tasks.find(task => task.id === id);

    if (!task) {
        return res.status(404).json({ message: "Task not found" });
    }

    task.status = req.body.status;

    res.json(task);
    logger.info(`Task created ${task._id}`);
});

let activeJobs = 0;

async function worker() {
    if (shuttingDown) {
        return;
    }

    if (activeJobs >= process.env.MAX_CONCURRENT_JOBS) {
        return;
    }

    const task = await Task.findOneAndUpdate(
        {
            status: "pending",
            deadLetter: false,
            runAt: { $lte: new Date() }
        },
        { status: "running" },
        {
            returnDocument: 'after',
            sort: { priority: -1, createdAt: 1 }
        });

    if (!task) return;

    if (task.type === "sendEmail") {

        if (emailJobsExecuted >= process.env.EMAIL_RATE_LIMIT) {

            task.status = "pending";
            task.runAt = new Date(Date.now() + 1000);
            await task.save();

            return; // worker skips execution
        }

        emailJobsExecuted++;
    }

    activeJobs++;

    logger.info(`Processing task ${task._id}`);

    try {

        // REAL EMAIL JOB
        if (task.type === "sendEmail") {
            if (task.emailSent) {
                logger.info(`Skipping duplicate email ${task._id}`);
                task.status = "completed";
                await task.save();
                activeJobs--;

                return;
            }

            await sendEmail(task.payload);
            task.emailSent = true;
        }
        else {
            // SIMULATED JOB (3 sec delay)
            await new Promise(resolve => setTimeout(resolve, 3000));
        }

        task.status = "completed";
        logger.info(`Task completed ${task._id}`);

    }
    catch (error) {
        logger.error(`Task ${task._id} failed: ${error.message}`);
        task.retryCount += 1;

        if (task.retryCount >= process.env.RETRY_LIMIT) {

            task.status = "failed";
            task.deadLetter = true;

            logger.error(`Task ${task._id} moved to Dead Letter Queue`);

        }
        else {

            task.status = "pending";

        }

    }

    await task.save();

    activeJobs--;

}


app.get("/dead-letter-jobs", async (req, res) => {
    const jobs = await Task.find({ deadLetter: true });
    res.json(jobs);
});

async function recoverStuckJobs() {
    const timeout = new Date(Date.now() - 60000); // 1 minute ago

    const stuckJobs = await Task.updateMany(
        {
            status: "running",
            updatedAt: { $lt: timeout }
        },
        {
            status: "pending"
        }
    );

    if (stuckJobs.modifiedCount > 0) {
        console.log(`Recovered ${stuckJobs.modifiedCount} stuck jobs`);
    }
}

app.get("/metrics", async (req, res) => {

    const pending = await Task.countDocuments({ status: "pending" });
    const running = await Task.countDocuments({ status: "running" });
    const completed = await Task.countDocuments({ status: "completed" });
    const failed = await Task.countDocuments({ status: "failed" });
    const dlq = await Task.countDocuments({ deadLetter: true });

    res.json({
        pending,
        running,
        completed,
        failed,
        dlq
    });

});

app.post("/upload-csv", upload.single("file"), async (req, res) => {

    const results = [];

    fs.createReadStream(req.file.path)
        .pipe(csv())
        .on("data", (data) => results.push(data))
        .on("end", async () => {

            const tasks = results.map(row => ({
                type: "sendEmail",
                payload: {
                    to: row.email,
                    subject: "CMR & CIC Report Consultation, Repair, and Improvement Services",
                    html: emailBody
                },
                status: "pending"
            }));

            await Task.insertMany(tasks);

            res.json({
                message: `${tasks.length} email jobs created`
            });
        });

});

process.on("SIGINT", async () => {

    console.log("Graceful shutdown started...");
    shuttingDown = true;

    const interval = setInterval(() => {

        if (activeJobs === 0) {
            console.log("All jobs finished. Server shutting down.");
            clearInterval(interval);
            process.exit(0);
        } else {
            console.log(`Waiting for ${activeJobs} running jobs to finish...`);
        }

    }, 1000);

});

const WORKER_COUNT = process.env.WORKER_COUNT || 5;

for (let i = 0; i < WORKER_COUNT; i++) {
    setInterval(worker, 1000);
}
setInterval(recoverStuckJobs, 60000);

setInterval(async () => {

    const pending = await Task.countDocuments({ status: "pending" });

    console.log(`System health → Pending jobs: ${pending}`);

}, 60000);
