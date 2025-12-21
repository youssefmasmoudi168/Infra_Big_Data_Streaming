const API_URL = window.location.origin;

const workspace = document.getElementById("notebook-workspace");
const addCellBtn = document.getElementById("add-cell-btn");
const runAllBtn = document.getElementById("run-all-btn");
const jobList = document.getElementById("job-list");

let cells = [];

// Templates
const defaultCode = `from pyspark.sql import SparkSession

spark = SparkSession.builder \\
    .appName("OrionAnalysis") \\
    .getOrCreate()

data = [("A", 10), ("B", 20)]
spark.createDataFrame(data, ["key", "value"]).show()

spark.stop()
`;

function createCell(content = "") {
    const cellId = Date.now() + Math.random().toString(36).substr(2, 5);
    const cell = {
        id: cellId,
        content: content || defaultCode,
        status: "idle",
        logs: "",
        jobId: null
    };

    cells.push(cell);
    renderNotebook();
    return cell;
}

function renderNotebook() {
    workspace.innerHTML = "";
    cells.forEach((cell, index) => {
        const cellEl = document.createElement("div");
        cellEl.className = `cell ${cell.status === 'running' ? 'active' : ''}`;
        cellEl.innerHTML = `
            <div class="cell-header">
                <div class="cell-info">In [${index + 1}]</div>
                <div class="cell-actions">
                    <span class="badge ${cell.status}">${cell.status}</span>
                    <button class="btn-icon-only run-cell" title="Run Cell">▶️</button>
                    <button class="btn-icon-only remove-cell" title="Remove Cell">🗑️</button>
                </div>
            </div>
            <textarea class="cell-input" spellcheck="false">${cell.content}</textarea>
            <div class="cell-output">${cell.logs}</div>
        `;

        // Event Listeners
        cellEl.querySelector(".cell-input").oninput = (e) => {
            cell.content = e.target.value;
        };

        cellEl.querySelector(".run-cell").onclick = () => executeCell(cell);
        cellEl.querySelector(".remove-cell").onclick = () => {
            cells = cells.filter(c => c.id !== cell.id);
            renderNotebook();
        };

        workspace.appendChild(cellEl);
    });
}

async function executeCell(cell) {
    cell.status = "queued";
    cell.logs = "Queuing job...";
    renderNotebook();

    try {
        const response = await fetch(`${API_URL}/submit`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ code: cell.content })
        });

        if (!response.ok) throw new Error(`HTTP ${response.status}`);

        const data = await response.json();
        cell.jobId = data.job_id;
        startPolling(cell);
    } catch (err) {
        cell.status = "failed";
        cell.logs = "Submission Error: " + err.message;
        renderNotebook();
    }
}

function startPolling(cell) {
    cell.status = "running";
    renderNotebook();

    const interval = setInterval(async () => {
        try {
            const response = await fetch(`${API_URL}/jobs/${cell.jobId}`);
            const job = await response.json();

            cell.status = job.status;
            cell.logs = job.logs || "Executing...";

            if (["success", "failed", "error"].includes(job.status)) {
                clearInterval(interval);
                fetchJobs(); // Update sidebar
            }
            renderNotebook();
        } catch (err) {
            clearInterval(interval);
            cell.status = "error";
            cell.logs = "Polling failed.";
            renderNotebook();
        }
    }, 2000);
}

async function fetchJobs() {
    try {
        const response = await fetch(`${API_URL}/jobs`);
        const jobs = await response.json();
        jobList.innerHTML = jobs.map(job => `
            <li class="job-item">
                <div class="job-id">${job.id.substring(0, 8)}</div>
                <div class="job-status">${job.status}</div>
            </li>
        `).join("");
    } catch (err) { }
}

addCellBtn.onclick = () => createCell();
runAllBtn.onclick = () => cells.forEach(c => executeCell(c));

// Initial Cell
createCell();
fetchJobs();
setInterval(fetchJobs, 10000);
