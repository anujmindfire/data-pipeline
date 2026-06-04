// --- CONFIGURATION PRESETS ---
const PRESETS = {
    covid: {
        name: "COVID-19 Latest Summary",
        sources: [
            {
                id: "covid-data",
                type: "csv",
                path: "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/latest/owid-covid-latest.csv",
                schema: {
                    "location": "country",
                    "date": "report_date",
                    "new_cases": "new_cases",
                    "new_deaths": "new_deaths"
                }
            }
        ],
        validation_rules: [
            { field: "country", rule: "required" },
            { field: "new_cases", rule: "min", param: "0" },
            { field: "new_deaths", rule: "min", param: "0" }
        ],
        transform_rules: [
            { field: "country", rule: "upper" },
            { field: "new_cases", rule: "cast", param: "float" },
            { field: "new_deaths", rule: "cast", param: "float" },
            { field: "processed_at", rule: "enrich_time" }
        ],
        aggregation_specs: [
            { field: "new_cases", func: "sum", target: "aggregate_global_cases" },
            { field: "new_deaths", func: "sum", target: "aggregate_global_deaths" },
            { field: "new_cases", func: "max", target: "highest_single_day_cases" },
            { field: "new_cases", func: "avg", target: "average_daily_cases" },
            { field: "new_cases", func: "sum", group_by: "country", target: "cases_by_country" }
        ],
        export_targets: [
            { type: "json", path: "data/exports/covid/covid_summary.json" },
            { type: "csv", path: "data/exports/covid/covid_summary.csv" }
        ],
        workers: {
            validation: 8,
            transformation: 8
        }
    },
    crypto: {
        name: "Crypto Markets Ingestion",
        sources: [
            {
                id: "coingecko-markets",
                type: "api",
                path: "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=12&page=1",
                schema: {
                    "id": "coin_id",
                    "symbol": "symbol",
                    "current_price": "price",
                    "market_cap": "market_cap"
                }
            }
        ],
        validation_rules: [
            { field: "coin_id", rule: "required" },
            { field: "price", rule: "min", param: "0" }
        ],
        transform_rules: [
            { field: "symbol", rule: "upper" },
            { field: "price", rule: "cast", param: "float" },
            { field: "market_cap", rule: "cast", param: "float" },
            { field: "synchronized_at", rule: "enrich_time" }
        ],
        aggregation_specs: [
            { field: "price", func: "avg", target: "average_market_price" },
            { field: "price", func: "max", target: "highest_market_price" },
            { field: "market_cap", func: "sum", target: "total_market_cap_aggregate" }
        ],
        export_targets: [
            { type: "json", path: "data/exports/crypto/crypto_latest.json" }
        ],
        workers: {
            validation: 4,
            transformation: 4
        }
    },
    mock: {
        name: "Heights & Weights Biometrics Ingest",
        sources: [
            {
                id: "biometrics-file",
                type: "csv",
                path: "samples/biometrics_sample.csv",
                schema: {
                    "Index": "ref_index",
                    "Height": "height_inches",
                    "Weight": "weight_pounds"
                }
            }
        ],
        validation_rules: [
            { field: "height_inches", rule: "min", param: "30" },
            { field: "weight_pounds", rule: "min", param: "30" }
        ],
        transform_rules: [
            { field: "height_inches", rule: "cast", param: "float" },
            { field: "weight_pounds", rule: "cast", param: "float" },
            // Add custom modifier rule just to test add_constant rule (e.g. adjust height calibration +10)
            { field: "height_inches", rule: "add_constant", param: "2.5" },
            { field: "ingestion_stamp", rule: "enrich_time" }
        ],
        aggregation_specs: [
            { field: "height_inches", func: "avg", target: "average_height_inches" },
            { field: "weight_pounds", func: "avg", target: "average_weight_pounds" },
            { field: "height_inches", func: "max", target: "max_height_inches" },
            { field: "weight_pounds", func: "min", target: "min_weight_pounds" }
        ],
        export_targets: [
            { type: "json", path: "data/exports/biometrics/biometrics_data.json" },
            { type: "csv", path: "data/exports/biometrics/biometrics_data.csv" }
        ],
        workers: {
            validation: 2,
            transformation: 2
        }
    }
};

// --- GLOBAL VARIABLES & TELEMETRY REGISTRY ---
let activeModalJobId = null;
let modalPollingInterval = null;
let listPollingInterval = null;
let latencyChart = null;

// --- INITIALIZE SPA DOM ---
document.addEventListener("DOMContentLoaded", () => {
    // Populate form with default preset (Covid)
    selectPreset("covid");

    // Hook Form presets buttons
    document.getElementById("preset-covid").addEventListener("click", () => selectPreset("covid"));
    document.getElementById("preset-crypto").addEventListener("click", () => selectPreset("crypto"));
    document.getElementById("preset-mock").addEventListener("click", () => selectPreset("mock"));

    // Pipeline Form submit
    document.getElementById("pipeline-form").addEventListener("submit", handleFormSubmit);

    // Refresh dashboard list manually
    document.getElementById("btn-refresh").addEventListener("click", () => {
        fetchJobs();
        showToast("Telemetry metrics updated", "fa-check-circle");
    });

    // Close Modal Controls
    document.getElementById("btn-close-modal").addEventListener("click", closeModal);
    document.getElementById("details-modal").addEventListener("click", (e) => {
        if (e.target.id === "details-modal") closeModal();
    });

    // Tab buttons hooks
    const tabButtons = document.querySelectorAll(".tab-btn");
    tabButtons.forEach(btn => {
        btn.addEventListener("click", () => {
            tabButtons.forEach(b => b.classList.remove("active"));
            btn.classList.add("active");
            
            const tabId = btn.getAttribute("data-tab");
            const panes = document.querySelectorAll(".tab-pane");
            panes.forEach(p => p.classList.remove("active"));
            document.getElementById(tabId).classList.add("active");
        });
    });

    // Modal action hooks
    document.getElementById("btn-cancel-job").addEventListener("click", cancelActiveJob);
    document.getElementById("btn-delete-job").addEventListener("click", deleteActiveJob);

    // Initial load list and start global background polling
    fetchJobs();
    listPollingInterval = setInterval(fetchJobs, 2000);
});

// Select Preset and Fill Forms
function selectPreset(presetKey) {
    const btnIds = { covid: "preset-covid", crypto: "preset-crypto", mock: "preset-mock" };
    
    // Toggle active preset classes
    Object.keys(btnIds).forEach(k => {
        document.getElementById(btnIds[k]).classList.remove("active");
    });
    document.getElementById(btnIds[presetKey]).classList.add("active");

    const data = PRESETS[presetKey];
    
    // Fill in Name and generate a dynamic random job ID based on template
    document.getElementById("job-id").value = presetKey + "-" + Math.random().toString(36).substring(2, 6);
    document.getElementById("job-name").value = data.name;
    
    // Fill job config textarea pretty formatted
    const configCopy = { ...data };
    delete configCopy.name; // ID and Name are separate inputs in form
    document.getElementById("job-config").value = JSON.stringify(configCopy, null, 2);
}

// POST Job Specification to API
async function handleFormSubmit(e) {
    e.preventDefault();
    
    const jobId = document.getElementById("job-id").value.trim();
    const jobName = document.getElementById("job-name").value.trim();
    const configRaw = document.getElementById("job-config").value.trim();
    
    let spec;
    try {
        spec = JSON.parse(configRaw);
    } catch (err) {
        showToast("Error parsing job configuration: JSON is invalid", "fa-exclamation-triangle");
        return;
    }

    // Set form fields into spec JSON
    if (jobId) spec.id = jobId;
    spec.name = jobName;

    const btnSubmit = document.getElementById("btn-submit");
    btnSubmit.disabled = true;
    btnSubmit.innerHTML = `<i class="fa-solid fa-spinner fa-spin"></i> Dispatching...`;

    try {
        const resp = await fetch("/api/v1/pipelines", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(spec)
        });

        const resData = await resp.json();
        
        if (!resp.ok) {
            throw new Error(resData.error || "Server rejected pipeline request");
        }

        showToast(`Pipeline job ${resData.job_id} successfully queued!`, "fa-rocket");
        
        // Reset ID field for next run
        const activePreset = document.querySelector(".btn-preset.active").id.replace("preset-", "");
        selectPreset(activePreset);

        fetchJobs();
    } catch (err) {
        showToast(err.message, "fa-exclamation-triangle");
    } finally {
        btnSubmit.disabled = false;
        btnSubmit.innerHTML = `<i class="fa-solid fa-rocket"></i> Dispatch Pipeline`;
    }
}

// Fetch lists of pipelines
async function fetchJobs() {
    try {
        const resp = await fetch("/api/v1/pipelines");
        if (!resp.ok) throw new Error("HTTP error " + resp.status);
        const list = await resp.json();
        renderJobsList(list);
    } catch (err) {
        console.error("Failed to fetch jobs list: ", err);
    }
}

// Render dynamic Jobs cards list
function renderJobsList(jobs) {
    const container = document.getElementById("jobs-container");
    if (!jobs || jobs.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <i class="fa-solid fa-network-wired text-muted"></i>
                <p>No pipelines found. Dispatch a job to begin monitoring.</p>
            </div>`;
        return;
    }

    let html = "";
    jobs.forEach(job => {
        // Handle values in case job progress is running or completed
        let processed = job.processed_records;
        let failed = job.failed_records;
        let total = job.total_records;
        let status = job.status;

        // If in-memory is enriched, overlay it
        if (job.active_progress) {
            processed = job.active_progress.processed_records;
            failed = job.active_progress.failed_records;
            total = job.active_progress.total_records;
            status = job.active_progress.status;
        }

        // Percent calculation
        let percent = 0;
        if (total > 0) {
            percent = Math.min(100, Math.round((processed + failed) * 100 / total));
        } else if (status === "COMPLETED") {
            percent = 100;
        }

        const badgeClass = `badge-${status.toLowerCase()}`;
        const startTimeStr = job.started_at ? new Date(job.started_at).toLocaleTimeString() : new Date(job.created_at).toLocaleTimeString();

        html += `
            <div class="job-card glass-card" onclick="openJobDetails('${job.id}')">
                <div class="job-card-header">
                    <div class="job-info">
                        <h3>${job.config ? JSON.parse(job.config).name || job.id : job.id}</h3>
                        <span class="job-meta"><i class="fa-regular fa-clock"></i> Started: ${startTimeStr} | ID: ${job.id}</span>
                    </div>
                    <span class="badge ${badgeClass}">${status}</span>
                </div>

                <div class="job-progress-section">
                    <div class="progress-stats">
                        <span>Progress</span>
                        <span>${percent}% (${processed + failed} / ${total > 0 ? total : 'Calculating...'})</span>
                    </div>
                    <div class="progress-bar-container">
                        <div class="progress-bar-fill success" style="width: ${total > 0 ? (processed * 100 / total) : (status === 'COMPLETED' ? 100 : 0)}%"></div>
                        <div class="progress-bar-fill error" style="width: ${total > 0 ? (failed * 100 / total) : 0}%"></div>
                    </div>
                </div>

                <div class="job-counters-grid">
                    <div class="counter-item">
                        <span class="counter-num">${processed}</span>
                        <span class="counter-lbl">Exported</span>
                    </div>
                    <div class="counter-item text-danger">
                        <span class="counter-num">${failed}</span>
                        <span class="counter-lbl">Failed</span>
                    </div>
                    <div class="counter-item">
                        <span class="counter-num">${job.active_progress ? job.active_progress.processing_rate.toFixed(1) : '0.0'}</span>
                        <span class="counter-lbl">recs/s</span>
                    </div>
                    <div class="counter-item">
                        <span class="counter-num">${job.completed_at ? formatDuration(new Date(job.completed_at) - new Date(job.started_at)) : 'Active'}</span>
                        <span class="counter-lbl">Duration</span>
                    </div>
                </div>
            </div>`;
    });

    container.innerHTML = html;
}

// Open Detail Audit Modal
function openJobDetails(jobId) {
    activeModalJobId = jobId;
    document.getElementById("details-modal").classList.add("active");
    
    // Immediate load and start modal active polling
    pollJobDetails();
    modalPollingInterval = setInterval(pollJobDetails, 1000);
}

// Poll specific job details
async function pollJobDetails() {
    if (!activeModalJobId) return;

    try {
        // Fetch progress details
        const respProg = await fetch(`/api/v1/pipelines/${activeModalJobId}/progress`);
        if (!respProg.ok) throw new Error("Progress fetch failed");
        const prog = await respProg.json();

        // Update modal metrics headers
        document.getElementById("modal-title").innerText = prog.name || `Pipeline Run`;
        document.getElementById("modal-job-id").innerText = prog.job_id;
        document.getElementById("det-status").innerText = prog.status;
        
        // Status Badge Style
        const detStatusEl = document.getElementById("det-status");
        detStatusEl.className = "m-val badge " + `badge-${prog.status.toLowerCase()}`;

        // Calculate and show success rates
        const totalProcessed = prog.processed_records + prog.failed_records;
        const successRate = totalProcessed > 0 ? Math.round((prog.processed_records * 100) / totalProcessed) : 100;
        document.getElementById("det-success-rate").innerText = `${successRate}%`;
        document.getElementById("det-rate").innerText = `${prog.processing_rate.toFixed(1)} recs/s`;

        // Calculate and show duration
        const durationMs = prog.end_time ? (new Date(prog.end_time) - new Date(prog.start_time)) : (new Date() - new Date(prog.start_time));
        document.getElementById("det-duration").innerText = formatDuration(durationMs);

        // Progress Counters
        document.getElementById("det-progress-text").innerText = `${prog.processed_records + prog.failed_records} / ${prog.total_records > 0 ? prog.total_records : 'Calculating...'}`;
        
        const total = prog.total_records > 0 ? prog.total_records : totalProcessed;
        document.getElementById("det-progress-success").style.width = `${total > 0 ? (prog.processed_records * 100 / total) : 0}%`;
        document.getElementById("det-progress-failed").style.width = `${total > 0 ? (prog.failed_records * 100 / total) : 0}%`;

        // Show/Hide cancel cancel button based on active run
        const isRunning = prog.status === "RUNNING" || prog.status === "PENDING";
        document.getElementById("btn-cancel-job").style.display = isRunning ? "inline-flex" : "none";

        // Draw dynamic latency chart
        renderLatencyChart(prog.stage_latencies_ms);

        // Load errors and results ONLY when required (reduces active bandwidth)
        if (document.querySelector(".tab-btn.active").getAttribute("data-tab") === "tab-results") {
            fetchResults(activeModalJobId);
        } else if (document.querySelector(".tab-btn.active").getAttribute("data-tab") === "tab-errors") {
            fetchErrors(activeModalJobId);
        }

        // Set quick error counter count
        document.getElementById("det-err-count").innerText = prog.failed_records;

    } catch (err) {
        console.error("Modal polling error:", err);
    }
}

// Fetch Aggregated final summaries
async function fetchResults(jobId) {
    try {
        const resp = await fetch(`/api/v1/pipelines/${jobId}/results`);
        if (!resp.ok) {
            document.getElementById("det-results-code").innerText = "{\n  \"message\": \"Job is still running or results not finalized.\"\n}";
            return;
        }
        const data = await resp.json();
        document.getElementById("det-results-code").innerText = JSON.stringify(data.aggregates, null, 2);
        
        // Show file locations in subtitle
        if (data.export_files && data.export_files.length > 0) {
            document.getElementById("det-results-time").innerText = `Exported to: ${data.export_files.join(", ")}`;
        }
    } catch (err) {
        document.getElementById("det-results-code").innerText = "{\n  \"error\": \"No final aggregates found. Complete the pipeline.\"\n}";
    }
}

// Fetch dynamic Audit failures list
async function fetchErrors(jobId) {
    try {
        const resp = await fetch(`/api/v1/pipelines/${jobId}/errors`);
        if (!resp.ok) return;
        const errs = await resp.json();

        const tbody = document.getElementById("det-errors-list");
        if (!errs || errs.length === 0) {
            tbody.innerHTML = `<tr><td colspan="3" class="text-center text-muted">No record errors encountered. Good work!</td></tr>`;
            return;
        }

        let html = "";
        errs.forEach(e => {
            html += `
                <tr>
                    <td class="text-danger" style="font-weight: 600;">${e.stage}</td>
                    <td><code style="font-family: 'JetBrains Mono', monospace; font-size: 0.85rem; color: #cbd5e1; background: rgba(255, 255, 255, 0.05); padding: 4px 8px; border-radius: 4px; word-break: break-all; display: block; max-height: 80px; overflow-y: auto;">${e.raw_data || 'N/A'}</code></td>
                    <td>${e.error_message}</td>
                </tr>`;
        });
        tbody.innerHTML = html;
    } catch (err) {
        console.error(err);
    }
}

// Plot Latency Bottles utilizing Chart.JS
function renderLatencyChart(latencies) {
    const ctx = document.getElementById('latencyChart').getContext('2d');
    
    // Map data to display fields
    const labels = ["Ingestion", "Validation", "Transformation", "Aggregation"];
    const values = [
        latencies["ingestion"] || 0,
        latencies["validation"] ? Math.max(0, latencies["validation"] - (latencies["ingestion"] || 0)) : 0,
        latencies["transformation"] ? Math.max(0, latencies["transformation"] - (latencies["validation"] || 0)) : 0,
        latencies["aggregation"] ? Math.max(0, latencies["aggregation"] - (latencies["transformation"] || 0)) : 0
    ];

    if (latencyChart) {
        // If chart exists, just update dataset values directly
        latencyChart.data.datasets[0].data = values;
        latencyChart.update();
        return;
    }

    // Gradient filling for absolute beautiful chart
    const purpleBlueGrad = ctx.createLinearGradient(0, 0, 400, 0);
    purpleBlueGrad.addColorStop(0, '#a370f7');
    purpleBlueGrad.addColorStop(1, '#3b82f6');

    latencyChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Individual Stage Duration (ms)',
                data: values,
                backgroundColor: purpleBlueGrad,
                borderColor: 'rgba(255, 255, 255, 0.1)',
                borderWidth: 1,
                borderRadius: 8,
                barThickness: 24,
            }]
        },
        options: {
            indexAxis: 'y', // Make horizontal bars
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: '#161c2d',
                    titleFont: { family: 'Outfit' },
                    bodyFont: { family: 'Outfit' },
                    borderColor: 'rgba(255, 255, 255, 0.08)',
                    borderWidth: 1
                }
            },
            scales: {
                x: {
                    grid: { color: 'rgba(255, 255, 255, 0.04)' },
                    ticks: { color: '#9ca3af', font: { family: 'Outfit' } }
                },
                y: {
                    grid: { display: false },
                    ticks: { color: '#f3f4f6', font: { family: 'Outfit', weight: '500' } }
                }
            }
        }
    });
}

// Cancel (Abort) Active Pipeline Job
async function cancelActiveJob() {
    if (!activeModalJobId) return;

    if (!confirm("Are you sure you want to stop this running data pipeline? All stages will be terminated immediately.")) return;

    try {
        const resp = await fetch(`/api/v1/pipelines/${activeModalJobId}/cancel`, {
            method: "PATCH"
        });
        const res = await resp.json();
        
        if (!resp.ok) throw new Error(res.error || "Failed to cancel");

        showToast("Cancellation signal dispatched!", "fa-circle-stop");
        pollJobDetails();
        fetchJobs();
    } catch (err) {
        showToast(err.message, "fa-exclamation-triangle");
    }
}

// DELETE pipeline and wipe details
async function deleteActiveJob() {
    if (!activeModalJobId) return;

    if (!confirm("Wipe all historical execution details, generated summaries and errors from the registry? Generated output files will remain intact.")) return;

    try {
        const resp = await fetch(`/api/v1/pipelines/${activeModalJobId}`, {
            method: "DELETE"
        });
        const res = await resp.json();
        
        if (!resp.ok) throw new Error(res.error || "Failed to delete");

        showToast("Job logs wiped successfully", "fa-trash-can");
        closeModal();
        fetchJobs();
    } catch (err) {
        showToast(err.message, "fa-exclamation-triangle");
    }
}

// Close Modal helper
function closeModal() {
    document.getElementById("details-modal").classList.remove("active");
    activeModalJobId = null;
    if (modalPollingInterval) {
        clearInterval(modalPollingInterval);
        modalPollingInterval = null;
    }
    if (latencyChart) {
        latencyChart.destroy();
        latencyChart = null;
    }
    fetchJobs();
}

// Time formatter
function formatDuration(ms) {
    if (ms < 1000) return `${ms}ms`;
    const secs = (ms / 1000).toFixed(1);
    if (secs < 60) return `${secs}s`;
    const mins = Math.floor(secs / 60);
    const remainingSecs = Math.round(secs % 60);
    return `${mins}m ${remainingSecs}s`;
}

// Toast Notifications emitter
function showToast(message, iconClass = "fa-info-circle") {
    const container = document.getElementById("toast-container");
    const toast = document.createElement("div");
    toast.className = "toast glass-card";
    toast.innerHTML = `<i class="fa-solid ${iconClass}"></i> <span>${message}</span>`;
    
    container.appendChild(toast);
    
    // Automatically fade out and remove after 4 seconds
    setTimeout(() => {
        toast.style.opacity = '0';
        toast.style.transition = 'opacity 0.5s ease-out';
        setTimeout(() => toast.remove(), 500);
    }, 4000);
}
