// js/main.js

document.addEventListener('DOMContentLoaded', () => {
    const csvFileInput = document.getElementById('ingCsvInput');
    const loadCsvButton = document.getElementById('loadCsvButton');
    const transactionsTableContainer = document.getElementById('transactionsTableContainer');

    let duckdb = null; // Will hold the DuckDB module after CDN load
    let db = null;     // Will hold the AsyncDuckDB instance
    let conn = null;   // Will hold the database connection

    /**
     * Initializes the DuckDB-WASM database and connects to it.
     * This function should be called once when the page loads.
     */
    async function initializeDuckDB() {
        try {
            // Ensure the duckdb global object is available from the CDN scripts
            if (typeof window.duckdb === 'undefined') {
                console.error("DuckDB-WASM not loaded. Check CDN script tags in index.html.");
                transactionsTableContainer.innerHTML = "<p style='color: red;'>Error: DuckDB-WASM library failed to load. Please check your internet connection or script paths.</p>";
                return;
            }
            duckdb = window.duckdb; // Assign the global duckdb object

            // Get the bundles provided by the CDN scripts
            const JSDELIVR_BUNDLES = duckdb.getBundles();
            const bundle = JSDELIVR_BUNDLES.find((b) => b.mvp); // Use the MVP bundle for minimal size

            // Create a Web Worker to run DuckDB in the background, keeping UI responsive
            const worker = new Worker(bundle.mainWorker);
            const logger = new duckdb.ConsoleLogger(); // Optional: for logging
            db = new duckdb.AsyncDuckDB(logger, worker);

            // Instantiate the database
            // bundle.pthreadWorker is used if you loaded duckdb-eh.js for multithreading
            await db.instantiate(bundle.mainWorker, bundle.pthreadWorker);
            conn = await db.connect();
            console.log("DuckDB-WASM initialized and connected.");

        } catch (e) {
            console.error("Failed to initialize DuckDB-WASM:", e);
            transactionsTableContainer.innerHTML = `<p style='color: red;'>Error initializing database: ${e.message}</p>`;
        }
    }

    // Call the initialization function when the DOM content is fully loaded
    initializeDuckDB();

    /**
     * Event listener for the "Load Data" button click.
     * Handles file reading, DuckDB registration, and querying.
     */
    loadCsvButton.addEventListener('click', async () => {
        const file = csvFileInput.files[0];
        if (!file) {
            alert("Please select an ING CSV file first.");
            return;
        }

        if (!conn) {
            alert("Database not ready. Please wait for initialization or refresh the page.");
            return;
        }

        transactionsTableContainer.innerHTML = "<p>Loading and processing data...</p>";

        try {
            // Read the file content as an ArrayBuffer
            const fileBuffer = await file.arrayBuffer();
            const fileUint8Array = new Uint8Array(fileBuffer);

            // Register the uploaded file in DuckDB's virtual file system
            // We'll use a consistent name 'ing_transactions.csv' within DuckDB queries
            await db.registerFileText('ing_transactions.csv', fileUint8Array);

            // SQL Query to process the ING CSV data
            // - Uses exact column names from your CSV (e.g., "Date", "Amount (EUR)")
            // - STRPTIME converts the 'Date' string (YYYYMMDD) to a proper DATE type
            // - REPLACE and CAST handle the comma decimal in "Amount (EUR)"
            // - Aliases (AS) are used to create cleaner column names for JavaScript consumption
            const query = `
                SELECT
                    STRPTIME("Date", '%Y%m%d')::DATE AS transaction_date,
                    "Name / Description" AS description,
                    "Account" AS account,
                    "Counterparty" AS counterparty,
                    "Code" AS transaction_code,
                    "Debit/credit" AS transaction_direction, -- 'Debit' or 'Credit'
                    CAST(REPLACE("Amount (EUR)", ',', '.') AS DOUBLE) AS amount_eur,
                    "Transaction type" AS transaction_type,
                    "Notifications" AS notification_details
                FROM 'ing_transactions.csv'
                ORDER BY transaction_date DESC
                LIMIT 50; -- Display top 50 most recent transactions
            `;
            const result = await conn.query(query);

            // Convert the query result to a standard JavaScript array of objects
            const data = result.toArray().map(row => row.toJSON());
            console.log("Fetched and processed data:", data);

            // Display the data in an HTML table
            displayDataInTable(data, transactionsTableContainer);

        } catch (e) {
            console.error("Error processing CSV with DuckDB:", e);
            transactionsTableContainer.innerHTML = `<p style='color: red;'>Error processing CSV: ${e.message}. Please check console for details.</p>`;
        } finally {
            // Important: Unregister the file after use to free up memory
            if (db && file) {
                await db.unregisterFile(file.name); // Using file.name as registered name
            }
        }
    });

    /**
     * Helper function to dynamically create and display an HTML table from data.
     * @param {Array<Object>} data - An array of objects, where each object is a row.
     * @param {HTMLElement} container - The DOM element where the table will be inserted.
     */
    function displayDataInTable(data, container) {
        if (data.length === 0) {
            container.innerHTML = "<p>No data found or processed.</p>";
            return;
        }

        let tableHtml = '<table border="1" style="width:100%; border-collapse: collapse;"><thead><tr>';
        const headers = Object.keys(data[0]);

        // Create table headers
        headers.forEach(header => {
            tableHtml += `<th style="padding: 8px; text-align: left; background-color: #f2f2f2;">${header}</th>`;
        });
        tableHtml += '</tr></thead><tbody>';

        // Create table rows
        data.forEach(row => {
            tableHtml += '<tr>';
            headers.forEach(header => {
                tableHtml += `<td style="padding: 8px; border: 1px solid #ddd;">${row[header] !== null ? row[header] : ''}</td>`;
            });
            tableHtml
