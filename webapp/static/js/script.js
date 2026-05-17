document.addEventListener('DOMContentLoaded', () => {
    let allAlerts = [];
    let displayedAlerts = [];
    const alertsPerPage = 50;
    let currentPage = 1;
    let isStreaming = true;
    let eventSource = new EventSource('/stream');

    const tableBody = document.getElementById('transactions-table');
    const filterSelect = document.getElementById('filter-status');
    const loadMoreButton = document.getElementById('load-more');
    const toggleStreamButton = document.getElementById('toggle-stream');

    const renderAlerts = () => {
        console.log('Rendering alerts:', allAlerts);
        allAlerts.sort((a, b) => {
            const aIsAttack = a.attack_status.toLowerCase().includes('attack');
            const bIsAttack = b.attack_status.toLowerCase().includes('attack');
            if (aIsAttack && !bIsAttack) return -1;
            if (!aIsAttack && bIsAttack) return 1;
            return b.timestamp - a.timestamp;
        });

        displayedAlerts = [];
        let normalCount = 0;
        allAlerts.forEach(alert => {
            const attackStatus = alert.attack_status.toLowerCase().includes('attack') ? 'attack' : 'normal';
            if (attackStatus === 'attack' || (filterSelect.value === 'all' && normalCount < 10)) {
                displayedAlerts.push(alert);
                if (attackStatus === 'normal') normalCount++;
            }
        });

        const start = (currentPage - 1) * alertsPerPage;
        const end = start + alertsPerPage;
        const paginatedAlerts = displayedAlerts.slice(0, end);

        tableBody.innerHTML = '';
        paginatedAlerts.forEach(alert => {
            const attackStatus = alert.attack_status.toLowerCase().includes('attack') ? 'attack' : 'normal';
            const row = document.createElement('tr');
            row.className = `alert-row ${attackStatus}`;
            row.innerHTML = `
                <td>${new Date(alert.timestamp * 1000).toLocaleString()}</td>
                <td class="${attackStatus}">
                    <i class="fas ${attackStatus === 'attack' ? 'fa-exclamation-triangle status-icon text-danger' : 'fa-check-circle status-icon text-success'}"></i>
                    ${alert.attack_status}
                </td>
                <td>${alert.rf_prediction}</td>
                <td>${alert.dt_prediction}</td>
                <td>${alert.src_ip}</td>
                <td>${alert.dst_ip}</td>
                <td>${alert.src_port}</td>
                <td>${alert.dst_port}</td>
                <td>${alert.protocol}</td>
            `;
            tableBody.appendChild(row);
        });

        loadMoreButton.style.display = end < displayedAlerts.length ? 'block' : 'none';
    };

    filterSelect.addEventListener('change', () => {
        currentPage = 1;
        renderAlerts();
    });

    const clearButton = document.getElementById('clear-table');
    clearButton.addEventListener('click', () => {
        allAlerts = [];
        displayedAlerts = [];
        currentPage = 1;
        renderAlerts();
    });

    loadMoreButton.addEventListener('click', () => {
        currentPage++;
        renderAlerts();
    });

    toggleStreamButton.addEventListener('click', () => {
        isStreaming = !isStreaming;
        if (isStreaming) {
            eventSource = new EventSource('/stream');
            setupEventSource();
            toggleStreamButton.innerHTML = '<i class="fas fa-pause me-1"></i> Pause Updates';
            toggleStreamButton.classList.remove('btn-success');
            toggleStreamButton.classList.add('btn-outline-secondary');
        } else {
            eventSource.close();
            toggleStreamButton.innerHTML = '<i class="fas fa-play me-1"></i> Resume Updates';
            toggleStreamButton.classList.remove('btn-outline-secondary');
            toggleStreamButton.classList.add('btn-success');
        }
    });

    const setupEventSource = () => {
        eventSource.onmessage = (event) => {
            const alert = JSON.parse(event.data);
            console.log('Received alert via SSE:', alert);

            allAlerts.push(alert);

            if (alert.attack_status.toLowerCase().includes('attack')) {
                alert(`ATTACK DETECTED! Source IP: ${alert.src_ip}, Destination IP: ${alert.dst_ip}`);
            }

            if (allAlerts.length > 100) {
                allAlerts.shift();
            }

            renderAlerts();
        };

        eventSource.onerror = () => {
            console.error('SSE connection error');
            if (isStreaming) {
                setTimeout(() => {
                    eventSource = new EventSource('/stream');
                    setupEventSource();
                }, 5000);
            }
        };
    };

    setupEventSource();

    const fetchInitialAlerts = async () => {
        try {
            const response = await fetch('/alerts');
            const initialAlerts = await response.json();
            console.log('Fetched initial alerts:', initialAlerts);
            if (initialAlerts.length > 0) {
                allAlerts = initialAlerts;
                renderAlerts();
            }
        } catch (error) {
            console.error('Error fetching initial alerts:', error);
        }
    };

    fetchInitialAlerts();
});