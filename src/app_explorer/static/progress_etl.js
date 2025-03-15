btn_start_etl.addEventListener('click', showProgress);
window.onload = showProgress;

function showProgress() {
    btn_start_etl.disabled = true;  // Disable the trigger button
    btn_start_etl.classList.remove('btn-primary');  // Remove the blue background
    btn_start_etl.classList.add('btn-secondary');  // Add a gray background
    btn_start_etl.textContent = "Data retrieval in progress...";  // Change the button text
    statusDiv.textContent = "Be patient, it won't be long...";  // Set the initial status
    loadingCard.style.display = "block"
    fetch('/simulate_etl')
        .then(response => response.json())
        .then(data => {
            let taskId = data.task_id;
            let isFetching = false;  // Add a flag to indicate whether a fetch request is in progress
            let intervalId = setInterval(() => {
                if (!isFetching) {  // Only send a new request if the previous one has completed
                    isFetching = true;  // Set the flag to true before sending the request
                    fetch(`/check_task/${taskId}`)
                        .then(response => response.json())
                        .then(data => {
                            let currentTime = new Date();
                            statusDiv.textContent = `${currentTime.toLocaleTimeString('nl-NL', { hour12: false })}: ${data.status}`;
                            if (data.collection_items) {
                                collectionItemDiv.textContent = `Collection item: ${data.collection_items.item}`
                                progressCollectionDiv.innerHTML = `<progress id="collection_item" class="w-100" value="${data.collection_items.current}" max="${data.collection_items.total}"></progress>`;
                                collectionArtistDiv.textContent = `Collection artist data for: ${data.collection_artists.item}`
                                progressArtistDiv.innerHTML = `<progress id="collection_artist" class="w-100" value="${data.collection_artists.current}" max="${data.collection_artists.total}"></progress>`;
                            }
                            if (data.status === 'SUCCESS') {
                                clearInterval(intervalId);
                                Swal.fire(
                                    'Discogs ETL done!',
                                    'Your collection information has been collected!',
                                    'success'
                                );
                                btn_start_etl.disabled = false;  // Re-enable the trigger button
                                btn_start_etl.classList.remove('btn-secondary');  // Remove the gray background
                                btn_start_etl.classList.add('btn-primary');  // Add the blue background
                                btn_start_etl.textContent = "Start";  // Change the button text
                                loadingCard.style.display = "none";
                            }
                            isFetching = false;  // Set the flag to false after the request has completed
                        });
                }
            }, 500);  // Poll every 1/2 second
        });

}