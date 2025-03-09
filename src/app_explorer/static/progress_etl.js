btn_start_etl.addEventListener('click', () => {
    btn_start_etl.disabled = true;  // Disable the trigger button
    btn_start_etl.classList.remove('btn-primary');  // Remove the blue background
    btn_start_etl.classList.add('btn-secondary disabled');  // Add a gray background
    btn_start_etl.textContent = "Task in progress...";  // Change the button text
    statusDiv.textContent = "Be patient, it won't be long...";  // Set the initial status
    fetch('/discogs_etl')
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
                            statusDiv.textContent = `${currentTime.toLocaleTimeString()}: ${data.status}`;  // Update the status DIV
                            if (data.step === 'Collection items') {
                                progressCollection.innerHTML = `<progress id="file" value="${data.iteration}" max="${data.total}"></progress>`;
                            }
                            if (data.step === 'Collection artist') {
                                progressArtist.innerHTML = `<progress id="file" value="${data.iteration}" max="${data.total}"></progress>`;
                            }
                            messageDiv.textContent = `${data.step} - ${data.item}`;
                            if (data.status === 'SUCCESS') {
                                clearInterval(intervalId);
                                Swal.fire(
                                    'Discogs ETL done!',
                                    'Your collection information has been collected!',
                                    'success'
                                );
                                triggerButton.disabled = false;  // Re-enable the trigger button
                                triggerButton.classList.remove('bg-gray-500');  // Remove the gray background
                                triggerButton.classList.add('bg-blue-500');  // Add the blue background
                                triggerButton.textContent = "TRIGGER TEST TASK";  // Change the button text
                            }
                            isFetching = false;  // Set the flag to false after the request has completed
                        });
                }
            }, 500);  // Poll every 1/2 second
        });
});
