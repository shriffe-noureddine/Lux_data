FROM apache/airflow:2.9.3

# Install Python deps as the airflow user (required by Airflow images)
USER airflow
COPY --chown=airflow:root requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
